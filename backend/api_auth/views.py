import json
import secrets
from datetime import datetime, timedelta, timezone as datetime_timezone
from urllib.parse import urlencode

from django.conf import settings
from django.contrib.auth.models import Group
from django.core.exceptions import ImproperlyConfigured, SuspiciousOperation
from django.http import HttpResponseRedirect
from django.http.request import validate_host
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from django_redis import get_redis_connection
from drf_spectacular.utils import extend_schema
from rest_framework import serializers, status, permissions, views
from rest_framework.exceptions import ValidationError
from rest_framework.response import Response
from rest_framework_simplejwt.tokens import RefreshToken
from rest_framework_simplejwt.views import TokenRefreshView, TokenVerifyView

from common.auth import SUPERADMIN_GROUP_NAME, ensure_superadmin_group, init_user
from common.authenticate.workos import WorkOSAuthClient
from common.celery_tasks import process_workos_webhook_task
from sql.models import Users
from api_core.response import success_response

WORKOS_STATE_COOKIE_NAME = "datamingle_workos_state"
WORKOS_SESSION_COOKIE_NAME = "datamingle_workos_session_id"
WORKOS_EXCHANGE_PREFIX = "workos-exchange-code:"
WORKOS_WEBHOOK_MAX_AGE_SECONDS = 300


def _cookie_secure(request):
    return request.is_secure() or not settings.DEBUG


def _validate_and_build_uri(request, path):
    host = request.get_host()
    if not validate_host(host, settings.ALLOWED_HOSTS):
        raise SuspiciousOperation(f"Request host '{host}' is not in ALLOWED_HOSTS.")
    return request.build_absolute_uri(path)


def _workos_callback_uri(request):
    return _validate_and_build_uri(request, "/api/auth/workos/callback/")


def _workos_logout_return_uri(request):
    return _validate_and_build_uri(request, "/login")


def _login_redirect_with_error(message):
    query = urlencode({"error": message})
    return HttpResponseRedirect(f"/login?{query}")


def _issue_local_token_pair(user):
    refresh = RefreshToken.for_user(user)
    return {"access": str(refresh.access_token), "refresh": str(refresh)}


def _workos_attr(source, name, default=""):
    if isinstance(source, dict):
        return source.get(name, default)
    return getattr(source, name, default)


def _json_safe_workos_value(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(key): _json_safe_workos_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe_workos_value(item) for item in value]

    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        return _json_safe_workos_value(model_dump())

    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        return _json_safe_workos_value(to_dict())

    if hasattr(value, "__dict__"):
        return {
            key: _json_safe_workos_value(item)
            for key, item in vars(value).items()
            if not key.startswith("_")
        }

    return str(value)


def _parse_workos_webhook_timestamp(value):
    if value in (None, ""):
        return None

    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=datetime_timezone.utc)

    raw_value = str(value).strip()
    if not raw_value:
        return None

    try:
        return datetime.fromtimestamp(float(raw_value), tz=datetime_timezone.utc)
    except ValueError:
        pass

    parsed_value = parse_datetime(raw_value)
    if parsed_value is None:
        raise SuspiciousOperation("WorkOS webhook timestamp is invalid.")
    if timezone.is_naive(parsed_value):
        parsed_value = timezone.make_aware(parsed_value, datetime_timezone.utc)
    return parsed_value


def _workos_signature_timestamp(event_signature):
    for part in event_signature.split(","):
        key, separator, value = part.strip().partition("=")
        if separator and key == "t":
            return _parse_workos_webhook_timestamp(value)
    return None


def _workos_event_timestamp(event):
    for field_name in ("created_at", "occurred_at", "timestamp"):
        value = _workos_attr(event, field_name, None)
        if value:
            return _parse_workos_webhook_timestamp(value)
    return None


def _validate_workos_webhook_freshness(event, event_signature=""):
    event_timestamp = _workos_signature_timestamp(
        event_signature
    ) or _workos_event_timestamp(event)
    if event_timestamp is None:
        return

    now = datetime.now(datetime_timezone.utc)
    if timezone.is_naive(event_timestamp):
        event_timestamp = timezone.make_aware(event_timestamp, datetime_timezone.utc)
    else:
        event_timestamp = event_timestamp.astimezone(datetime_timezone.utc)

    allowed_skew = timedelta(seconds=WORKOS_WEBHOOK_MAX_AGE_SECONDS)
    if now - event_timestamp > allowed_skew:
        raise SuspiciousOperation("WorkOS webhook timestamp is too old.")
    if event_timestamp - now > allowed_skew:
        raise SuspiciousOperation("WorkOS webhook timestamp is too far in the future.")


def _require_workos_linked_user(user):
    if not user.workos_user_id:
        raise ValidationError("This Datamingle account is not linked to a WorkOS user.")


def _workos_display_name(first_name, last_name, fallback):
    display_name = " ".join(
        value for value in (first_name.strip(), last_name.strip()) if value
    ).strip()
    return display_name or fallback


def _serialize_workos_profile(workos_user):
    first_name = str(_workos_attr(workos_user, "first_name", "") or "")
    last_name = str(_workos_attr(workos_user, "last_name", "") or "")
    email = str(_workos_attr(workos_user, "email", "") or "").strip().lower()
    profile_picture_url = str(
        _workos_attr(workos_user, "profile_picture_url", "") or ""
    ).strip()

    return {
        "id": str(_workos_attr(workos_user, "id", "") or ""),
        "email": email,
        "first_name": first_name,
        "last_name": last_name,
        "display_name": _workos_display_name(first_name, last_name, email),
        "profile_picture_url": profile_picture_url,
    }


def _sync_local_user_from_workos_profile(user, workos_user):
    profile = _serialize_workos_profile(workos_user)
    updated_fields = []

    if profile["email"] and user.email != profile["email"]:
        user.email = profile["email"]
        updated_fields.append("email")

    if profile["display_name"] and user.display != profile["display_name"]:
        user.display = profile["display_name"]
        updated_fields.append("display")

    if user.avatar_url != profile["profile_picture_url"]:
        user.avatar_url = profile["profile_picture_url"]
        updated_fields.append("avatar_url")

    if updated_fields:
        user.save(update_fields=sorted(set(updated_fields)))

    return profile


def _serialize_workos_session(workos_session, current_session_id=""):
    session_id = str(_workos_attr(workos_session, "id", "") or "")
    return {
        "id": session_id,
        "status": str(_workos_attr(workos_session, "status", "") or ""),
        "auth_method": str(_workos_attr(workos_session, "auth_method", "") or ""),
        "ip_address": str(_workos_attr(workos_session, "ip_address", "") or ""),
        "user_agent": str(_workos_attr(workos_session, "user_agent", "") or ""),
        "expires_at": str(_workos_attr(workos_session, "expires_at", "") or ""),
        "ended_at": str(_workos_attr(workos_session, "ended_at", "") or ""),
        "created_at": str(_workos_attr(workos_session, "created_at", "") or ""),
        "updated_at": str(_workos_attr(workos_session, "updated_at", "") or ""),
        "is_current": bool(current_session_id and session_id == current_session_id),
    }


def _normalized_allowlist(value):
    if isinstance(value, str):
        value = [value]
    return {item.strip().lower() for item in value if item and item.strip()}


def _workos_superadmin_role_slugs():
    return _normalized_allowlist(settings.WORKOS_SUPERADMIN_ROLE_SLUGS)


def _has_workos_superadmin_role(auth_result):
    role_slugs = {
        str(role_slug).strip().lower()
        for role_slug in getattr(auth_result, "role_slugs", ())
        if str(role_slug).strip()
    }
    return bool(role_slugs & _workos_superadmin_role_slugs())


def _sync_workos_superadmin_access(user, should_be_superadmin):
    if should_be_superadmin:
        user.groups.add(ensure_superadmin_group())
        return

    superadmin_group = Group.objects.filter(name=SUPERADMIN_GROUP_NAME).first()
    if superadmin_group:
        user.groups.remove(superadmin_group)


def _get_unique_user_by_email(email):
    matching_users = list(Users.objects.filter(email__iexact=email))
    if len(matching_users) > 1:
        raise SuspiciousOperation(
            "Multiple Datamingle users share the same email address."
        )
    return matching_users[0] if matching_users else None


def _provision_or_update_workos_user(auth_result):
    if auth_result.organization_id != settings.WORKOS_ORGANIZATION_ID:
        raise SuspiciousOperation("WorkOS returned an unexpected organization.")

    user = Users.objects.filter(workos_user_id=auth_result.user_id).first()
    created = False

    if user is None:
        user = _get_unique_user_by_email(auth_result.email)

    if user is None:
        if Users.objects.filter(username__iexact=auth_result.email).exists():
            raise SuspiciousOperation(
                "Unable to provision a WorkOS user because the username already exists."
            )
        user = Users.objects.create_user(
            username=auth_result.email,
            email=auth_result.email,
            display=auth_result.display_name or auth_result.email,
            is_active=True,
        )
        init_user(user)
        created = True

    if not user.is_active:
        raise SuspiciousOperation("This Datamingle account is inactive.")

    updated_fields = []

    if user.workos_user_id != auth_result.user_id:
        user.workos_user_id = auth_result.user_id
        updated_fields.append("workos_user_id")

    if user.email != auth_result.email:
        user.email = auth_result.email
        updated_fields.append("email")

    display_name = auth_result.display_name or auth_result.email
    if display_name and user.display != display_name:
        user.display = display_name
        updated_fields.append("display")

    avatar_url = auth_result.profile_picture_url or ""
    if user.avatar_url != avatar_url:
        user.avatar_url = avatar_url
        updated_fields.append("avatar_url")

    superuser_allowlist = _normalized_allowlist(settings.WORKOS_SUPERUSER_EMAILS)
    staff_allowlist = _normalized_allowlist(settings.WORKOS_STAFF_EMAILS)
    email = auth_result.email.lower()
    workos_superadmin = _has_workos_superadmin_role(auth_result)
    should_be_superuser = email in superuser_allowlist or workos_superadmin
    should_be_staff = should_be_superuser or email in staff_allowlist

    if user.is_superuser != should_be_superuser:
        user.is_superuser = should_be_superuser
        updated_fields.append("is_superuser")
    if user.is_staff != should_be_staff:
        user.is_staff = should_be_staff
        updated_fields.append("is_staff")

    if updated_fields:
        user.save(update_fields=updated_fields)
    elif created:
        user.save()

    _sync_workos_superadmin_access(user, should_be_superuser)
    return user


class SPATokenRefreshView(TokenRefreshView):
    permission_classes = [permissions.AllowAny]

    def post(self, request, *args, **kwargs):
        response = super().post(request, *args, **kwargs)
        if response.status_code < 400:
            return success_response(
                data=response.data, status_code=response.status_code
            )
        return response


class SPATokenVerifyView(TokenVerifyView):
    permission_classes = [permissions.AllowAny]

    def post(self, request, *args, **kwargs):
        response = super().post(request, *args, **kwargs)
        if response.status_code < 400:
            return success_response(
                data=response.data, status_code=response.status_code
            )
        return response


class WorkOSAuthorizeView(views.APIView):
    permission_classes = [permissions.AllowAny]

    def get(self, request):
        state = secrets.token_urlsafe(32)
        authorization_url = WorkOSAuthClient().get_authorization_url(
            state=state,
            redirect_uri=_workos_callback_uri(request),
        )
        response = HttpResponseRedirect(authorization_url)
        response.set_cookie(
            WORKOS_STATE_COOKIE_NAME,
            state,
            max_age=300,
            httponly=True,
            samesite="Lax",
            secure=_cookie_secure(request),
        )
        return response


class WorkOSCallbackView(views.APIView):
    permission_classes = [permissions.AllowAny]

    def get(self, request):
        code = request.query_params.get("code", "").strip()
        state = request.query_params.get("state", "").strip()
        expected_state = request.COOKIES.get(WORKOS_STATE_COOKIE_NAME, "")

        if not code:
            response = _login_redirect_with_error("Missing WorkOS authorization code.")
            response.delete_cookie(WORKOS_STATE_COOKIE_NAME)
            return response

        if not expected_state or expected_state != state:
            response = _login_redirect_with_error(
                "Invalid WorkOS authentication state."
            )
            response.delete_cookie(WORKOS_STATE_COOKIE_NAME)
            return response

        try:
            auth_result = WorkOSAuthClient().authenticate_with_code(
                code=code,
                ip_address=request.META.get("REMOTE_ADDR", ""),
                user_agent=request.META.get("HTTP_USER_AGENT", ""),
            )
            user = _provision_or_update_workos_user(auth_result)
            exchange_code = secrets.token_urlsafe(32)
            redis_connection = get_redis_connection("default")
            redis_connection.set(
                f"{WORKOS_EXCHANGE_PREFIX}{exchange_code}",
                json.dumps({"user_id": user.pk}),
                ex=60,
            )

            response = HttpResponseRedirect(
                f"/login/callback?{urlencode({'code': exchange_code})}"
            )
            response.set_cookie(
                WORKOS_SESSION_COOKIE_NAME,
                auth_result.session_id,
                max_age=60 * 60 * 24 * 7,
                httponly=True,
                samesite="Lax",
                secure=_cookie_secure(request),
            )
            response.delete_cookie(WORKOS_STATE_COOKIE_NAME)
            return response
        except Exception as exc:
            response = _login_redirect_with_error(
                str(exc) or "WorkOS authentication failed."
            )
            response.delete_cookie(WORKOS_STATE_COOKIE_NAME)
            return response


class WorkOSExchangeSerializer(serializers.Serializer):
    code = serializers.CharField()


class WorkOSExchangeView(views.APIView):
    permission_classes = [permissions.AllowAny]

    def post(self, request):
        serializer = WorkOSExchangeSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        redis_connection = get_redis_connection("default")
        redis_key = f"{WORKOS_EXCHANGE_PREFIX}{serializer.validated_data['code']}"
        payload = redis_connection.get(redis_key)
        if not payload:
            return Response(
                {"errors": "The WorkOS login exchange code is invalid or expired."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        redis_connection.delete(redis_key)

        data = json.loads(payload)
        try:
            user = Users.objects.get(pk=data["user_id"])
        except Users.DoesNotExist:
            return Response(
                {
                    "errors": "The Datamingle user for this WorkOS login no longer exists."
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if not user.is_active:
            return Response(
                {"errors": "This Datamingle account is inactive."},
                status=status.HTTP_403_FORBIDDEN,
            )

        return success_response(data=_issue_local_token_pair(user))


class WorkOSProfileUpdateSerializer(serializers.Serializer):
    first_name = serializers.CharField(
        allow_blank=True, required=True, trim_whitespace=True, max_length=100
    )
    last_name = serializers.CharField(
        allow_blank=True, required=True, trim_whitespace=True, max_length=100
    )

    def validate(self, attrs):
        if not attrs["first_name"] and not attrs["last_name"]:
            raise serializers.ValidationError(
                "Enter at least a first name or last name."
            )
        return attrs


class WorkOSProfileView(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request):
        _require_workos_linked_user(request.user)
        workos_user = WorkOSAuthClient().get_user(request.user.workos_user_id)
        profile = _sync_local_user_from_workos_profile(request.user, workos_user)
        return success_response(data=profile)

    def patch(self, request):
        _require_workos_linked_user(request.user)
        serializer = WorkOSProfileUpdateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        workos_user = WorkOSAuthClient().update_user_profile(
            user_id=request.user.workos_user_id,
            first_name=serializer.validated_data["first_name"],
            last_name=serializer.validated_data["last_name"],
        )
        profile = _sync_local_user_from_workos_profile(request.user, workos_user)
        return success_response(data=profile, detail="WorkOS profile updated.")


class WorkOSSessionsView(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request):
        _require_workos_linked_user(request.user)
        current_session_id = request.COOKIES.get(WORKOS_SESSION_COOKIE_NAME, "")
        sessions_response = WorkOSAuthClient().list_sessions(
            user_id=request.user.workos_user_id
        )
        sessions = _workos_attr(sessions_response, "data", []) or []
        return success_response(
            data=[
                _serialize_workos_session(
                    workos_session=session,
                    current_session_id=current_session_id,
                )
                for session in sessions
            ]
        )


class WorkOSSessionRevokeView(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    def post(self, request, session_id):
        _require_workos_linked_user(request.user)
        current_session_id = request.COOKIES.get(WORKOS_SESSION_COOKIE_NAME, "")
        if current_session_id and session_id == current_session_id:
            return Response(
                {"errors": "Use logout to end the current browser session."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        client = WorkOSAuthClient()
        sessions_response = client.list_sessions(user_id=request.user.workos_user_id)
        sessions = _workos_attr(sessions_response, "data", []) or []
        if not any(_workos_attr(session, "id") == session_id for session in sessions):
            return Response(
                {"errors": "The selected WorkOS session was not found."},
                status=status.HTTP_404_NOT_FOUND,
            )

        client.revoke_session(session_id=session_id)
        return success_response(detail="WorkOS session revoked.")


class WorkOSWebhookView(views.APIView):
    permission_classes = [permissions.AllowAny]
    authentication_classes = []

    @extend_schema(
        summary="WorkOS Webhook",
        description="Receive WorkOS Directory Sync webhooks and reconcile local resource-group membership.",
    )
    def post(self, request):
        event_signature = request.headers.get("WorkOS-Signature", "")

        try:
            event = json.loads(request.body.decode("utf-8"))
            event_payload = _json_safe_workos_value(event)
            _validate_workos_webhook_freshness(event_payload, event_signature)
            task_result = process_workos_webhook_task.delay(event_payload)
            if isinstance(task_result, dict):
                result = task_result
            else:
                result = {
                    "queued": True,
                    "event": str(_workos_attr(event_payload, "event", "") or ""),
                }
        except (ValueError, SuspiciousOperation, ImproperlyConfigured) as exc:
            return Response({"errors": str(exc)}, status=status.HTTP_400_BAD_REQUEST)

        return success_response(data=result, detail="WorkOS webhook queued.")


class WorkOSLogoutView(views.APIView):
    permission_classes = [permissions.AllowAny]

    def get(self, request):
        session_id = request.COOKIES.get(WORKOS_SESSION_COOKIE_NAME, "")
        redirect_url = _workos_logout_return_uri(request)

        if session_id:
            try:
                redirect_url = WorkOSAuthClient().get_logout_url(
                    session_id=session_id,
                    return_to=redirect_url,
                )
            except Exception:
                redirect_url = _workos_logout_return_uri(request)

        response = HttpResponseRedirect(redirect_url)
        response.delete_cookie(WORKOS_SESSION_COOKIE_NAME)
        response.delete_cookie(WORKOS_STATE_COOKIE_NAME)
        return response
