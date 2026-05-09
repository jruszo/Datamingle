import json
import secrets
from urllib.parse import urlencode

from django.conf import settings
from django.core.exceptions import SuspiciousOperation
from django.http import HttpResponseRedirect
from django_redis import get_redis_connection
from rest_framework import serializers, status, permissions, views
from rest_framework.response import Response
from rest_framework_simplejwt.tokens import RefreshToken
from rest_framework_simplejwt.views import TokenRefreshView, TokenVerifyView

from common.auth import init_user
from common.authenticate.workos import WorkOSAuthClient
from sql.models import Users
from api_core.response import success_response

WORKOS_STATE_COOKIE_NAME = "datamingle_workos_state"
WORKOS_SESSION_COOKIE_NAME = "datamingle_workos_session_id"
WORKOS_EXCHANGE_PREFIX = "workos-exchange-code:"


def _cookie_secure(request):
    return request.is_secure() or not settings.DEBUG


def _workos_callback_uri(request):
    return request.build_absolute_uri("/api/auth/workos/callback/")


def _workos_logout_return_uri(request):
    return request.build_absolute_uri("/login")


def _login_redirect_with_error(message):
    query = urlencode({"error": message})
    return HttpResponseRedirect(f"/login?{query}")


def _issue_local_token_pair(user):
    refresh = RefreshToken.for_user(user)
    return {"access": str(refresh.access_token), "refresh": str(refresh)}


def _normalized_allowlist(value):
    return {item.strip().lower() for item in value if item and item.strip()}


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

    if email in superuser_allowlist:
        if not user.is_superuser:
            user.is_superuser = True
            updated_fields.append("is_superuser")
        if not user.is_staff:
            user.is_staff = True
            updated_fields.append("is_staff")
    elif email in staff_allowlist and not user.is_staff:
        user.is_staff = True
        updated_fields.append("is_staff")

    if updated_fields:
        user.save(update_fields=updated_fields)
    elif created:
        user.save()

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
