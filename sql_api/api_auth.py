import json
import random
import secrets
import time
from urllib.parse import urlencode

from django.conf import settings
from django.contrib.auth import authenticate
from django.core.exceptions import SuspiciousOperation
from django.http import HttpResponseRedirect
from django_redis import get_redis_connection
from drf_spectacular.utils import extend_schema
from rest_framework import serializers, status, permissions, views
from rest_framework.response import Response
from rest_framework_simplejwt.serializers import TokenObtainPairSerializer
from rest_framework_simplejwt.tokens import RefreshToken
from rest_framework_simplejwt.views import (
    TokenObtainPairView,
    TokenRefreshView,
    TokenVerifyView,
)

from common.auth import init_user
from common.authenticate.workos import WorkOSAuthClient
from common.config import SysConfig
from common.twofa import get_authenticator
from sql.models import TwoFactorAuthConfig, Users
from .response import success_response

WORKOS_STATE_COOKIE_NAME = "datamingle_workos_state"
WORKOS_SESSION_COOKIE_NAME = "datamingle_workos_session_id"
WORKOS_EXCHANGE_PREFIX = "workos-exchange-code:"


def _is_workos_mode():
    return settings.AUTH_MODE == "workos"


def _cookie_secure(request):
    return request.is_secure() or not settings.DEBUG


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
    if (
        auth_result.organization_id
        and auth_result.organization_id != settings.WORKOS_ORGANIZATION_ID
    ):
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


class TokenSMSCaptchaSerializer(serializers.Serializer):
    username = serializers.CharField(label="Username")
    password = serializers.CharField(label="Password")

    def validate(self, attrs):
        username = attrs.get("username")
        password = attrs.get("password")
        user = authenticate(username=username, password=password)
        if not user:
            raise serializers.ValidationError(
                {"errors": "Incorrect username or password."}
            )
        attrs["user"] = user
        return attrs


class SPATokenObtainPairSerializer(TokenObtainPairSerializer):
    otp = serializers.CharField(required=False, label="One-time password/code")
    auth_type = serializers.ChoiceField(
        choices=["totp", "sms"], required=False, label="2FA method"
    )

    def validate(self, attrs):
        otp = attrs.pop("otp", None)
        auth_type = attrs.pop("auth_type", None)
        data = super().validate(attrs)
        user = self.user

        configured_auth_types = sorted(
            set(
                TwoFactorAuthConfig.objects.filter(user=user).values_list(
                    "auth_type", flat=True
                )
            )
        )
        enforce_2fa = bool(SysConfig().get("enforce_2fa", False))
        requires_2fa = bool(configured_auth_types) or enforce_2fa
        if not requires_2fa:
            return data

        if enforce_2fa and not configured_auth_types:
            raise serializers.ValidationError(
                {
                    "errors": "2FA is required but not configured for this account.",
                    "code": "2fa_setup_required",
                }
            )

        if not auth_type:
            raise serializers.ValidationError(
                {
                    "errors": "2FA code is required.",
                    "code": "2fa_required",
                    "available_auth_types": configured_auth_types,
                }
            )

        if auth_type not in configured_auth_types:
            raise serializers.ValidationError(
                {
                    "errors": "Unsupported auth_type for this account.",
                    "code": "2fa_invalid_method",
                    "available_auth_types": configured_auth_types,
                }
            )

        if not otp:
            raise serializers.ValidationError(
                {
                    "errors": "Missing otp.",
                    "code": "2fa_required",
                    "available_auth_types": configured_auth_types,
                }
            )

        authenticator = get_authenticator(user=user, auth_type=auth_type)
        verify_result = authenticator.verify(str(otp))
        if verify_result.get("status") != 0:
            raise serializers.ValidationError(
                {
                    "errors": verify_result.get("msg", "Invalid verification code."),
                    "code": "2fa_invalid",
                }
            )

        return data


class SPATokenObtainPairView(TokenObtainPairView):
    permission_classes = [permissions.AllowAny]
    serializer_class = SPATokenObtainPairSerializer

    def post(self, request, *args, **kwargs):
        if _is_workos_mode():
            return Response(
                {
                    "errors": (
                        "Password login is disabled while WorkOS authentication is active."
                    )
                },
                status=status.HTTP_403_FORBIDDEN,
            )
        response = super().post(request, *args, **kwargs)
        if response.status_code < 400:
            return success_response(
                data=response.data, status_code=response.status_code
            )
        return response


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


class TokenSMSCaptchaView(views.APIView):
    permission_classes = [permissions.AllowAny]

    @extend_schema(
        summary="Request SMS Login OTP",
        request=TokenSMSCaptchaSerializer,
        description="Validate username/password and send an SMS verification code for token login.",
    )
    def post(self, request):
        serializer = TokenSMSCaptchaSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        user = serializer.validated_data["user"]

        try:
            sms_config = TwoFactorAuthConfig.objects.get(user=user, auth_type="sms")
        except TwoFactorAuthConfig.DoesNotExist:
            return Response(
                {"errors": "SMS 2FA is not configured for this account."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        otp = "{:06d}".format(random.randint(0, 999999))
        authenticator = get_authenticator(user=user, auth_type="sms")
        result = authenticator.get_captcha(phone=sms_config.phone, otp=otp)
        if result.get("status") != 0:
            return Response(
                {"errors": result.get("msg", "Failed to send SMS verification code.")},
                status=status.HTTP_400_BAD_REQUEST,
            )

        r = get_redis_connection("default")
        data = {"otp": otp, "update_time": int(time.time())}
        r.set(f"captcha-{sms_config.phone}", json.dumps(data), 300)
        return success_response()


class AuthConfigView(views.APIView):
    permission_classes = [permissions.AllowAny]

    def get(self, request):
        return success_response(data={"mode": settings.AUTH_MODE})


class WorkOSAuthorizeView(views.APIView):
    permission_classes = [permissions.AllowAny]

    def get(self, request):
        if not _is_workos_mode():
            return Response(
                {"errors": "WorkOS authentication is not enabled."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        state = secrets.token_urlsafe(32)
        authorization_url = WorkOSAuthClient().get_authorization_url(state=state)
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
        if not _is_workos_mode():
            return _login_redirect_with_error("WorkOS authentication is not enabled.")

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
        if not _is_workos_mode():
            return Response(
                {"errors": "WorkOS authentication is not enabled."},
                status=status.HTTP_400_BAD_REQUEST,
            )

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
        redirect_url = "/login"

        if _is_workos_mode():
            redirect_url = settings.WORKOS_LOGOUT_REDIRECT_URI
            if session_id:
                try:
                    redirect_url = WorkOSAuthClient().get_logout_url(
                        session_id=session_id
                    )
                except Exception:
                    redirect_url = settings.WORKOS_LOGOUT_REDIRECT_URI

        response = HttpResponseRedirect(redirect_url)
        response.delete_cookie(WORKOS_SESSION_COOKIE_NAME)
        response.delete_cookie(WORKOS_STATE_COOKIE_NAME)
        return response
