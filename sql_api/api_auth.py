import json
import random
import time

from django.contrib.auth import authenticate
from django_redis import get_redis_connection
from drf_spectacular.utils import extend_schema
from rest_framework import serializers, status, permissions, views
from rest_framework.response import Response
from rest_framework_simplejwt.serializers import TokenObtainPairSerializer
from rest_framework_simplejwt.views import TokenObtainPairView

from common.config import SysConfig
from common.twofa import get_authenticator
from sql.models import TwoFactorAuthConfig


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
            return Response(result, status=status.HTTP_400_BAD_REQUEST)

        r = get_redis_connection("default")
        data = {"otp": otp, "update_time": int(time.time())}
        r.set(f"captcha-{sms_config.phone}", json.dumps(data), 300)
        return Response({"status": 0, "msg": "ok"})
