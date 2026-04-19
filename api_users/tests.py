from api_core.legacy_tests import TestTokenAuth2FA, TestUser
from django.test import SimpleTestCase

from api_users.serializers import (
    CurrentUserPasswordChangeSerializer,
    TwoFAVerifySerializer,
    UserAuthSerializer,
    UserManagementCreateSerializer,
)


class UserSerializerTests(SimpleTestCase):
    def test_password_fields_preserve_whitespace(self):
        create_serializer = UserManagementCreateSerializer()
        self.assertFalse(create_serializer.fields["password"].trim_whitespace)

        auth_serializer = UserAuthSerializer()
        self.assertFalse(auth_serializer.fields["password"].trim_whitespace)

        password_change_serializer = CurrentUserPasswordChangeSerializer()
        self.assertFalse(
            password_change_serializer.fields["current_password"].trim_whitespace
        )
        self.assertFalse(
            password_change_serializer.fields["new_password"].trim_whitespace
        )
        self.assertFalse(
            password_change_serializer.fields["new_password_confirm"].trim_whitespace
        )

    def test_two_fa_verify_preserves_leading_zeroes(self):
        serializer = TwoFAVerifySerializer(
            data={"otp": "000123", "phone": "15551234567", "auth_type": "sms"}
        )

        self.assertTrue(serializer.is_valid(), serializer.errors)
        self.assertEqual(serializer.validated_data["otp"], "000123")

    def test_two_fa_verify_rejects_unknown_auth_type(self):
        serializer = TwoFAVerifySerializer(data={"otp": "000123", "auth_type": "email"})

        self.assertFalse(serializer.is_valid())
        self.assertIn("auth_type", serializer.errors)

    def test_two_fa_verify_requires_key_for_totp(self):
        serializer = TwoFAVerifySerializer(data={"otp": "000123", "auth_type": "totp"})

        self.assertFalse(serializer.is_valid())
        self.assertEqual(serializer.errors["errors"][0], "Missing key.")
