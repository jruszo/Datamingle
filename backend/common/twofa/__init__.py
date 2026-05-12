from sql.models import TwoFactorAuthConfig


class TwoFactorAuthBase:
    def __init__(self, user=None):
        self.user = user

    def get_captcha(self):
        """Get verification code."""

    def verify(self, otp):
        """Verify one-time password."""

    def enable(self):
        """Enable."""
        result = {"status": 1, "msg": "failed"}
        return result

    def disable(self, auth_type):
        """Disable."""
        result = {"status": 0, "msg": "ok"}
        try:
            TwoFactorAuthConfig.objects.get(
                user=self.user, auth_type=auth_type
            ).delete()
        except TwoFactorAuthConfig.DoesNotExist as e:
            result = {"status": 0, "msg": str(e)}
        return result

    @property
    def auth_type(self):
        """Return auth type."""
        return "base"


def get_authenticator(user=None, auth_type=None):
    """Get authenticator instance."""
    if auth_type == "totp":
        from .totp import TOTP

        return TOTP(user=user)

    elif auth_type == "sms":
        from .sms import SMS

        return SMS(user=user)
