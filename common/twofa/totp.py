from django.db import transaction
from django.http import HttpResponse
from qrcode import QRCode, constants
from sql.models import TwoFactorAuthConfig
from . import TwoFactorAuthBase
from io import BytesIO
import traceback
import logging
import pyotp

logger = logging.getLogger("default")


class TOTP(TwoFactorAuthBase):
    """Time-based One-time Password for apps such as Google Authenticator."""

    def __init__(self, user=None):
        super(TOTP, self).__init__(user=user)
        self.user = user

    def verify(self, otp, key=None):
        """Verify one-time password."""
        result = {"status": 0, "msg": "ok"}
        if key:
            secret_key = key
        else:
            secret_key = TwoFactorAuthConfig.objects.get(
                username=self.user.username, auth_type=self.auth_type
            ).secret_key
        t = pyotp.TOTP(secret_key)
        status = t.verify(otp)
        result["status"] = 0 if status else 1
        result["msg"] = "ok" if status else "Invalid verification code."
        return result

    def generate_key(self):
        """Generate secret key."""
        result = {"status": 0, "msg": "ok", "data": {}}

        # Generate user secret key
        secret_key = pyotp.random_base32(32)
        result["data"] = {"auth_type": self.auth_type, "key": secret_key}

        return result

    def save(self, secret_key):
        """Save 2FA configuration."""
        result = {"status": 0, "msg": "ok"}

        try:
            with transaction.atomic():
                # Remove old 2FA config
                self.disable(self.auth_type)
                # Create new 2FA config
                TwoFactorAuthConfig.objects.create(
                    username=self.user.username,
                    auth_type=self.auth_type,
                    secret_key=secret_key,
                    user=self.user,
                )
        except Exception as msg:
            result["status"] = 1
            result["msg"] = str(msg)
            logger.error(traceback.format_exc())

        return result

    @property
    def auth_type(self):
        """Return auth type code."""
        return "totp"


def generate_qrcode(request, data):
    """Generate and return QR-code image stream."""
    user = request.user

    username = user.username if user.is_authenticated else request.session.get("user")
    secret_key = data

    # Generate QR code
    qr_data = pyotp.totp.TOTP(secret_key).provisioning_uri(
        username, issuer_name="Archery"
    )
    qrcode = QRCode(
        version=1, error_correction=constants.ERROR_CORRECT_L, box_size=6, border=4
    )
    try:
        qrcode.add_data(qr_data)
        qrcode.make(fit=True)
        qr_img = qrcode.make_image()
        buffer = BytesIO()
        qr_img.save(buffer)
        img_stream = buffer.getvalue()
    except Exception as msg:
        logger.error(str(msg))
        logger.error(traceback.format_exc())
    else:
        return HttpResponse(img_stream, content_type="image/png")
