from common.utils.aliyun_sms import AliyunSMS
from django_redis import get_redis_connection
from django.db import transaction
from sql.models import TwoFactorAuthConfig
from . import TwoFactorAuthBase
from common.config import SysConfig
import traceback
import logging
import json
import time

logger = logging.getLogger("default")


class SMS(TwoFactorAuthBase):
    """SMS one-time code verification."""

    def __init__(self, user=None):
        super(SMS, self).__init__(user=user)
        self.user = user

        sms_provider = SysConfig().get("sms_provider", "disabled")
        if sms_provider == "aliyun":
            from common.utils.aliyun_sms import AliyunSMS

            self.client = AliyunSMS()
        elif sms_provider == "tencent":
            from common.utils.tencent_sms import TencentSMS

            self.client = TencentSMS()
        else:
            self.client = None

    def get_captcha(self, **kwargs):
        """Get verification code."""
        result = {"status": 0, "msg": "ok"}
        r = get_redis_connection("default")
        data = r.get(f"captcha-{kwargs['phone']}")
        if data:
            captcha = json.loads(data.decode("utf8"))
            if int(time.time()) - captcha["update_time"] > 60:
                if self.client:
                    result = self.client.send_code(**kwargs)
                else:
                    result = {"status": 1, "msg": "SMS provider is not configured."}
            else:
                result["status"] = 1
                result["msg"] = (
                    "Too many requests for verification codes. "
                    f"Please retry in {captcha['update_time'] - int(time.time()) + 60} seconds."
                )
        else:
            if self.client:
                result = self.client.send_code(**kwargs)
            else:
                result = {"status": 1, "msg": "SMS provider is not configured."}
        return result

    def verify(self, otp, phone=None):
        """Verify OTP code."""
        result = {"status": 0, "msg": "ok"}
        if phone:
            phone = phone
        else:
            phone = TwoFactorAuthConfig.objects.get(username=self.user.username).phone

        r = get_redis_connection("default")
        data = r.get(f"captcha-{phone}")
        if not data:
            result["status"] = 1
            result["msg"] = "Code was not requested or has expired."
        else:
            captcha = json.loads(data.decode("utf8"))
            if otp != captcha["otp"]:
                result["status"] = 1
                result["msg"] = "Invalid verification code."
        return result

    def save(self, phone):
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
                    phone=phone,
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
        return "sms"
