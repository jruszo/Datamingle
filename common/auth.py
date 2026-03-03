import datetime
import logging
import traceback

import simplejson as json
from django.contrib.sessions.backends.db import SessionStore
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.password_validation import validate_password
from django.core.exceptions import ValidationError
from django.contrib.auth.models import Group
from django.http import HttpResponse, HttpResponseRedirect
from django.urls import reverse

from django.conf import settings
from common.config import SysConfig
from common.utils.ding_api import get_ding_user_id
from sql.models import Users, ResourceGroup, TwoFactorAuthConfig

logger = logging.getLogger("default")


def init_user(user):
    """
    Attach the default resource groups and permission groups to a user.
    :param user:
    :return:
    """
    # Add to default permission groups
    default_auth_group = SysConfig().get("default_auth_group", "")
    if default_auth_group:
        default_auth_group = default_auth_group.split(",")
        [
            user.groups.add(group)
            for group in Group.objects.filter(name__in=default_auth_group)
        ]

    # Add to default resource groups
    default_resource_group = SysConfig().get("default_resource_group", "")
    if default_resource_group:
        default_resource_group = default_resource_group.split(",")
        [
            user.resource_group.add(group)
            for group in ResourceGroup.objects.filter(
                group_name__in=default_resource_group
            )
        ]


class ArcheryAuth(object):
    def __init__(self, request):
        self.request = request
        self.sys_config = SysConfig()

    @staticmethod
    def challenge(username=None, password=None):
        # Validate credentials only; return the user object on success and clear counters
        user = authenticate(username=username, password=password)
        # Login success
        if user:
            # Reset failed login count on successful login
            user.failed_login_count = 0
            user.save()
            return user

    def authenticate(self):
        username = self.request.POST.get("username")
        password = self.request.POST.get("password")
        # Check whether the user already exists
        try:
            user = Users.objects.get(username=username)
        except Users.DoesNotExist:
            authenticated_user = self.challenge(username=username, password=password)
            if authenticated_user:
                # First-login initialization for LDAP users
                init_user(authenticated_user)
                return {"status": 0, "msg": "ok", "data": authenticated_user}
            else:
                return {
                    "status": 1,
                    "msg": "Incorrect username or password. Please try again.",
                    "data": "",
                }
        except:
            logger.error("Error while validating user credentials")
            logger.error(traceback.format_exc())
            return {
                "status": 1,
                "msg": "Service error. Please contact the administrator.",
                "data": "",
            }
        # Existing user: check whether the account is currently locked
        # Load lock configuration
        lock_count = int(self.sys_config.get("lock_cnt_threshold", 5))
        lock_time = int(self.sys_config.get("lock_time_threshold", 60 * 5))
        # Check lock status
        if user.failed_login_count and user.last_login_failed_at:
            if user.failed_login_count >= lock_count:
                now = datetime.datetime.now()
                if (
                    user.last_login_failed_at + datetime.timedelta(seconds=lock_time)
                    > now
                ):
                    return {
                        "status": 3,
                        "msg": (
                            f"Too many failed login attempts. "
                            f"This account is locked. Please retry in about {lock_time} seconds."
                        ),
                        "data": "",
                    }
                else:
                    # Lock period expired: reset failure count
                    user.failed_login_count = 0
                    user.save()
        authenticated_user = self.challenge(username=username, password=password)
        if authenticated_user:
            if not authenticated_user.last_login:
                init_user(authenticated_user)
            return {"status": 0, "msg": "ok", "data": authenticated_user}
        user.failed_login_count += 1
        user.last_login_failed_at = datetime.datetime.now()
        user.save()
        return {
            "status": 1,
            "msg": "Incorrect username or password. Please try again.",
            "data": "",
        }


# AJAX endpoint called by the login page to validate credentials
def authenticate_entry(request):
    """Receive an HTTP request and validate credentials via `ArcheryAuth`."""
    new_auth = ArcheryAuth(request)
    result = new_auth.authenticate()
    if result["status"] == 0:
        authenticated_user = result["data"]
        twofa_enabled = TwoFactorAuthConfig.objects.filter(user=authenticated_user)
        # Whether global 2FA enforcement is enabled
        if SysConfig().get("enforce_2fa"):
            # Whether the user already configured 2FA
            if twofa_enabled:
                verify_mode = "verify_only"
            else:
                verify_mode = "verify_config"
            # Create a non-authenticated session for 2FA flow
            s = SessionStore()
            s["user"] = authenticated_user.username
            s["verify_mode"] = verify_mode
            s.set_expiry(300)
            s.create()
            result = {"status": 0, "msg": "ok", "data": s.session_key}
        else:
            # Whether user has configured 2FA
            if twofa_enabled:
                # Create a non-authenticated session for 2FA verification
                s = SessionStore()
                s["user"] = authenticated_user.username
                s["verify_mode"] = "verify_only"
                s.set_expiry(300)
                s.create()
                result = {"status": 0, "msg": "ok", "data": s.session_key}
            else:
                # No 2FA configured; log in directly
                login(request, authenticated_user)
                # Fetch DingTalk user ID for direct notifications
                if SysConfig().get(
                    "ding_to_person"
                ) is True and "admin" not in request.POST.get("username"):
                    get_ding_user_id(request.POST.get("username"))
                result = {"status": 0, "msg": "ok", "data": None}

    return HttpResponse(json.dumps(result), content_type="application/json")


# Register user
def sign_up(request):
    sign_up_enabled = SysConfig().get("sign_up_enabled", False)
    if not sign_up_enabled:
        result = {
            "status": 1,
            "msg": "Sign-up is disabled. Contact admin.",
            "data": None,
        }
        return HttpResponse(json.dumps(result), content_type="application/json")
    username = request.POST.get("username")
    password = request.POST.get("password")
    password2 = request.POST.get("password2")
    display = request.POST.get("display")
    email = request.POST.get("email")
    result = {"status": 0, "msg": "ok", "data": None}

    if not (username and password):
        result["status"] = 1
        result["msg"] = "Username and password cannot be empty."
    elif len(Users.objects.filter(username=username)) > 0:
        result["status"] = 1
        result["msg"] = "Username already exists."
    elif password != password2:
        result["status"] = 1
        result["msg"] = "The two password entries do not match."
    elif not display:
        result["status"] = 1
        result["msg"] = "Display name is required."
    else:
        # Validate password
        try:
            validate_password(password)
            Users.objects.create_user(
                username=username,
                password=password,
                display=display,
                email=email,
                is_active=1,
                is_staff=True,
            )
        except ValidationError as msg:
            result["status"] = 1
            result["msg"] = str(msg)
    return HttpResponse(json.dumps(result), content_type="application/json")


# Sign out
def sign_out(request):
    user = request.user
    logout(request)
    # If DingTalk auth is enabled, redirect to DingTalk logout page
    if user.ding_user_id and settings.ENABLE_DINGDING:
        return HttpResponseRedirect(
            redirect_to="https://login.dingtalk.com/oauth2/logout"
        )
    return HttpResponseRedirect(reverse("sql:login"))
