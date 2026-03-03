# -*- coding: UTF-8 -*-
import re
from django.http import HttpResponseRedirect
from django.utils.deprecation import MiddlewareMixin

IGNORE_URL = [
    "/login/",
    "/login/2fa/",
    "/authenticate/",
    "/signup/",
    "/api/info",
    "/oidc/callback/",
    "/oidc/authenticate/",
    "/oidc/logout/",
    "/dingding/callback/",
    "/dingding/authenticate/",
    "/cas/authenticate/",
]

IGNORE_URL_RE = r"/api/(v1|auth)/\w+"


class CheckLoginMiddleware(MiddlewareMixin):
    @staticmethod
    def process_request(request):
        """
        Check authentication before each request.
        Redirect unauthenticated users to `/login/`.
        """
        if not request.user.is_authenticated:
            # Whitelisted URLs that should not redirect to login
            if (
                request.path not in IGNORE_URL
                and re.match(IGNORE_URL_RE, request.path) is None
                and not (
                    re.match(r"/user/qrcode/\w+", request.path)
                    and request.session.get("user")
                )
            ):
                return HttpResponseRedirect("/login/")
