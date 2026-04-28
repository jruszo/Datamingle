# -*- coding: UTF-8 -*-
from django.utils.deprecation import MiddlewareMixin


class CheckLoginMiddleware(MiddlewareMixin):
    @staticmethod
    def process_request(request):
        """
        Legacy bootstrap pages no longer exist.

        Leave API and admin authentication to their own handlers instead of
        redirecting unauthenticated requests to the removed Django login pages.
        """
        return None
