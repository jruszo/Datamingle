from rest_framework import permissions
from common.config import SysConfig


class IsInUserWhitelist(permissions.BasePermission):
    """
    Custom permission that only allows whitelisted users to call the API.
    """

    def has_permission(self, request, view):
        config = SysConfig().get("api_user_whitelist")
        user_list = config.split(",") if config else []
        api_user_whitelist = [int(uid) for uid in user_list]

        # Only users listed in api_user_whitelist are allowed.
        return request.user.id in api_user_whitelist


class IsOwner(permissions.BasePermission):
    """
    Permission is granted only when the `engineer` parameter matches the request user.
    """

    def has_permission(self, request, view):
        try:
            engineer = request.data["engineer"]
        except KeyError as e:
            return False

        return engineer == request.user.username
