from rest_framework import permissions


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
