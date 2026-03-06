from rest_framework import permissions


class IsOwner(permissions.BasePermission):
    """
    Legacy alias kept for compatibility with existing imports.
    Enforces authenticated requests without trusting request payload ownership fields.
    """

    def has_permission(self, request, view):
        return bool(request.user and request.user.is_authenticated)
