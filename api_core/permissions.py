from rest_framework import permissions


class IsOwner(permissions.BasePermission):
    """
    Legacy alias kept for compatibility with existing imports.
    Enforces authenticated requests without trusting request payload ownership fields.
    """

    def has_permission(self, request, view):
        return bool(request.user and request.user.is_authenticated)


class IsStaffOrSuperuser(permissions.BasePermission):
    """Allow authenticated staff members and superusers."""

    message = "Only staff members or superusers can access system settings."

    def has_permission(self, request, view):
        user = request.user
        return bool(
            user and user.is_authenticated and (user.is_staff or user.is_superuser)
        )
