from django.contrib.auth.models import Group, Permission

SUPERADMIN_GROUP_NAME = "superadmin"


def ensure_superadmin_group():
    group, _ = Group.objects.get_or_create(name=SUPERADMIN_GROUP_NAME)
    group.permissions.set(Permission.objects.all())
    return group


def init_user(user):
    """Initialize a user without granting team-scoped permissions globally."""
    return user
