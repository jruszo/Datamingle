from django.contrib.auth.models import Group, Permission

from common.config import SysConfig

SUPERADMIN_GROUP_NAME = "superadmin"


def ensure_superadmin_group():
    group, _ = Group.objects.get_or_create(name=SUPERADMIN_GROUP_NAME)
    group.permissions.set(Permission.objects.all())
    return group


def init_user(user):
    """
    Attach the default permission groups to a user.
    :param user:
    :return:
    """
    default_auth_group = SysConfig().get("default_auth_group", "")
    if default_auth_group:
        default_auth_group = default_auth_group.split(",")
        [
            user.groups.add(group)
            for group in Group.objects.filter(name__in=default_auth_group)
        ]
