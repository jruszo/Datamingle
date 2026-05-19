from django.contrib.auth.models import Group, Permission

from common.config import SysConfig
from sql.models import ResourceGroup

SUPERADMIN_GROUP_NAME = "superadmin"


def ensure_superadmin_group():
    group, _ = Group.objects.get_or_create(name=SUPERADMIN_GROUP_NAME)
    group.permissions.set(Permission.objects.all())
    return group


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
