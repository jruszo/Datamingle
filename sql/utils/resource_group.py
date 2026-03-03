# -*- coding: UTF-8 -*-

from sql.models import Users, Instance, ResourceGroup


def user_groups(user):
    """
    Get list of resource groups associated with the user.
    :param user:
    :return:
    """
    if user.is_superuser:
        group_list = [group for group in ResourceGroup.objects.filter(is_deleted=0)]
    else:
        group_list = [
            group
            for group in Users.objects.get(id=user.id).resource_group.filter(
                is_deleted=0
            )
        ]
    return group_list


def user_instances(user, type=None, db_type=None, tag_codes=None):
    """
    Get user instance list (indirectly associated through resource groups).
    :param user:
    :param type: Instance type all: all, master: primary, slave: replica
    :param db_type: Database types, ['mysql', 'mssql']
    :param tag_codes: Tag code list, ['can_write', 'can_read']
    :return:
    """
    # User with permission to access all instances.
    if user.has_perm("sql.query_all_instances"):
        instances = Instance.objects.all()
    else:
        # First, get resource groups associated with the user.
        resource_groups = ResourceGroup.objects.filter(users=user, is_deleted=0)
        # Then, get instances.
        instances = Instance.objects.filter(resource_group__in=resource_groups)
    # Filter by type.
    if type:
        instances = instances.filter(type=type)

    # Filter by db_type.
    if db_type:
        instances = instances.filter(db_type__in=db_type)

    # Filter by tag.
    if tag_codes:
        for tag_code in tag_codes:
            instances = instances.filter(
                instance_tag__tag_code=tag_code, instance_tag__active=True
            )
    return instances.distinct()


def auth_group_users(auth_group_names, group_id):
    """
    Get users in a resource group associated with specified permission groups.
    :param auth_group_names: Permission group name list
    :param group_id: Resource group ID
    :return:
    """
    # Get users associated with the resource group.
    users = ResourceGroup.objects.get(group_id=group_id).users_set.all()
    # Filter users belonging to specified permission groups.
    users = users.filter(groups__name__in=auth_group_names, is_active=1)
    return users
