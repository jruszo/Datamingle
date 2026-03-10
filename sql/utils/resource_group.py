# -*- coding: UTF-8 -*-

import datetime

from django.db.models import Q

from sql.models import (
    Users,
    Instance,
    ResourceGroup,
    TemporaryResourceGroupGrant,
    TemporaryInstanceGrant,
    InstanceAccessLevel,
)

READ_ACCESS_LEVELS = {
    InstanceAccessLevel.QUERY,
    InstanceAccessLevel.QUERY_DML,
    InstanceAccessLevel.QUERY_DML_DDL,
}
WRITE_ACCESS_LEVELS = {
    InstanceAccessLevel.QUERY_DML,
    InstanceAccessLevel.QUERY_DML_DDL,
}
DDL_ACCESS_LEVELS = {InstanceAccessLevel.QUERY_DML_DDL}


def _today():
    return datetime.date.today()


def active_resource_group_grants(user, on_date=None):
    active_on = on_date or _today()
    return TemporaryResourceGroupGrant.objects.filter(
        user=user,
        is_revoked=False,
        valid_date__gte=active_on,
        resource_group__is_deleted=0,
    )


def active_instance_grants(user, on_date=None):
    active_on = on_date or _today()
    return TemporaryInstanceGrant.objects.filter(
        user=user,
        is_revoked=False,
        valid_date__gte=active_on,
    ).select_related("instance", "resource_group")


def _grant_levels_for_tags(tag_codes):
    if not tag_codes:
        return None

    normalized = set(tag_codes)
    if "can_write" in normalized:
        return WRITE_ACCESS_LEVELS
    if "can_read" in normalized:
        return READ_ACCESS_LEVELS
    return set()


def temp_instance_access_level(user, instance, on_date=None):
    grant = (
        active_instance_grants(user, on_date=on_date)
        .filter(instance=instance)
        .order_by("-grant_id")
        .first()
    )
    return grant.access_level if grant else None


def has_any_active_instance_grant(user, on_date=None):
    return active_instance_grants(user, on_date=on_date).exists()


def user_has_group_instance_access(user, instance, tag_codes=None):
    if user.is_superuser:
        return True
    if user.has_perm("sql.query_all_instances") and (
        not tag_codes or "can_read" in set(tag_codes)
    ):
        return True

    queryset = Instance.objects.filter(
        pk=instance.pk, resource_group__in=user_groups(user)
    )
    if tag_codes:
        for tag_code in tag_codes:
            queryset = queryset.filter(
                instance_tag__tag_code=tag_code,
                instance_tag__active=True,
            )
    return queryset.distinct().exists()


def user_has_instance_query_access(user, instance):
    if user_has_group_instance_access(user, instance, tag_codes=["can_read"]):
        return True
    return temp_instance_access_level(user, instance) in READ_ACCESS_LEVELS


def user_has_instance_workflow_access(user, instance, syntax_type):
    if user_has_group_instance_access(user, instance, tag_codes=["can_write"]):
        return True

    access_level = temp_instance_access_level(user, instance)
    if not access_level:
        return False
    if syntax_type == 2:
        return access_level in WRITE_ACCESS_LEVELS
    if syntax_type == 1:
        return access_level in DDL_ACCESS_LEVELS
    return access_level in DDL_ACCESS_LEVELS


def user_groups(user):
    """
    Get list of resource groups associated with the user for access checks.
    This includes active temporary grants.
    :param user:
    :return:
    """
    if user.is_superuser:
        group_list = [group for group in ResourceGroup.objects.filter(is_deleted=0)]
    else:
        group_list = list(
            ResourceGroup.objects.filter(
                Q(users=user)
                | Q(
                    temporaryresourcegroupgrant__user=user,
                    temporaryresourcegroupgrant__is_revoked=False,
                    temporaryresourcegroupgrant__valid_date__gte=_today(),
                ),
                is_deleted=0,
            ).distinct()
        )
    return group_list


def user_member_groups(user):
    """
    Get list of resource groups the user is directly a member of.
    Temporary grants are intentionally excluded for governance checks such as
    approvals and grant management.
    :param user:
    :return:
    """
    if user.is_superuser:
        return [group for group in ResourceGroup.objects.filter(is_deleted=0)]
    return list(ResourceGroup.objects.filter(users=user, is_deleted=0).distinct())


def user_instances(user, type=None, db_type=None, tag_codes=None):
    """
    Get user instance list (indirectly associated through resource groups).
    :param user:
    :param type: Instance type all: all, master: primary, slave: replica
    :param db_type: Database types, ['mysql', 'mssql']
    :param tag_codes: Tag code list, ['can_write', 'can_read']
    :return:
    """
    grant_levels = _grant_levels_for_tags(tag_codes)
    temp_grant_instance_ids = []
    if grant_levels is None:
        temp_grant_instance_ids = list(
            active_instance_grants(user).values_list("instance_id", flat=True)
        )
    elif grant_levels:
        temp_grant_instance_ids = list(
            active_instance_grants(user)
            .filter(access_level__in=grant_levels)
            .values_list("instance_id", flat=True)
        )

    # User with permission to access all instances.
    if user.has_perm("sql.query_all_instances"):
        instances = Instance.objects.all()
    else:
        resource_groups = user_groups(user)
        instances = Instance.objects.filter(
            Q(resource_group__in=resource_groups) | Q(id__in=temp_grant_instance_ids)
        )
    # Filter by type.
    if type:
        instances = instances.filter(type=type)

    # Filter by db_type.
    if db_type:
        instances = instances.filter(db_type__in=db_type)

    # Filter by tag.
    if tag_codes:
        tagged_instances = Instance.objects.filter(pk__in=instances.values("pk"))
        for tag_code in tag_codes:
            tagged_instances = tagged_instances.filter(
                instance_tag__tag_code=tag_code, instance_tag__active=True
            )
        if temp_grant_instance_ids:
            instances = tagged_instances | Instance.objects.filter(
                id__in=temp_grant_instance_ids
            )
        else:
            instances = tagged_instances
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
