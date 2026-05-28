# -*- coding: UTF-8 -*-

import datetime

from django.db.models import Q

from sql.models import (
    Users,
    Instance,
    ResourceGroup,
    ResourceAccessRole,
    ResourceGroupMembership,
    ResourceGroupMembershipSource,
    RESOURCE_ACCESS_ROLE_CATALOG,
    RESOURCE_ACCESS_ROLE_RANKS,
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

LEGACY_AUTH_GROUP_ROLE_MAP = {
    "DBA": ResourceAccessRole.RESOURCE_OWNER,
    "PM": ResourceAccessRole.WORKFLOW_APPROVER,
    "RD": ResourceAccessRole.WORKFLOW_REQUESTER,
    "QA": ResourceAccessRole.QUERY,
    "Default": ResourceAccessRole.QUERY,
}


def access_role_catalog():
    return [dict(role) for role in RESOURCE_ACCESS_ROLE_CATALOG]


def access_role_rank(access_role):
    role_code = normalize_access_role(access_role)
    return RESOURCE_ACCESS_ROLE_RANKS.get(role_code, 0)


def access_role_label(access_role):
    role_code = normalize_access_role(access_role)
    for role in RESOURCE_ACCESS_ROLE_CATALOG:
        if role["code"] == role_code:
            return role["label"]
    return str(access_role)


def normalize_access_role(value, default=ResourceAccessRole.WORKFLOW_APPROVER):
    if value in RESOURCE_ACCESS_ROLE_RANKS:
        return value

    value = str(value or "").strip()
    if not value:
        return ""
    if value in RESOURCE_ACCESS_ROLE_RANKS:
        return value
    if value in LEGACY_AUTH_GROUP_ROLE_MAP:
        return LEGACY_AUTH_GROUP_ROLE_MAP[value]

    try:
        from django.contrib.auth.models import Group

        if value.isdigit():
            group = Group.objects.filter(id=int(value)).first()
        else:
            group = Group.objects.filter(name=value).first()
        if group:
            return LEGACY_AUTH_GROUP_ROLE_MAP.get(group.name, default)
    except Exception:
        pass

    return default


def normalize_access_role_sequence(raw_value):
    if raw_value in (None, ""):
        return []
    if isinstance(raw_value, (list, tuple)):
        values = raw_value
    else:
        values = str(raw_value).split(",")

    roles = []
    for value in values:
        role = normalize_access_role(value)
        if role and role not in roles:
            roles.append(role)
    return roles


def roles_at_or_below(access_role):
    rank = access_role_rank(access_role)
    return [
        role["code"] for role in RESOURCE_ACCESS_ROLE_CATALOG if role["rank"] <= rank
    ]


def user_highest_resource_role(user, resource_group):
    if user.is_superuser:
        return ResourceAccessRole.RESOURCE_OWNER
    group_id = getattr(resource_group, "group_id", resource_group)
    membership = (
        ResourceGroupMembership.objects.filter(
            user=user,
            resource_group_id=group_id,
            resource_group__is_deleted=0,
        )
        .order_by("-access_role")
        .first()
    )
    return membership.access_role if membership else ""


def user_has_resource_role(user, resource_group, minimum_role):
    if user.is_superuser:
        return True
    minimum_role = normalize_access_role(minimum_role, default=minimum_role)
    group_id = getattr(resource_group, "group_id", resource_group)
    memberships = ResourceGroupMembership.objects.filter(
        user=user,
        resource_group_id=group_id,
        resource_group__is_deleted=0,
    )
    return any(
        access_role_rank(membership.access_role) >= access_role_rank(minimum_role)
        for membership in memberships
    )


def user_has_any_resource_role(user, minimum_role):
    if user.is_superuser:
        return ResourceGroup.objects.filter(is_deleted=0).exists()
    minimum_rank = access_role_rank(minimum_role)
    return any(
        access_role_rank(access_role) >= minimum_rank
        for access_role in ResourceGroupMembership.objects.filter(
            user=user, resource_group__is_deleted=0
        ).values_list("access_role", flat=True)
    )


def resource_groups_for_role(user, minimum_role=ResourceAccessRole.QUERY):
    if user.is_superuser:
        return ResourceGroup.objects.filter(is_deleted=0)
    minimum_rank = access_role_rank(minimum_role)
    group_ids = [
        membership.resource_group_id
        for membership in ResourceGroupMembership.objects.filter(
            user=user, resource_group__is_deleted=0
        )
        if access_role_rank(membership.access_role) >= minimum_rank
    ]
    return ResourceGroup.objects.filter(group_id__in=group_ids, is_deleted=0)


def resource_role_users(role_codes, group_id):
    roles = normalize_access_role_sequence(role_codes)
    if not roles:
        return Users.objects.none()
    minimum_rank = min(access_role_rank(role) for role in roles)
    user_ids = [
        membership.user_id
        for membership in ResourceGroupMembership.objects.filter(
            resource_group_id=group_id,
            resource_group__is_deleted=0,
            user__is_active=1,
        ).select_related("user")
        if access_role_rank(membership.access_role) >= minimum_rank
    ]
    return Users.objects.filter(id__in=user_ids, is_active=1).order_by(
        "display", "username", "id"
    )


def sync_user_legacy_resource_groups(user):
    group_ids = ResourceGroupMembership.objects.filter(
        user=user,
        resource_group__is_deleted=0,
    ).values_list("resource_group_id", flat=True)
    user.resource_group.set(ResourceGroup.objects.filter(group_id__in=group_ids))


def set_user_resource_memberships(user, access_rows, membership_source=None):
    source = membership_source or ResourceGroupMembershipSource.DATAMINGLE
    with_source = ResourceGroupMembership.objects.filter(
        user=user, membership_source=source
    )
    with_source.delete()
    for row in access_rows:
        resource_group = row.get("resource_group")
        resource_group_id = row.get("resource_group_id") or getattr(
            resource_group, "group_id", None
        )
        access_role = normalize_access_role(
            row.get("access_role") or ResourceAccessRole.QUERY,
            default=ResourceAccessRole.QUERY,
        )
        if not resource_group_id or not access_role:
            continue
        existing = ResourceGroupMembership.objects.filter(
            user=user,
            resource_group_id=resource_group_id,
        ).first()
        if existing and existing.membership_source != source:
            continue
        ResourceGroupMembership.objects.update_or_create(
            user=user,
            resource_group_id=resource_group_id,
            defaults={
                "access_role": access_role,
                "membership_source": source,
            },
        )
    sync_user_legacy_resource_groups(user)


def set_resource_group_memberships(resource_group, access_rows, membership_source=None):
    source = membership_source or ResourceGroupMembershipSource.DATAMINGLE
    existing_user_ids = set(
        ResourceGroupMembership.objects.filter(
            resource_group=resource_group,
            membership_source=source,
        ).values_list("user_id", flat=True)
    )
    ResourceGroupMembership.objects.filter(
        resource_group=resource_group, membership_source=source
    ).delete()
    changed_user_ids = set(existing_user_ids)
    for row in access_rows:
        user = row.get("user")
        user_id = row.get("user_id") or getattr(user, "id", None)
        access_role = normalize_access_role(
            row.get("access_role") or ResourceAccessRole.QUERY,
            default=ResourceAccessRole.QUERY,
        )
        if not user_id or not access_role:
            continue
        existing = ResourceGroupMembership.objects.filter(
            user_id=user_id,
            resource_group=resource_group,
        ).first()
        if existing and existing.membership_source != source:
            continue
        changed_user_ids.add(user_id)
        ResourceGroupMembership.objects.update_or_create(
            user_id=user_id,
            resource_group=resource_group,
            defaults={
                "access_role": access_role,
                "membership_source": source,
            },
        )
    for user_id in changed_user_ids:
        if user_id:
            sync_user_legacy_resource_groups(Users.objects.get(id=user_id))


def _today():
    return datetime.date.today()


def active_resource_group_grants(user, on_date=None):
    active_on = on_date or _today()
    return TemporaryResourceGroupGrant.objects.filter(
        user=user,
        is_revoked=False,
        valid_date__gte=active_on,
        resource_group__is_deleted=0,
    ).select_related("user", "resource_group")


def active_instance_grants(user, on_date=None):
    active_on = on_date or _today()
    return TemporaryInstanceGrant.objects.filter(
        Q(user=user)
        | Q(user__isnull=True, resource_group__in=user_member_groups(user)),
        is_revoked=False,
        valid_date__gte=active_on,
    ).select_related("user", "instance", "resource_group")


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
    if user.is_superuser:
        return True

    group_ids = [
        group_id
        for group_id in instance.resource_group.filter(is_deleted=0).values_list(
            "group_id", flat=True
        )
        if user_has_resource_role(user, group_id, ResourceAccessRole.WORKFLOW_REQUESTER)
    ]
    if group_ids:
        queryset = Instance.objects.filter(
            pk=instance.pk,
            resource_group__group_id__in=group_ids,
            instance_tag__tag_code="can_write",
            instance_tag__active=True,
        )
        if queryset.exists():
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
                Q(memberships__user=user)
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
    return list(
        ResourceGroup.objects.filter(
            memberships__user=user,
            is_deleted=0,
        ).distinct()
    )


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
    role_codes = normalize_access_role_sequence(auth_group_names)
    return resource_role_users(role_codes, group_id)
