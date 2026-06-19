# -*- coding: UTF-8 -*-

import datetime

from django.contrib.auth.models import Group
from django.db.models import Q

from sql.models import (
    Instance,
    InstanceAccessLevel,
    Team,
    TeamMembership,
    TeamPermissionGroup,
    TemporaryInstanceGrant,
    TemporaryTeamGrant,
    Users,
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

LEGACY_ROLE_GROUP_MAP = {
    "query": "QA",
    "workflow_requester": "RD",
    "workflow_approver": "PM",
    "resource_owner": "DBA",
    "team_owner": "DBA",
}


def permission_level_catalog():
    return [
        {
            "id": group.id,
            "name": group.name,
            "permissions": sorted(
                f"{permission.content_type.app_label}.{permission.codename}"
                for permission in group.permissions.select_related("content_type").all()
            ),
        }
        for group in Group.objects.exclude(name="superadmin")
        .prefetch_related("permissions__content_type")
        .order_by("name", "id")
    ]


def normalize_permission_level(value, default=None):
    if isinstance(value, Group):
        return value.id
    value = str(value or "").strip()
    if not value:
        return default
    mapped_name = LEGACY_ROLE_GROUP_MAP.get(value, value)
    queryset = Group.objects.exclude(name="superadmin")
    group = (
        queryset.filter(id=int(mapped_name)).first()
        if mapped_name.isdigit()
        else queryset.filter(name=mapped_name).first()
    )
    return group.id if group else default


def normalize_permission_level_sequence(raw_value):
    if raw_value in (None, ""):
        return []
    values = (
        raw_value if isinstance(raw_value, (list, tuple)) else str(raw_value).split(",")
    )
    group_ids = []
    for value in values:
        group_id = normalize_permission_level(value)
        if group_id and group_id not in group_ids:
            group_ids.append(group_id)
    return group_ids


def permission_level_label(permission_level):
    group_id = normalize_permission_level(permission_level)
    if not group_id:
        return str(permission_level or "")
    return (
        Group.objects.filter(id=group_id).values_list("name", flat=True).first() or ""
    )


def roles_at_or_below(permission_level):
    group_id = normalize_permission_level(permission_level)
    return [str(group_id)] if group_id else []


def _group_has_permission(group_id, permission):
    app_label, codename = permission.split(".", 1)
    return Group.objects.filter(
        id=group_id,
        permissions__content_type__app_label=app_label,
        permissions__codename=codename,
    ).exists()


def user_has_resource_role(user, team, required_permission):
    if user.is_superuser:
        return True
    team_id = getattr(team, "team_id", team)
    permission_level_ids = list(
        TeamMembership.objects.filter(
            user=user,
            team_id=team_id,
            team__is_deleted=0,
        ).values_list("permission_level_id", flat=True)
    )
    permission_level_ids.extend(
        TemporaryTeamGrant.objects.filter(
            user=user,
            team_id=team_id,
            team__is_deleted=0,
            is_revoked=False,
            valid_date__gte=_today(),
        ).values_list("permission_level_id", flat=True)
    )
    return any(
        _group_has_permission(permission_level_id, required_permission)
        for permission_level_id in permission_level_ids
    )


def user_has_any_resource_role(user, required_permission):
    return teams_for_role(user, required_permission).exists()


def user_highest_resource_role(user, team):
    membership = (
        TeamMembership.objects.filter(user=user, team=team)
        .select_related("permission_level")
        .first()
    )
    return membership.permission_level_id if membership else ""


def teams_for_role(user, required_permission=TeamPermissionGroup.QUERY):
    if user.is_superuser:
        return Team.objects.filter(is_deleted=0)
    app_label, codename = required_permission.split(".", 1)
    return Team.objects.filter(
        Q(
            memberships__user=user,
            memberships__permission_level__permissions__content_type__app_label=app_label,
            memberships__permission_level__permissions__codename=codename,
        )
        | Q(
            temporaryteamgrant__user=user,
            temporaryteamgrant__is_revoked=False,
            temporaryteamgrant__valid_date__gte=_today(),
            temporaryteamgrant__permission_level__permissions__content_type__app_label=app_label,
            temporaryteamgrant__permission_level__permissions__codename=codename,
        ),
        is_deleted=0,
    ).distinct()


def resource_role_users(permission_levels, team_id):
    group_ids = normalize_permission_level_sequence(permission_levels)
    if not group_ids:
        return Users.objects.none()
    return (
        Users.objects.filter(
            team_memberships__team_id=team_id,
            team_memberships__permission_level_id__in=group_ids,
            is_active=1,
        )
        .distinct()
        .order_by("display", "username", "id")
    )


def set_user_resource_memberships(user, access_rows, membership_source=None):
    TeamMembership.objects.filter(user=user).delete()
    for row in access_rows:
        team = row.get("team")
        team_id = row.get("team_id") or getattr(team, "team_id", None)
        permission_level_id = normalize_permission_level(
            row.get("permission_level_id") or row.get("permission_level")
        )
        if team_id and permission_level_id:
            TeamMembership.objects.update_or_create(
                user=user,
                team_id=team_id,
                defaults={"permission_level_id": permission_level_id},
            )


def set_team_memberships(team, access_rows, membership_source=None):
    TeamMembership.objects.filter(team=team).delete()
    for row in access_rows:
        user = row.get("user")
        user_id = row.get("user_id") or getattr(user, "id", None)
        permission_level_id = normalize_permission_level(
            row.get("permission_level_id") or row.get("permission_level")
        )
        if user_id and permission_level_id:
            TeamMembership.objects.update_or_create(
                user_id=user_id,
                team=team,
                defaults={"permission_level_id": permission_level_id},
            )


def sync_user_legacy_teams(user):
    return None


def _today():
    return datetime.date.today()


def active_team_grants(user, on_date=None):
    active_on = on_date or _today()
    return TemporaryTeamGrant.objects.filter(
        user=user,
        is_revoked=False,
        valid_date__gte=active_on,
        team__is_deleted=0,
    ).select_related("user", "team", "permission_level")


def active_instance_grants(user, on_date=None):
    active_on = on_date or _today()
    return TemporaryInstanceGrant.objects.filter(
        Q(user=user) | Q(user__isnull=True, team__in=user_member_groups(user)),
        is_revoked=False,
        valid_date__gte=active_on,
    ).select_related("user", "instance", "team")


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
    team_ids = [
        team_id
        for team_id in instance.resource_group.filter(is_deleted=0).values_list(
            "team_id", flat=True
        )
        if user_has_resource_role(user, team_id, TeamPermissionGroup.WORKFLOW_REQUESTER)
    ]
    if (
        team_ids
        and Instance.objects.filter(
            pk=instance.pk,
            resource_group__team_id__in=team_ids,
            instance_tag__tag_code="can_write",
            instance_tag__active=True,
        ).exists()
    ):
        return True
    access_level = temp_instance_access_level(user, instance)
    if syntax_type == 2:
        return access_level in WRITE_ACCESS_LEVELS
    return access_level in DDL_ACCESS_LEVELS


def user_groups(user):
    if user.is_superuser:
        return list(Team.objects.filter(is_deleted=0))
    return list(
        Team.objects.filter(
            Q(memberships__user=user)
            | Q(
                temporaryteamgrant__user=user,
                temporaryteamgrant__is_revoked=False,
                temporaryteamgrant__valid_date__gte=_today(),
            ),
            is_deleted=0,
        ).distinct()
    )


def user_member_groups(user):
    if user.is_superuser:
        return list(Team.objects.filter(is_deleted=0))
    return list(Team.objects.filter(memberships__user=user, is_deleted=0).distinct())


def user_instances(user, type=None, db_type=None, tag_codes=None):
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

    if user.has_perm("sql.query_all_instances"):
        instances = Instance.objects.all()
    else:
        instances = Instance.objects.filter(
            Q(resource_group__in=user_groups(user)) | Q(id__in=temp_grant_instance_ids)
        )
    if type:
        instances = instances.filter(type=type)
    if db_type:
        instances = instances.filter(db_type__in=db_type)
    if tag_codes:
        tagged_instances = Instance.objects.filter(pk__in=instances.values("pk"))
        for tag_code in tag_codes:
            tagged_instances = tagged_instances.filter(
                instance_tag__tag_code=tag_code, instance_tag__active=True
            )
        instances = (
            tagged_instances | Instance.objects.filter(id__in=temp_grant_instance_ids)
            if temp_grant_instance_ids
            else tagged_instances
        )
    return instances.distinct()


def auth_group_users(auth_group_names, team_id):
    return resource_role_users(auth_group_names, team_id)


# Approval-flow storage keeps historical auth-group field names. These aliases keep
# that internal format isolated while active team APIs use permission-level terms.
normalize_permission_group = normalize_permission_level
normalize_permission_group_sequence = normalize_permission_level_sequence
permission_group_label = permission_level_label
