import hashlib
import logging
from datetime import timezone as datetime_timezone

from django.conf import settings
from django.core.exceptions import SuspiciousOperation
from django.db import IntegrityError, transaction
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from common.auth import init_user
from common.authenticate.workos import WorkOSAuthClient
from sql.models import (
    ResourceGroup,
    Users,
    WorkOSDirectoryGroup,
    WorkOSDirectoryGroupMembership,
    WorkOSDirectorySyncEvent,
)

logger = logging.getLogger(__name__)

DIRECTORY_EVENT_TYPES = {
    "dsync.group.created",
    "dsync.group.updated",
    "dsync.group.deleted",
    "dsync.group.user_added",
    "dsync.group.user_removed",
    "dsync.user.created",
    "dsync.user.updated",
    "dsync.user.deleted",
    "dsync.deleted",
}


def _get_attr(source, name, default=""):
    if isinstance(source, dict):
        return source.get(name, default)
    return getattr(source, name, default)


def _workos_id(source):
    return str(_get_attr(source, "id", "") or "").strip()


def _directory_id(source):
    return str(_get_attr(source, "directory_id", "") or "").strip()


def _organization_id(source):
    return str(_get_attr(source, "organization_id", "") or "").strip()


def _validate_organization(source):
    organization_id = _organization_id(source)
    expected_organization_id = settings.WORKOS_ORGANIZATION_ID
    if (
        organization_id
        and expected_organization_id
        and organization_id != expected_organization_id
    ):
        raise SuspiciousOperation(
            "WorkOS Directory Sync event used an unexpected organization."
        )


def _parse_workos_datetime(value):
    if not value:
        return None
    parsed_value = parse_datetime(str(value))
    if parsed_value and timezone.is_aware(parsed_value) and not settings.USE_TZ:
        return timezone.make_naive(parsed_value, datetime_timezone.utc)
    return parsed_value


def _resource_group_name(workos_group):
    raw_name = str(_get_attr(workos_group, "name", "") or "").strip()
    base_name = " ".join(raw_name.split()) or _workos_id(workos_group)
    max_length = ResourceGroup._meta.get_field("group_name").max_length
    if len(base_name) <= max_length:
        return base_name

    digest = hashlib.sha1(base_name.encode("utf-8")).hexdigest()[:12]
    return f"{base_name[: max_length - 13]}-{digest}"


def _directory_user_email(workos_user):
    email = str(_get_attr(workos_user, "email", "") or "").strip().lower()
    if email:
        return email

    emails = _get_attr(workos_user, "emails", []) or []
    for candidate in emails:
        value = str(_get_attr(candidate, "value", "") or "").strip().lower()
        if value:
            return value

    username = str(_get_attr(workos_user, "username", "") or "").strip().lower()
    if "@" in username:
        return username

    return ""


def _directory_user_display_name(workos_user, email):
    first_name = str(_get_attr(workos_user, "first_name", "") or "").strip()
    last_name = str(_get_attr(workos_user, "last_name", "") or "").strip()
    display_name = " ".join(value for value in (first_name, last_name) if value).strip()
    if display_name:
        return display_name[:50]
    return email[:50]


def _get_unique_user_by_email(email):
    matching_users = list(Users.objects.select_for_update().filter(email__iexact=email))
    if len(matching_users) > 1:
        raise SuspiciousOperation(
            "Multiple Datamingle users share the same email address."
        )
    return matching_users[0] if matching_users else None


def _get_or_create_resource_group(workos_group, existing_mapping=None):
    group_name = _resource_group_name(workos_group)
    if existing_mapping:
        resource_group = existing_mapping.resource_group
        if resource_group.group_name == group_name and resource_group.is_deleted == 0:
            return resource_group

        conflicting_group = (
            ResourceGroup.objects.filter(group_name=group_name)
            .exclude(pk=resource_group.pk)
            .first()
        )
        if conflicting_group:
            return conflicting_group

        resource_group.group_name = group_name
        resource_group.is_deleted = 0
        resource_group.save(update_fields=["group_name", "is_deleted"])
        return resource_group

    resource_group, _ = ResourceGroup.objects.update_or_create(
        group_name=group_name,
        defaults={
            "group_parent_id": 0,
            "group_sort": 1,
            "group_level": 1,
            "is_deleted": 0,
        },
    )
    return resource_group


def upsert_directory_group(workos_group):
    _validate_organization(workos_group)
    workos_group_id = _workos_id(workos_group)
    directory_id = _directory_id(workos_group)
    if not workos_group_id or not directory_id:
        raise SuspiciousOperation(
            "WorkOS directory group payload is missing required IDs."
        )

    with transaction.atomic():
        existing_mapping = (
            WorkOSDirectoryGroup.objects.select_related("resource_group")
            .filter(workos_group_id=workos_group_id)
            .first()
        )
        resource_group = _get_or_create_resource_group(workos_group, existing_mapping)
        mapping, _ = WorkOSDirectoryGroup.objects.update_or_create(
            workos_group_id=workos_group_id,
            defaults={
                "directory_id": directory_id,
                "organization_id": _organization_id(workos_group),
                "idp_id": str(_get_attr(workos_group, "idp_id", "") or ""),
                "name": str(_get_attr(workos_group, "name", "") or "").strip()
                or workos_group_id,
                "resource_group": resource_group,
                "is_deleted": False,
                "workos_updated_at": _parse_workos_datetime(
                    _get_attr(workos_group, "updated_at", None)
                ),
            },
        )
    return mapping


def _directory_resource_group_ids(directory_id):
    if not directory_id:
        return set()
    return set(
        WorkOSDirectoryGroup.objects.filter(
            directory_id=directory_id,
            is_deleted=False,
            resource_group__is_deleted=0,
        ).values_list("resource_group_id", flat=True)
    )


def apply_directory_memberships(user):
    resource_groups = [
        membership.directory_group.resource_group
        for membership in WorkOSDirectoryGroupMembership.objects.select_related(
            "directory_group__resource_group"
        ).filter(
            user=user,
            directory_group__is_deleted=False,
            directory_group__resource_group__is_deleted=0,
        )
    ]
    directory_resource_group_ids = _directory_resource_group_ids(
        user.workos_directory_id
    )
    preserved_resource_groups = list(
        user.resource_group.exclude(group_id__in=directory_resource_group_ids)
    )
    user.resource_group.set([*preserved_resource_groups, *resource_groups])


def replace_directory_memberships(user, directory_groups):
    mappings = [upsert_directory_group(group) for group in directory_groups]
    with transaction.atomic():
        WorkOSDirectoryGroupMembership.objects.filter(user=user).exclude(
            directory_group__in=mappings
        ).delete()
        for mapping in mappings:
            WorkOSDirectoryGroupMembership.objects.get_or_create(
                user=user, directory_group=mapping
            )
        apply_directory_memberships(user)
    return mappings


def upsert_directory_user(workos_user):
    _validate_organization(workos_user)
    directory_user_id = _workos_id(workos_user)
    directory_id = _directory_id(workos_user)
    if not directory_user_id or not directory_id:
        raise SuspiciousOperation(
            "WorkOS directory user payload is missing required IDs."
        )

    email = _directory_user_email(workos_user)

    with transaction.atomic():
        user = (
            Users.objects.select_for_update()
            .filter(workos_directory_user_id=directory_user_id)
            .first()
        )
        if user is None and email:
            user = _get_unique_user_by_email(email)

        if user is None:
            if not email:
                logger.info(
                    "Skipping WorkOS directory user without an email: %s",
                    directory_user_id,
                )
                return None
            if (
                Users.objects.select_for_update()
                .filter(username__iexact=email)
                .exists()
            ):
                raise SuspiciousOperation(
                    "Unable to provision a WorkOS directory user because the username already exists."
                )
            try:
                user = Users.objects.create_user(
                    username=email,
                    email=email,
                    display=_directory_user_display_name(workos_user, email),
                    is_active=True,
                )
            except IntegrityError as exc:
                raise SuspiciousOperation(
                    "Unable to provision a WorkOS directory user because the username already exists."
                ) from exc
            init_user(user)
            user.resource_group.clear()

        state = str(_get_attr(workos_user, "state", "") or "").strip().lower()
        next_is_active = state not in {"inactive", "deleted"}
        updated_fields = []

        for field_name, value in (
            ("workos_directory_user_id", directory_user_id),
            ("workos_directory_id", directory_id),
        ):
            if getattr(user, field_name) != value:
                setattr(user, field_name, value)
                updated_fields.append(field_name)

        if not user.workos_directory_managed:
            user.workos_directory_managed = True
            updated_fields.append("workos_directory_managed")

        if email and user.email != email:
            user.email = email
            updated_fields.append("email")

        display_name = _directory_user_display_name(workos_user, email or user.email)
        if display_name and user.display != display_name:
            user.display = display_name
            updated_fields.append("display")

        if user.is_active != next_is_active:
            user.is_active = next_is_active
            updated_fields.append("is_active")

        if updated_fields:
            user.save(update_fields=updated_fields)

        apply_directory_memberships(user)
        return user


def delete_directory_user(workos_user):
    user = upsert_directory_user(workos_user)
    if user is None:
        return None

    with transaction.atomic():
        WorkOSDirectoryGroupMembership.objects.filter(user=user).delete()
        apply_directory_memberships(user)
        if user.is_active:
            user.is_active = False
            user.save(update_fields=["is_active"])
    return user


def add_directory_group_membership(workos_user, workos_group):
    user = upsert_directory_user(workos_user)
    mapping = upsert_directory_group(workos_group)
    if user is None:
        return None

    with transaction.atomic():
        WorkOSDirectoryGroupMembership.objects.get_or_create(
            user=user, directory_group=mapping
        )
        apply_directory_memberships(user)
    return user


def remove_directory_group_membership(workos_user, workos_group):
    user = upsert_directory_user(workos_user)
    mapping = upsert_directory_group(workos_group)
    if user is None:
        return None

    with transaction.atomic():
        WorkOSDirectoryGroupMembership.objects.filter(
            user=user, directory_group=mapping
        ).delete()
        apply_directory_memberships(user)
    return user


def delete_directory_group(workos_group):
    workos_group_id = _workos_id(workos_group)
    if not workos_group_id:
        raise SuspiciousOperation(
            "WorkOS directory group payload is missing required IDs."
        )
    mapping = WorkOSDirectoryGroup.objects.filter(
        workos_group_id=workos_group_id
    ).first()
    if mapping is None:
        mapping = upsert_directory_group(workos_group)
    affected_users = list(
        Users.objects.filter(workos_directory_memberships__directory_group=mapping)
        .distinct()
        .order_by("id")
    )
    with transaction.atomic():
        WorkOSDirectoryGroupMembership.objects.filter(directory_group=mapping).delete()
        mapping.is_deleted = True
        mapping.save(update_fields=["is_deleted", "sys_time"])
        for user in affected_users:
            apply_directory_memberships(user)
    return mapping


def delete_directory(directory_payload):
    directory_id = _workos_id(directory_payload) or _directory_id(directory_payload)
    if not directory_id:
        raise SuspiciousOperation("WorkOS directory payload is missing a directory ID.")

    affected_users = list(
        Users.objects.filter(
            workos_directory_id=directory_id, workos_directory_managed=True
        ).order_by("id")
    )
    with transaction.atomic():
        WorkOSDirectoryGroupMembership.objects.filter(
            directory_group__directory_id=directory_id
        ).delete()
        WorkOSDirectoryGroup.objects.filter(directory_id=directory_id).update(
            is_deleted=True
        )
        for user in affected_users:
            apply_directory_memberships(user)
            if user.is_active:
                user.is_active = False
                user.save(update_fields=["is_active"])


def _event_id(event):
    return str(_get_attr(event, "id", "") or "").strip()


def _event_type(event):
    return str(_get_attr(event, "event", "") or "").strip()


def _event_data(event):
    return _get_attr(event, "data", {}) or {}


def _membership_user(data):
    return _get_attr(data, "user", {}) or {}


def _membership_group(data):
    return _get_attr(data, "group", {}) or {}


def process_directory_event(event):
    event_id = _event_id(event)
    event_type = _event_type(event)

    if not event_id or not event_type:
        raise SuspiciousOperation("WorkOS webhook payload is missing event metadata.")
    if event_type not in DIRECTORY_EVENT_TYPES:
        return {"processed": False, "event": event_type}

    with transaction.atomic():
        _, created = WorkOSDirectorySyncEvent.objects.get_or_create(
            event_id=event_id, defaults={"event_type": event_type}
        )
        if not created:
            return {"processed": False, "event": event_type, "duplicate": True}

        data = _event_data(event)
        if event_type in {"dsync.group.created", "dsync.group.updated"}:
            upsert_directory_group(data)
        elif event_type == "dsync.group.deleted":
            delete_directory_group(data)
        elif event_type in {"dsync.user.created", "dsync.user.updated"}:
            upsert_directory_user(data)
        elif event_type == "dsync.user.deleted":
            delete_directory_user(data)
        elif event_type == "dsync.group.user_added":
            add_directory_group_membership(
                _membership_user(data), _membership_group(data)
            )
        elif event_type == "dsync.group.user_removed":
            remove_directory_group_membership(
                _membership_user(data), _membership_group(data)
            )
        elif event_type == "dsync.deleted":
            delete_directory(data)

    return {"processed": True, "event": event_type}


def sync_directory(directory_id, client=None):
    client = client or WorkOSAuthClient()
    seen_group_ids = set()
    seen_user_ids = set()

    for workos_group in client.list_directory_groups(directory_id=directory_id):
        mapping = upsert_directory_group(workos_group)
        seen_group_ids.add(mapping.workos_group_id)

    for workos_user in client.list_directory_users(directory_id=directory_id):
        user = upsert_directory_user(workos_user)
        if user is None:
            continue
        seen_user_ids.add(user.workos_directory_user_id)
        user_groups = list(
            client.list_directory_groups_for_user(
                directory_user_id=user.workos_directory_user_id
            )
        )
        for group in user_groups:
            seen_group_ids.add(_workos_id(group))
        replace_directory_memberships(user, user_groups)

    stale_groups = WorkOSDirectoryGroup.objects.filter(
        directory_id=directory_id, is_deleted=False
    ).exclude(workos_group_id__in=seen_group_ids)
    for mapping in stale_groups:
        delete_directory_group(
            {
                "id": mapping.workos_group_id,
                "directory_id": mapping.directory_id,
                "organization_id": mapping.organization_id,
                "name": mapping.name,
            }
        )

    stale_users = Users.objects.filter(
        workos_directory_id=directory_id, workos_directory_managed=True
    ).exclude(workos_directory_user_id__in=seen_user_ids)
    for user in stale_users:
        with transaction.atomic():
            WorkOSDirectoryGroupMembership.objects.filter(user=user).delete()
            apply_directory_memberships(user)
            if user.is_active:
                user.is_active = False
                user.save(update_fields=["is_active"])

    return {
        "groups": len(seen_group_ids),
        "users": len(seen_user_ids),
        "stale_groups": stale_groups.count(),
        "stale_users": stale_users.count(),
    }
