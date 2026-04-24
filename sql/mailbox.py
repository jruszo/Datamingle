from django.contrib.auth.models import Group, Permission
from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from common.utils.const import WorkflowAction, WorkflowStatus
from common.config import SysConfig
from sql.models import (
    ArchiveConfig,
    MailboxCategory,
    MailboxItem,
    MailboxSourceType,
    PermissionRequest,
    SqlWorkflow,
    Users,
    WorkflowLog,
)
from sql.utils.resource_group import user_groups, user_member_groups
from sql.utils.sql_review import can_execute

MAILBOX_BACKFILL_BATCH_SIZE = 1000


def _source_type_for(source):
    if isinstance(source, SqlWorkflow):
        return MailboxSourceType.SQL_WORKFLOW
    if isinstance(source, ArchiveConfig):
        return MailboxSourceType.ARCHIVE
    if isinstance(source, PermissionRequest):
        return MailboxSourceType.PERMISSION_REQUEST
    raise ValueError(f"Unsupported mailbox source: {type(source)!r}")


def _source_id_for(source):
    if isinstance(source, PermissionRequest):
        return source.request_id
    return source.id


def _source_title(source):
    if isinstance(source, SqlWorkflow):
        return source.workflow_name
    return source.title


def _requester_username(source):
    if isinstance(source, SqlWorkflow):
        return source.engineer
    return source.user_name


def _requester_display(source):
    if isinstance(source, SqlWorkflow):
        return source.engineer_display or source.engineer
    return source.user_display or source.user_name


def _action_path_for(source):
    source_id = _source_id_for(source)
    source_type = _source_type_for(source)
    if source_type == MailboxSourceType.SQL_WORKFLOW:
        return f"/workflows/{source_id}"
    if source_type == MailboxSourceType.ARCHIVE:
        return f"/archives/{source_id}"
    return f"/permission-management?requestId={source_id}"


def _base_metadata_for(source):
    metadata = {
        "source_name": _source_title(source),
        "requester_username": _requester_username(source),
        "requester_display": _requester_display(source),
        "status": source.status,
        "status_label": source.get_status_display(),
    }
    if isinstance(source, SqlWorkflow):
        metadata.update(
            {
                "instance_name": source.instance.instance_name,
                "syntax_type": source.syntax_type,
                "is_offline_export": bool(source.is_offline_export),
            }
        )
    elif isinstance(source, ArchiveConfig):
        metadata.update(
            {
                "resource_group_name": source.resource_group.group_name,
                "execution_mode": source.execution_mode,
                "execution_state": source.execution_state,
                "execution_state_label": source.get_execution_state_display(),
            }
        )
    elif isinstance(source, PermissionRequest):
        metadata.update(
            {
                "resource_group_name": source.resource_group.group_name,
                "target_type": source.target_type,
                "instance_name": (
                    source.instance.instance_name if source.instance_id else ""
                ),
            }
        )
    return metadata


def _users_with_any_permission(*permission_codenames):
    permission_filters = Q(is_superuser=True)
    permissions = Permission.objects.filter(codename__in=permission_codenames)
    if permissions.exists():
        permission_filters |= Q(user_permissions__in=permissions)
        permission_filters |= Q(groups__permissions__in=permissions)
    return Users.objects.filter(is_active=True).filter(permission_filters).distinct()


def _active_user_by_username(username):
    if not username:
        return None
    return Users.objects.filter(username=username, is_active=True).first()


def _is_self_audit_blocked(user, source, ban_self_audit):
    if user.is_superuser:
        return False
    return user.username == _requester_username(source) and bool(ban_self_audit)


def _approval_permission_codename(source):
    if isinstance(source, SqlWorkflow):
        return "sql_review"
    if isinstance(source, ArchiveConfig):
        return "archive_review"
    if isinstance(source, PermissionRequest):
        return "query_review"
    raise ValueError(f"Unsupported mailbox source: {type(source)!r}")


def _current_reviewers(source):
    audit = source.get_audit()
    if (
        not audit
        or audit.current_status != WorkflowStatus.WAITING
        or not str(audit.current_audit).strip()
    ):
        return []

    try:
        current_group = Group.objects.get(id=int(audit.current_audit))
    except (Group.DoesNotExist, ValueError):
        return []

    required_permission = _approval_permission_codename(source)
    ban_self_audit = SysConfig().get("ban_self_audit")
    reviewers = []
    seen_usernames = set()
    for user in current_group.user_set.filter(is_active=True):
        member_group_ids = {group.group_id for group in user_member_groups(user)}
        if isinstance(source, SqlWorkflow):
            in_scope = source.group_id in member_group_ids
        else:
            in_scope = source.resource_group_id in member_group_ids
        if not in_scope:
            continue
        if user.username in seen_usernames or _is_self_audit_blocked(
            user, source, ban_self_audit
        ):
            continue
        if not (user.is_superuser or user.has_perm(f"sql.{required_permission}")):
            continue
        seen_usernames.add(user.username)
        reviewers.append(user)
    return reviewers


def _sql_workflow_is_execution_actionable(source):
    return source.status in {"workflow_review_pass", "workflow_timingtask"}


def _archive_is_execution_actionable(source):
    return (
        source.status == WorkflowStatus.PASSED
        and source.state
        and source.execution_state == "idle"
    )


def _archive_can_manage(user, source):
    if user.is_superuser:
        return True
    if not user.has_perm("sql.archive_mgt"):
        return False
    group_ids = {group.group_id for group in user_groups(user)}
    return source.resource_group_id in group_ids


def _execution_needed_recipients(source):
    recipients = {}
    requester = _active_user_by_username(_requester_username(source))
    if requester:
        recipients[requester.id] = requester

    if isinstance(source, SqlWorkflow):
        if not _sql_workflow_is_execution_actionable(source):
            return []
        candidates = _users_with_any_permission(
            "sql_execute",
            "sql_execute_for_resource_group",
        )
        for user in candidates:
            if can_execute(user, source.id):
                recipients[user.id] = user
        return list(recipients.values())

    if isinstance(source, ArchiveConfig):
        if not _archive_is_execution_actionable(source):
            return []
        for user in _users_with_any_permission("archive_mgt"):
            if _archive_can_manage(user, source):
                recipients[user.id] = user
        return list(recipients.values())

    return []


def _latest_human_executor(source):
    audit = source.get_audit()
    if not audit:
        return None
    return (
        WorkflowLog.objects.filter(
            audit_id=audit.audit_id,
            operation_type=WorkflowAction.EXECUTE_START,
        )
        .exclude(operator="")
        .order_by("-id")
        .first()
    )


def _execution_finished_recipients(source, actor=None):
    recipients = {}
    requester = _active_user_by_username(_requester_username(source))
    if requester:
        recipients[requester.id] = requester

    explicit_actor = actor
    if explicit_actor is None:
        log = _latest_human_executor(source)
        if log:
            explicit_actor = _active_user_by_username(log.operator)

    if explicit_actor and explicit_actor.is_active:
        recipients[explicit_actor.id] = explicit_actor
    return list(recipients.values())


def _upsert_action_item(
    recipient,
    category,
    source,
    title,
    body,
    metadata,
    dedupe_key,
):
    item, created = MailboxItem.objects.get_or_create(
        recipient=recipient,
        dedupe_key=dedupe_key,
        defaults={
            "category": category,
            "source_type": _source_type_for(source),
            "source_id": _source_id_for(source),
            "title": title,
            "body": body,
            "action_path": _action_path_for(source),
            "metadata": metadata,
        },
    )
    if created:
        return item

    item.category = category
    item.source_type = _source_type_for(source)
    item.source_id = _source_id_for(source)
    item.title = title
    item.body = body
    item.action_path = _action_path_for(source)
    item.metadata = metadata
    if item.resolved_at is not None:
        item.resolved_at = None
        item.is_unread = True
        item.read_at = None
    item.save(
        update_fields=[
            "category",
            "source_type",
            "source_id",
            "title",
            "body",
            "action_path",
            "metadata",
            "resolved_at",
            "is_unread",
            "read_at",
            "sys_time",
        ]
    )
    return item


def _sync_action_items(source, category, recipients, title, body, metadata):
    with transaction.atomic():
        recipient_ids = {recipient.id for recipient in recipients}
        dedupe_key = f"{category}:{_source_type_for(source)}:{_source_id_for(source)}"
        now = timezone.now()

        for recipient in recipients:
            _upsert_action_item(
                recipient=recipient,
                category=category,
                source=source,
                title=title,
                body=body,
                metadata=metadata,
                dedupe_key=dedupe_key,
            )

        queryset = MailboxItem.objects.filter(
            source_type=_source_type_for(source),
            source_id=_source_id_for(source),
            category=category,
            dedupe_key=dedupe_key,
            resolved_at__isnull=True,
        )
        if recipient_ids:
            queryset = queryset.exclude(recipient_id__in=recipient_ids)
        queryset.update(resolved_at=now, sys_time=now)


def sync_approval_notifications(source, reload=True):
    if reload:
        source = _reload_source(source)
    reviewers = _current_reviewers(source)
    title = f"Approval needed: {_source_title(source)}"
    body = f"{_requester_display(source)} is waiting for review."
    metadata = _base_metadata_for(source)
    metadata["reviewer_count"] = len(reviewers)
    _sync_action_items(
        source=source,
        category=MailboxCategory.APPROVAL_NEEDED,
        recipients=reviewers,
        title=title,
        body=body,
        metadata=metadata,
    )


def sync_execution_needed_notifications(source, reload=True):
    if reload:
        source = _reload_source(source)
    recipients = _execution_needed_recipients(source)
    if isinstance(source, SqlWorkflow):
        body = "This workflow is approved and ready for execution."
    elif isinstance(source, ArchiveConfig):
        body = "This archive is approved and can be run now."
    else:
        body = "Execution is needed."
    metadata = _base_metadata_for(source)
    metadata["recipient_count"] = len(recipients)
    _sync_action_items(
        source=source,
        category=MailboxCategory.EXECUTION_NEEDED,
        recipients=recipients,
        title=f"Execution needed: {_source_title(source)}",
        body=body,
        metadata=metadata,
    )


def emit_execution_finished_notifications(source, outcome, dedupe_suffix, actor=None):
    source = _reload_source(source)
    recipients = _execution_finished_recipients(source, actor=actor)
    if not recipients:
        return []

    normalized_outcome = "success" if outcome == "success" else "failure"
    status_text = (
        "finished successfully"
        if normalized_outcome == "success"
        else "finished with errors"
    )
    metadata = _base_metadata_for(source)
    metadata["outcome"] = normalized_outcome
    created_items = []
    for recipient in recipients:
        created_items.append(
            MailboxItem.objects.update_or_create(
                recipient=recipient,
                dedupe_key=(
                    f"{MailboxCategory.EXECUTION_FINISHED}:"
                    f"{_source_type_for(source)}:{_source_id_for(source)}:{dedupe_suffix}"
                ),
                defaults={
                    "category": MailboxCategory.EXECUTION_FINISHED,
                    "source_type": _source_type_for(source),
                    "source_id": _source_id_for(source),
                    "title": f"Execution finished: {_source_title(source)}",
                    "body": f"This execution {status_text}.",
                    "action_path": _action_path_for(source),
                    "metadata": metadata,
                    "resolved_at": None,
                },
            )[0]
        )
    return created_items


def resolve_mailbox_items(source, category=None):
    source = _reload_source(source)
    now = timezone.now()
    queryset = MailboxItem.objects.filter(
        source_type=_source_type_for(source),
        source_id=_source_id_for(source),
        resolved_at__isnull=True,
    )
    if category:
        queryset = queryset.filter(category=category)
    return queryset.update(resolved_at=now, sys_time=now)


def mark_mailbox_item_read(item):
    if not item.is_unread:
        return item
    item.is_unread = False
    item.read_at = timezone.now()
    item.save(update_fields=["is_unread", "read_at", "sys_time"])
    return item


def mark_all_mailbox_items_read(user):
    now = timezone.now()
    return MailboxItem.objects.filter(recipient=user, is_unread=True).update(
        is_unread=False,
        read_at=now,
        sys_time=now,
    )


def preview_mailbox_items(user, limit=5):
    return (
        MailboxItem.objects.filter(recipient=user)
        .select_related("recipient")
        .order_by("-is_unread", "-create_time", "-id")[:limit]
    )


def _backfill_sources_in_batches(queryset, *sync_handlers, batch_size=None):
    batch_size = batch_size or MAILBOX_BACKFILL_BATCH_SIZE
    last_pk = 0
    ordered_queryset = queryset.order_by("pk")

    while True:
        batch = list(ordered_queryset.filter(pk__gt=last_pk)[:batch_size])
        if not batch:
            return

        with transaction.atomic():
            for source in batch:
                for sync_handler in sync_handlers:
                    sync_handler(source, reload=False)

        last_pk = batch[-1].pk


def backfill_mailbox_notifications():
    _backfill_sources_in_batches(
        SqlWorkflow.objects.select_related("instance"),
        sync_approval_notifications,
        sync_execution_needed_notifications,
    )
    _backfill_sources_in_batches(
        ArchiveConfig.objects.select_related(
            "resource_group",
            "src_instance",
        ),
        sync_approval_notifications,
        sync_execution_needed_notifications,
    )
    _backfill_sources_in_batches(
        PermissionRequest.objects.select_related(
            "resource_group",
            "instance",
        ),
        sync_approval_notifications,
    )


def _reload_source(source):
    if isinstance(source, SqlWorkflow):
        return SqlWorkflow.objects.select_related("instance").get(id=source.id)
    if isinstance(source, ArchiveConfig):
        return ArchiveConfig.objects.select_related(
            "resource_group",
            "src_instance",
        ).get(id=source.id)
    if isinstance(source, PermissionRequest):
        return PermissionRequest.objects.select_related(
            "resource_group",
            "instance",
        ).get(request_id=source.request_id)
    raise ValueError(f"Unsupported mailbox source: {type(source)!r}")
