# -*- coding: UTF-8 -*-

import datetime
import logging

from django.db import transaction
from django.db.models import Q
from django.shortcuts import get_object_or_404
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiTypes
from rest_framework import generics, permissions, serializers, status, views
from rest_framework.exceptions import PermissionDenied

from common.utils.const import WorkflowAction, WorkflowStatus, WorkflowType
from sql.archiver import (
    ARCHIVE_EXECUTION_STATE_IDLE,
    ARCHIVE_EXECUTION_STATE_QUEUED,
    ARCHIVE_EXECUTION_STATE_RUNNING,
    ARCHIVE_EXECUTION_ONE_TIME,
    ARCHIVE_EXECUTION_SCHEDULED,
    ARCHIVE_METHOD_DML,
    ARCHIVE_METHOD_PT_ARCHIVER,
    ARCHIVE_SCHEDULE_DAILY,
    ARCHIVE_SCHEDULE_WEEKLY,
    ARCHIVE_WEEKDAY_ORDER,
    calculate_next_archive_run,
    cancel_archive_schedule,
    get_archive_schedule,
    normalize_archive_weekdays,
    schedule_archive,
    serialize_archive_weekdays,
)
from sql.mailbox import (
    resolve_mailbox_items,
    sync_approval_notifications,
    sync_execution_needed_notifications,
)
from sql.models import (
    ArchiveConfig,
    ArchiveLog,
    Instance,
    Team,
    WorkflowLog,
)
from sql.notify import notify_for_audit
from sql.utils.team import (
    WRITE_ACCESS_LEVELS,
    permission_group_label,
    active_instance_grants,
    normalize_permission_group,
    teams_for_role,
    resource_role_users,
    user_groups,
    user_instances,
)
from sql.utils.workflow_audit import Audit, AuditException, AuditV2, get_auditor

from api_core.pagination import CustomizedPagination
from api_core.response import success_response
from common.task_queue import async_task
from api_agents.models import AgentToolArtifact
from api_agents.services import command_capable_assignment_for_instance

logger = logging.getLogger("default")
ARCHIVE_APPLY_PERMISSION = "sql.archive_apply"
ARCHIVE_REVIEW_PERMISSION = "sql.archive_review"
ARCHIVE_MANAGE_PERMISSION = "sql.archive_mgt"

ARCHIVE_SUPPORTED_DB_TYPES = ("mysql", "pgsql")


def _archive_agent_assignment(instance_id):
    instance = Instance.objects.filter(pk=instance_id).only("db_type").first()
    if instance is None or instance.db_type not in ARCHIVE_SUPPORTED_DB_TYPES:
        return None
    assignment = command_capable_assignment_for_instance(
        instance_id, db_type=instance.db_type
    )
    if assignment is None:
        return None
    if instance.db_type == "pgsql":
        return assignment
    if not AgentToolArtifact.objects.filter(
        enabled=True,
        tool_name=AgentToolArtifact.TOOL_PT_ARCHIVER,
        platform=assignment.agent.platform,
        architecture=assignment.agent.architecture,
    ).exists():
        return None
    return assignment


def _require_archive_agent(archive_config):
    if _archive_agent_assignment(archive_config.src_instance_id) is None:
        if archive_config.src_instance.db_type == "mysql":
            message = (
                "No online command-capable agent with pt-archiver is available "
                "for this instance."
            )
        else:
            message = "No compatible online command-capable agent is available for this instance."
        raise serializers.ValidationError({"errors": message})


def _sync_archive_mailbox_notifications_safe(workflow):
    try:
        sync_approval_notifications(workflow)
        sync_execution_needed_notifications(workflow)
    except Exception:
        logger.exception(
            "Archive mailbox sync failed for archive_id=%s while calling "
            "sync_approval_notifications(workflow) and "
            "sync_execution_needed_notifications(workflow)",
            workflow.id,
        )


def _sync_archive_execution_mailbox_notifications_safe(workflow):
    try:
        sync_execution_needed_notifications(workflow)
    except Exception:
        logger.exception(
            "Archive execution-needed mailbox sync failed for archive_id=%s while "
            "calling sync_execution_needed_notifications(workflow)",
            workflow.id,
        )


def _resolve_archive_mailbox_items_safe(workflow):
    try:
        resolve_mailbox_items(workflow, category="execution_needed")
    except Exception:
        logger.exception(
            "Archive execution-needed mailbox resolution failed for archive_id=%s "
            "while calling resolve_mailbox_items(workflow, "
            "category='execution_needed')",
            workflow.id,
        )


def _require_archive_module_access(user):
    if (
        user.is_superuser
        or user.has_perm("sql.menu_archive")
        or teams_for_role(user, ARCHIVE_APPLY_PERMISSION).exists()
    ):
        return
    raise PermissionDenied("You do not have permission to access archive workflows.")


def _require_archive_apply_access(user):
    _require_archive_module_access(user)
    if user.is_superuser or teams_for_role(user, ARCHIVE_APPLY_PERMISSION).exists():
        return
    raise PermissionDenied("You do not have permission to submit archive workflows.")


def _archive_can_view(user, archive_config):
    if user.is_superuser or archive_config.user_name == user.username:
        return True

    if teams_for_role(user, ARCHIVE_REVIEW_PERMISSION).exists() or user.has_perm(
        ARCHIVE_MANAGE_PERMISSION
    ):
        group_ids = [
            group.team_id for group in teams_for_role(user, ARCHIVE_REVIEW_PERMISSION)
        ]
        if user.has_perm(ARCHIVE_MANAGE_PERMISSION):
            group_ids = [group.team_id for group in user_groups(user)]
        return archive_config.team_id in group_ids
    return False


def _archive_can_manage(user, archive_config):
    if user.is_superuser:
        return True
    if not teams_for_role(user, ARCHIVE_MANAGE_PERMISSION).exists():
        return False
    group_ids = [
        group.team_id for group in teams_for_role(user, ARCHIVE_MANAGE_PERMISSION)
    ]
    return archive_config.team_id in group_ids


def _archive_queryset_for_user(user):
    queryset = ArchiveConfig.objects.select_related(
        "team",
        "src_instance",
    ).all()
    if user.is_superuser:
        return queryset
    if teams_for_role(user, ARCHIVE_REVIEW_PERMISSION).exists() or user.has_perm(
        ARCHIVE_MANAGE_PERMISSION
    ):
        scoped_groups = teams_for_role(user, ARCHIVE_REVIEW_PERMISSION)
        if user.has_perm(ARCHIVE_MANAGE_PERMISSION):
            scoped_groups = user_groups(user)
        group_ids = [group.team_id for group in scoped_groups]
        return queryset.filter(team_id__in=group_ids)
    return queryset.filter(user_name=user.username)


def _archive_capable_instances(user):
    return (
        user_instances(user)
        .filter(db_type__in=ARCHIVE_SUPPORTED_DB_TYPES)
        .prefetch_related("resource_group")
        .order_by("instance_name", "id")
    )


def _archive_submission_scope(user):
    can_submit_directly = (
        user.is_superuser or teams_for_role(user, ARCHIVE_APPLY_PERMISSION).exists()
    )
    instances = _archive_capable_instances(user)
    direct_group_ids = (
        {
            group.team_id
            for group in teams_for_role(user, ARCHIVE_APPLY_PERMISSION)
            if group.is_deleted == 0
        }
        if can_submit_directly
        else set()
    )
    temporary_groups_by_instance = {}

    for grant in (
        active_instance_grants(user)
        .filter(
            access_level__in=WRITE_ACCESS_LEVELS,
            team__is_deleted=0,
        )
        .select_related("team")
    ):
        groups = temporary_groups_by_instance.setdefault(grant.instance_id, {})
        groups[grant.team_id] = grant.team

    teams = {}
    instance_payload = []
    for instance in instances:
        allowed_groups = {}

        if can_submit_directly:
            direct_groups = {
                team_id: team_name
                for team_id, team_name in instance.resource_group.filter(
                    is_deleted=0, team_id__in=direct_group_ids
                ).values_list("team_id", "team_name")
            }
            allowed_groups.update(direct_groups)

        for team_id, group in temporary_groups_by_instance.get(instance.id, {}).items():
            allowed_groups[team_id] = group.team_name

        if not allowed_groups:
            continue

        sorted_groups = sorted(
            allowed_groups.items(), key=lambda item: (item[1], item[0])
        )
        for team_id, team_name in sorted_groups:
            teams[team_id] = team_name
        instance_payload.append(
            {
                "id": instance.id,
                "instance_name": instance.instance_name,
                "db_type": instance.db_type,
                "type": instance.type,
                "label": f"{instance.instance_name} | {instance.db_type} | {instance.host}",
                "team_ids": [team_id for team_id, _ in sorted_groups],
                "team_names": [team_name for _, team_name in sorted_groups],
                "available_archive_methods": (
                    [ARCHIVE_METHOD_PT_ARCHIVER]
                    if instance.db_type == "mysql"
                    else [ARCHIVE_METHOD_DML]
                ),
            }
        )

    return {
        "teams": [
            {
                "team_id": team_id,
                "team_name": team_name,
                "label": team_name,
            }
            for team_id, team_name in sorted(
                teams.items(), key=lambda item: (item[1], item[0])
            )
        ],
        "instances": instance_payload,
    }


def _archive_teams(user):
    return _archive_submission_scope(user)["teams"]


def _serialize_archive_review_info(archive_config):
    audit = Audit.detail_by_workflow_id(archive_config.id, WorkflowType.ARCHIVE)
    audit_auth_groups = (
        audit.audit_auth_groups if audit else archive_config.audit_auth_groups or ""
    )
    current_status = audit.current_status if audit else archive_config.status
    current_role = None
    if (
        audit
        and current_status == WorkflowStatus.WAITING
        and str(audit.current_audit).strip()
    ):
        current_role = normalize_permission_group(audit.current_audit)

    review_info = []
    has_met_current_node = False
    for role in str(audit_auth_groups).split(","):
        token = str(role).strip()
        if not token:
            review_info.append(
                {
                    "team_name": "Auto",
                    "is_current_node": False,
                    "is_passed_node": current_status == WorkflowStatus.PASSED,
                }
            )
            continue

        role = normalize_permission_group(token)
        role_label = permission_group_label(role)
        is_current_node = (
            current_status == WorkflowStatus.WAITING and current_role == role
        )
        if current_status == WorkflowStatus.WAITING:
            is_passed_node = not has_met_current_node and current_role != role
            if is_current_node:
                has_met_current_node = True
                is_passed_node = False
            elif not is_passed_node:
                has_met_current_node = True
        else:
            is_passed_node = current_status == WorkflowStatus.PASSED

        review_info.append(
            {
                "team_name": role_label,
                "is_current_node": is_current_node,
                "is_passed_node": is_passed_node,
            }
        )
    return review_info


def _serialize_archive_current_reviewers(archive_config):
    audit = Audit.detail_by_workflow_id(archive_config.id, WorkflowType.ARCHIVE)
    if (
        not audit
        or audit.current_status != WorkflowStatus.WAITING
        or not str(audit.current_audit).strip()
    ):
        return []

    current_role = normalize_permission_group(audit.current_audit)
    reviewers = []
    seen_usernames = set()

    for user in resource_role_users([current_role], archive_config.team_id):
        if user.username in seen_usernames:
            continue
        seen_usernames.add(user.username)
        reviewers.append(
            {
                "id": user.id,
                "username": user.username,
                "display": user.display,
            }
        )
    return reviewers


def _archive_execution_state_label(archive_config):
    if archive_config.execution_state == ARCHIVE_EXECUTION_STATE_QUEUED:
        return "Queued"
    if archive_config.execution_state == ARCHIVE_EXECUTION_STATE_RUNNING:
        return "Running"
    if archive_config.execution_mode == ARCHIVE_EXECUTION_ONE_TIME:
        if archive_config.last_archive_time and not archive_config.state:
            return "Completed"
        if archive_config.status == WorkflowStatus.PASSED:
            return "Ready"
        return "Pending"
    if archive_config.state:
        return "Enabled"
    return "Disabled"


def _serialize_archive_detail(archive_config, user):
    if not _archive_can_view(user, archive_config):
        raise PermissionDenied(
            "You do not have permission to view this archive workflow."
        )

    audit = Audit.detail_by_workflow_id(archive_config.id, WorkflowType.ARCHIVE)
    logs = []
    last_operation_info = ""
    if audit:
        workflow_logs = WorkflowLog.objects.filter(audit_id=audit.audit_id).order_by(
            "-id"
        )
        logs = [
            {
                "operation_type_desc": log.operation_type_desc,
                "operation_info": log.operation_info,
                "operator_display": log.operator_display,
                "operation_time": log.operation_time,
            }
            for log in workflow_logs
        ]
        if logs:
            last_operation_info = logs[0]["operation_info"]

    archive_logs = []
    for log in ArchiveLog.objects.filter(archive=archive_config).order_by("-id")[:20]:
        archive_logs.append(
            {
                "id": log.id,
                "cmd": log.cmd,
                "condition": log.condition,
                "archive_method": log.archive_method,
                "mode": log.mode,
                "success": log.success,
                "error_info": log.error_info,
                "select_cnt": log.select_cnt,
                "insert_cnt": log.insert_cnt,
                "delete_cnt": log.delete_cnt,
                "start_time": log.start_time,
                "end_time": log.end_time,
                "statistics": log.statistics,
            }
        )

    schedule = get_archive_schedule(archive_config.id)
    current_status = archive_config.status
    can_review_now = False
    if current_status == WorkflowStatus.WAITING:
        can_review_now = Audit.can_review(user, archive_config.id, WorkflowType.ARCHIVE)

    is_manager = _archive_can_manage(user, archive_config)
    can_cancel_now = (
        archive_config.user_name == user.username
        and current_status == WorkflowStatus.WAITING
    )
    can_run_now = (
        is_manager
        and current_status == WorkflowStatus.PASSED
        and archive_config.state
        and archive_config.execution_state == ARCHIVE_EXECUTION_STATE_IDLE
    )
    can_enable = (
        is_manager
        and archive_config.execution_mode == ARCHIVE_EXECUTION_SCHEDULED
        and current_status == WorkflowStatus.PASSED
        and not archive_config.state
    )
    can_disable = (
        is_manager
        and archive_config.execution_mode == ARCHIVE_EXECUTION_SCHEDULED
        and archive_config.state
    )

    return {
        "id": archive_config.id,
        "title": archive_config.title,
        "status": archive_config.status,
        "status_label": archive_config.get_status_display(),
        "execution_state_label": _archive_execution_state_label(archive_config),
        "archive_method": archive_config.archive_method,
        "execution_mode": archive_config.execution_mode,
        "schedule_frequency": archive_config.schedule_frequency,
        "schedule_time": (
            archive_config.schedule_time.strftime("%H:%M")
            if archive_config.schedule_time
            else None
        ),
        "schedule_weekdays": normalize_archive_weekdays(
            archive_config.schedule_weekdays
        ),
        "next_run_at": schedule.next_run if schedule else archive_config.next_run_at,
        "last_archive_time": archive_config.last_archive_time,
        "state": archive_config.state,
        "team": {
            "team_id": archive_config.team.team_id,
            "team_name": archive_config.team.team_name,
        },
        "src_instance": {
            "id": archive_config.src_instance.id,
            "instance_name": archive_config.src_instance.instance_name,
            "db_type": archive_config.src_instance.db_type,
        },
        "src_db_name": archive_config.src_db_name,
        "src_table_name": archive_config.src_table_name,
        "condition": archive_config.condition,
        "sleep": archive_config.sleep,
        "create_time": archive_config.create_time,
        "user_name": archive_config.user_name,
        "user_display": archive_config.user_display,
        "review_info": _serialize_archive_review_info(archive_config),
        "current_reviewers": _serialize_archive_current_reviewers(archive_config),
        "logs": logs,
        "archive_logs": archive_logs,
        "last_operation_info": last_operation_info,
        "is_can_review": can_review_now,
        "is_can_cancel": can_cancel_now,
        "is_can_run_now": can_run_now,
        "is_can_enable": can_enable,
        "is_can_disable": can_disable,
    }


class ArchiveMetadataSerializer(serializers.Serializer):
    teams = serializers.ListField()
    instances = serializers.ListField()
    schedule_frequencies = serializers.ListField()
    weekdays = serializers.ListField()


class ArchiveCreateSerializer(serializers.Serializer):
    title = serializers.CharField(max_length=50)
    team_id = serializers.IntegerField()
    instance_id = serializers.IntegerField()
    db_name = serializers.CharField(max_length=64)
    table_name = serializers.CharField(max_length=64)
    condition = serializers.CharField(max_length=1000)
    archive_method = serializers.ChoiceField(
        choices=[ARCHIVE_METHOD_PT_ARCHIVER, ARCHIVE_METHOD_DML],
        default=ARCHIVE_METHOD_PT_ARCHIVER,
    )
    execution_mode = serializers.ChoiceField(
        choices=[ARCHIVE_EXECUTION_ONE_TIME, ARCHIVE_EXECUTION_SCHEDULED],
        default=ARCHIVE_EXECUTION_ONE_TIME,
    )
    schedule_frequency = serializers.ChoiceField(
        choices=[ARCHIVE_SCHEDULE_DAILY, ARCHIVE_SCHEDULE_WEEKLY],
        required=False,
        allow_null=True,
    )
    schedule_time = serializers.TimeField(
        required=False,
        allow_null=True,
        input_formats=["%H:%M", "iso-8601"],
    )
    schedule_weekdays = serializers.ListField(
        child=serializers.ChoiceField(choices=list(ARCHIVE_WEEKDAY_ORDER)),
        required=False,
        allow_empty=True,
    )
    sleep = serializers.IntegerField(required=False, min_value=0, default=1)

    def validate(self, attrs):
        execution_mode = attrs.get("execution_mode")
        archive_method = attrs.get("archive_method")
        instance_id = attrs.get("instance_id")

        try:
            instance = Instance.objects.get(pk=instance_id)
        except Instance.DoesNotExist:
            raise serializers.ValidationError({"errors": "Instance does not exist."})

        attrs["instance"] = instance
        expected_method = (
            ARCHIVE_METHOD_PT_ARCHIVER
            if instance.db_type == "mysql"
            else ARCHIVE_METHOD_DML
        )
        if instance.db_type not in ARCHIVE_SUPPORTED_DB_TYPES:
            raise serializers.ValidationError(
                {"errors": "Archiving is only available for MySQL and PostgreSQL."}
            )
        if archive_method != expected_method:
            raise serializers.ValidationError(
                {
                    "errors": (
                        "pt-archiver is only available for MySQL; "
                        f"use {expected_method} for {instance.get_db_type_display()}."
                    )
                }
            )
        if execution_mode == ARCHIVE_EXECUTION_SCHEDULED:
            if not attrs.get("schedule_frequency") or not attrs.get("schedule_time"):
                raise serializers.ValidationError(
                    {
                        "errors": (
                            "schedule_frequency and schedule_time are required for scheduled archives."
                        )
                    }
                )
            if attrs["schedule_frequency"] == ARCHIVE_SCHEDULE_WEEKLY and not attrs.get(
                "schedule_weekdays"
            ):
                raise serializers.ValidationError(
                    {"errors": "Weekly schedules require at least one weekday."}
                )
        else:
            attrs["schedule_frequency"] = None
            attrs["schedule_time"] = None
            attrs["schedule_weekdays"] = []
        return attrs


class ArchiveReviewSerializer(serializers.Serializer):
    audit_type = serializers.ChoiceField(choices=["pass", "reject", "cancel"])
    audit_remark = serializers.CharField(required=False, allow_blank=True, default="")


class ArchiveStateSerializer(serializers.Serializer):
    enabled = serializers.BooleanField()


class ArchiveListSerializer(serializers.ModelSerializer):
    status_label = serializers.CharField(source="get_status_display", read_only=True)
    team_name = serializers.CharField(source="team.team_name", read_only=True)
    src_instance_name = serializers.CharField(
        source="src_instance.instance_name", read_only=True
    )

    class Meta:
        model = ArchiveConfig
        fields = (
            "id",
            "title",
            "status",
            "status_label",
            "archive_method",
            "execution_mode",
            "schedule_frequency",
            "state",
            "src_instance_name",
            "src_db_name",
            "src_table_name",
            "team_name",
            "user_display",
            "create_time",
            "last_archive_time",
            "next_run_at",
        )


class ArchiveLogSerializer(serializers.ModelSerializer):
    class Meta:
        model = ArchiveLog
        fields = (
            "id",
            "cmd",
            "condition",
            "archive_method",
            "mode",
            "success",
            "error_info",
            "select_cnt",
            "insert_cnt",
            "delete_cnt",
            "start_time",
            "end_time",
            "statistics",
        )


class ArchiveMetadata(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="Archive Metadata",
        responses={200: ArchiveMetadataSerializer},
        description="Return teams, archive-capable instances, and scheduler options for the archive SPA.",
    )
    def get(self, request):
        _require_archive_module_access(request.user)
        submission_scope = _archive_submission_scope(request.user)
        payload = {
            "teams": submission_scope["teams"],
            "instances": submission_scope["instances"],
            "schedule_frequencies": [
                {"value": ARCHIVE_SCHEDULE_DAILY, "label": "Daily"},
                {"value": ARCHIVE_SCHEDULE_WEEKLY, "label": "Weekly"},
            ],
            "weekdays": [
                {"value": weekday, "label": weekday.title()}
                for weekday in ARCHIVE_WEEKDAY_ORDER
            ],
        }
        serializer = ArchiveMetadataSerializer(payload)
        return success_response(data=serializer.data)


class ArchiveApprovalPreview(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="Archive Approval Preview",
        parameters=[
            OpenApiParameter(
                name="team_id",
                type=OpenApiTypes.INT,
                location=OpenApiParameter.QUERY,
                required=True,
            )
        ],
        responses={200: OpenApiTypes.OBJECT},
        description="Resolve the approval chain for an archive workflow before submission.",
    )
    def get(self, request):
        _require_archive_apply_access(request.user)
        team_id = request.query_params.get("team_id")
        if not team_id:
            raise serializers.ValidationError({"errors": "team_id is required."})
        try:
            team_id = int(team_id)
        except (TypeError, ValueError):
            raise serializers.ValidationError({"errors": "team_id must be an integer."})

        allowed_group_ids = {
            group["team_id"]
            for group in _archive_submission_scope(request.user)["teams"]
        }
        if not request.user.is_superuser and team_id not in allowed_group_ids:
            raise PermissionDenied("You do not have access to this team.")

        team = get_object_or_404(Team, pk=team_id, is_deleted=0)
        audit_auth_groups = Audit.settings(team_id, WorkflowType.ARCHIVE)
        if audit_auth_groups is None:
            raise serializers.ValidationError(
                {"errors": "Approval flow is not configured for this team."}
            )

        if audit_auth_groups == "":
            readable = "No approval required"
            review_info = [
                {
                    "team_name": "Auto",
                    "is_auto_pass": True,
                    "is_current_node": False,
                    "is_passed_node": True,
                }
            ]
        else:
            readable_groups = []
            review_info = []
            for role in audit_auth_groups.split(","):
                role_label = permission_group_label(role)
                readable_groups.append(role_label)
                review_info.append(
                    {
                        "team_name": role_label,
                        "is_auto_pass": False,
                        "is_current_node": False,
                        "is_passed_node": False,
                    }
                )
            readable = " -> ".join(readable_groups)

        return success_response(
            data={
                "team_id": team.team_id,
                "team_name": team.team_name,
                "audit_auth_groups": audit_auth_groups,
                "display": readable,
                "review_info": review_info,
            }
        )


class ArchiveListCreate(generics.ListAPIView):
    permission_classes = [permissions.IsAuthenticated]
    pagination_class = CustomizedPagination
    serializer_class = ArchiveListSerializer

    def get_queryset(self):
        queryset = _archive_queryset_for_user(self.request.user)
        params = self.request.query_params
        search = params.get("search", "").strip()
        status_filter = params.get("status", "").strip()
        execution_mode = params.get("execution_mode", "").strip()
        instance_id = params.get("instance_id", "").strip()
        team_id = params.get("team_id", "").strip()

        if status_filter:
            queryset = queryset.filter(status=status_filter)
        if execution_mode:
            queryset = queryset.filter(execution_mode=execution_mode)
        if instance_id:
            queryset = queryset.filter(src_instance_id=instance_id)
        if team_id:
            queryset = queryset.filter(team_id=team_id)
        if search:
            queryset = queryset.filter(
                Q(title__icontains=search)
                | Q(user_display__icontains=search)
                | Q(src_db_name__icontains=search)
                | Q(src_table_name__icontains=search)
                | Q(src_instance__instance_name__icontains=search)
                | Q(team__team_name__icontains=search)
            )
        return queryset.order_by("-create_time", "-id")

    @extend_schema(
        summary="Archive List",
        responses={200: ArchiveListSerializer},
        description="List visible archive workflows.",
    )
    def get(self, request):
        _require_archive_module_access(request.user)
        return super().get(request)

    @extend_schema(
        summary="Create Archive Workflow",
        request=ArchiveCreateSerializer,
        responses={201: OpenApiTypes.OBJECT},
        description="Submit a one-time or scheduled archive workflow for approval.",
    )
    def post(self, request):
        _require_archive_apply_access(request.user)
        serializer = ArchiveCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data
        submission_scope = _archive_submission_scope(request.user)
        scoped_instances = {
            instance["id"]: instance for instance in submission_scope["instances"]
        }
        scoped_group_ids = {group["team_id"] for group in submission_scope["teams"]}

        instance = data["instance"]
        instance_scope = scoped_instances.get(instance.id)
        if instance_scope is None:
            raise PermissionDenied(
                "The selected instance is not associated with your writable scope."
            )
        if not request.user.is_superuser and data["team_id"] not in scoped_group_ids:
            raise PermissionDenied("You do not have access to this team.")

        team = get_object_or_404(Team, pk=data["team_id"], is_deleted=0)
        instance_group_ids = set(instance_scope["team_ids"])
        if team.team_id not in instance_group_ids and not request.user.is_superuser:
            raise serializers.ValidationError(
                {"errors": "The selected team is not available for this instance."}
            )

        next_run_at = None
        archive_info = ArchiveConfig(
            title=data["title"],
            team=team,
            audit_auth_groups="",
            src_instance=instance,
            src_db_name=data["db_name"],
            src_table_name=data["table_name"],
            dest_instance=None,
            dest_db_name="",
            dest_table_name="",
            condition=data["condition"],
            mode="purge",
            no_delete=False,
            sleep=data["sleep"],
            archive_method=data["archive_method"],
            execution_mode=data["execution_mode"],
            schedule_frequency=data.get("schedule_frequency"),
            schedule_time=data.get("schedule_time"),
            schedule_weekdays=serialize_archive_weekdays(
                data.get("schedule_weekdays", [])
            ),
            next_run_at=None,
            status=WorkflowStatus.WAITING,
            state=False,
            user_name=request.user.username,
            user_display=request.user.display,
        )

        with transaction.atomic():
            audit_handler = get_auditor(
                workflow=archive_info,
                team=team.team_name,
                team_id=team.team_id,
            )
            try:
                audit_handler.create_audit()
            except AuditException as exc:
                logger.exception("Failed to create archive approval flow")
                raise serializers.ValidationError(
                    {"errors": "Failed to create approval flow. Contact admin."}
                ) from exc

            audit_handler.workflow.status = audit_handler.audit.current_status
            if audit_handler.audit.current_status == WorkflowStatus.PASSED:
                audit_handler.workflow.state = True
                if audit_handler.workflow.execution_mode == ARCHIVE_EXECUTION_SCHEDULED:
                    next_run_at = calculate_next_archive_run(audit_handler.workflow)
                    audit_handler.workflow.next_run_at = next_run_at
            audit_handler.workflow.save()

            if (
                audit_handler.workflow.execution_mode == ARCHIVE_EXECUTION_SCHEDULED
                and audit_handler.audit.current_status == WorkflowStatus.PASSED
            ):
                schedule_archive(audit_handler.workflow, run_at=next_run_at)
        _sync_archive_mailbox_notifications_safe(audit_handler.workflow)

        async_task(
            notify_for_audit,
            workflow_audit=audit_handler.audit,
            timeout=60,
            task_name=f"archive-submit-{audit_handler.workflow.id}",
        )
        return success_response(
            data={"id": audit_handler.workflow.id},
            detail="Archive workflow submitted successfully.",
            status_code=status.HTTP_201_CREATED,
        )


class ArchiveDetail(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="Archive Detail",
        responses={200: OpenApiTypes.OBJECT},
        description="Get archive workflow detail including approval flow, execution actions, and archive logs.",
    )
    def get(self, request, archive_id):
        _require_archive_module_access(request.user)
        archive_config = get_object_or_404(
            ArchiveConfig.objects.select_related("team", "src_instance"),
            pk=archive_id,
        )
        return success_response(
            data=_serialize_archive_detail(archive_config, request.user)
        )


class ArchiveReviewCreate(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="Review Archive Workflow",
        request=ArchiveReviewSerializer,
        responses={200: OpenApiTypes.OBJECT},
        description="Approve, reject, or cancel an archive workflow.",
    )
    def post(self, request, archive_id):
        serializer = ArchiveReviewSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data

        archive_config = get_object_or_404(ArchiveConfig, pk=archive_id)
        auditor = get_auditor(
            workflow=archive_config,
            team=archive_config.team.team_name,
            team_id=archive_config.team_id,
        )

        if data["audit_type"] == "pass":
            action = WorkflowAction.PASS
        elif data["audit_type"] == "reject":
            action = WorkflowAction.REJECT
        else:
            action = WorkflowAction.ABORT

        with transaction.atomic():
            try:
                workflow_audit_detail = auditor.operate(
                    action, request.user, data["audit_remark"]
                )
            except AuditException as exc:
                logger.exception(
                    "Archive audit failed for archive_id=%s user=%s action=%s",
                    archive_id,
                    request.user,
                    action,
                )
                raise serializers.ValidationError(
                    {"errors": "Operation failed."}
                ) from None

            auditor.workflow.status = auditor.audit.current_status
            if auditor.audit.current_status == WorkflowStatus.PASSED:
                auditor.workflow.state = True
                if auditor.workflow.execution_mode == ARCHIVE_EXECUTION_SCHEDULED:
                    auditor.workflow.next_run_at = calculate_next_archive_run(
                        auditor.workflow
                    )
            else:
                auditor.workflow.state = False
                auditor.workflow.next_run_at = None
                cancel_archive_schedule(auditor.workflow.id)
            auditor.workflow.save(update_fields=["status", "state", "next_run_at"])

            if (
                auditor.workflow.execution_mode == ARCHIVE_EXECUTION_SCHEDULED
                and auditor.audit.current_status == WorkflowStatus.PASSED
            ):
                schedule_archive(auditor.workflow, run_at=auditor.workflow.next_run_at)
        _sync_archive_mailbox_notifications_safe(auditor.workflow)

        async_task(
            notify_for_audit,
            workflow_audit=auditor.audit,
            workflow_audit_detail=workflow_audit_detail,
            timeout=60,
            task_name=f"archive-review-{archive_id}",
        )
        return success_response(detail="Archive workflow reviewed successfully.")


class ArchiveRunNow(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="Run Archive Workflow Now",
        responses={200: OpenApiTypes.OBJECT},
        description="Queue an approved archive workflow for immediate execution.",
    )
    def post(self, request, archive_id):
        with transaction.atomic():
            archive_config = get_object_or_404(
                ArchiveConfig.objects.select_for_update(),
                pk=archive_id,
            )
            if not _archive_can_manage(request.user, archive_config):
                raise PermissionDenied(
                    "You do not have permission to execute archive workflows."
                )
            if archive_config.status != WorkflowStatus.PASSED:
                raise serializers.ValidationError(
                    {"errors": "Archive workflow is not approved."}
                )
            if not archive_config.state:
                raise serializers.ValidationError(
                    {"errors": "Archive workflow is disabled."}
                )
            if archive_config.execution_state != ARCHIVE_EXECUTION_STATE_IDLE:
                raise serializers.ValidationError(
                    {"errors": "Archive execution is already queued or running."}
                )
            _require_archive_agent(archive_config)

            archive_config.execution_state = ARCHIVE_EXECUTION_STATE_QUEUED
            archive_config.save(update_fields=["execution_state"])

            audit = Audit.detail_by_workflow_id(archive_id, WorkflowType.ARCHIVE)
            if audit:
                Audit.add_log(
                    audit_id=audit.audit_id,
                    operation_type=WorkflowAction.EXECUTE_START,
                    operation_type_desc="Archive Queued",
                    operation_info="Archive workflow queued for execution",
                    operator=request.user.username,
                    operator_display=request.user.display,
                )
            async_task(
                "sql.archiver.archive",
                archive_id,
                "manual",
                hook="sql.archiver.archive_task_callback",
                timeout=-1,
                task_name=f"archive-{archive_id}",
            )
        _resolve_archive_mailbox_items_safe(archive_config)
        return success_response(detail="Archive execution queued.")


class ArchiveStateUpdate(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="Enable or Disable Scheduled Archive",
        request=ArchiveStateSerializer,
        responses={200: OpenApiTypes.OBJECT},
        description="Toggle a scheduled archive workflow on or off.",
    )
    def post(self, request, archive_id):
        serializer = ArchiveStateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        archive_config = get_object_or_404(ArchiveConfig, pk=archive_id)
        if not _archive_can_manage(request.user, archive_config):
            raise PermissionDenied(
                "You do not have permission to manage archive workflows."
            )
        if archive_config.execution_mode != ARCHIVE_EXECUTION_SCHEDULED:
            raise serializers.ValidationError(
                {"errors": "Only scheduled archives can be enabled or disabled."}
            )
        if archive_config.status != WorkflowStatus.PASSED:
            raise serializers.ValidationError(
                {"errors": "Archive workflow is not approved."}
            )

        enabled = serializer.validated_data["enabled"]
        if enabled:
            _require_archive_agent(archive_config)
        with transaction.atomic():
            archive_config.state = enabled
            if enabled:
                archive_config.next_run_at = calculate_next_archive_run(archive_config)
                archive_config.save(update_fields=["state", "next_run_at"])
                schedule_archive(archive_config, run_at=archive_config.next_run_at)
            else:
                archive_config.next_run_at = None
                archive_config.save(update_fields=["state", "next_run_at"])
                cancel_archive_schedule(archive_id)
        _sync_archive_execution_mailbox_notifications_safe(archive_config)

        audit = Audit.detail_by_workflow_id(archive_id, WorkflowType.ARCHIVE)
        if audit:
            Audit.add_log(
                audit_id=audit.audit_id,
                operation_type=WorkflowAction.EXECUTE_SET_TIME,
                operation_type_desc="Archive Schedule Updated",
                operation_info=(
                    "Archive schedule enabled"
                    if enabled
                    else "Archive schedule disabled"
                ),
                operator=request.user.username,
                operator_display=request.user.display,
            )
        return success_response(detail="Archive schedule updated.")


class ArchiveLogList(generics.ListAPIView):
    permission_classes = [permissions.IsAuthenticated]
    pagination_class = CustomizedPagination
    serializer_class = ArchiveLogSerializer

    def get_queryset(self):
        archive_id = self.kwargs["archive_id"]
        archive_config = get_object_or_404(ArchiveConfig, pk=archive_id)
        if not _archive_can_view(self.request.user, archive_config):
            raise PermissionDenied("You do not have permission to view archive logs.")
        return ArchiveLog.objects.filter(archive=archive_id).order_by("-id")

    @extend_schema(
        summary="Archive Logs",
        responses={200: ArchiveLogSerializer},
        description="List execution logs for an archive workflow.",
    )
    def get(self, request, archive_id):
        _require_archive_module_access(request.user)
        return super().get(request)
