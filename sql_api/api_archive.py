# -*- coding: UTF-8 -*-

import datetime
import logging

from django.contrib.auth.models import Group
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
from sql.models import ArchiveConfig, ArchiveLog, Instance, ResourceGroup, WorkflowLog
from sql.notify import notify_for_audit
from sql.utils.resource_group import (
    WRITE_ACCESS_LEVELS,
    active_instance_grants,
    user_groups,
    user_has_group_instance_access,
    user_instances,
    user_member_groups,
)
from sql.utils.workflow_audit import Audit, AuditException, AuditV2, get_auditor

from .pagination import CustomizedPagination
from .response import success_response
from common.task_queue import async_task

logger = logging.getLogger("default")

ARCHIVE_SUPPORTED_DB_TYPES = (
    "mysql",
    "pgsql",
    "mssql",
    "oracle",
    "clickhouse",
    "doris",
)


def _require_archive_module_access(user):
    if user.is_superuser or user.has_perm("sql.menu_archive"):
        return
    raise PermissionDenied("You do not have permission to access archive workflows.")


def _require_archive_apply_access(user):
    _require_archive_module_access(user)
    if user.is_superuser or user.has_perm("sql.archive_apply"):
        return
    raise PermissionDenied("You do not have permission to submit archive workflows.")


def _archive_can_view(user, archive_config):
    if user.is_superuser or archive_config.user_name == user.username:
        return True

    if user.has_perm("sql.archive_review") or user.has_perm("sql.archive_mgt"):
        group_ids = [group.group_id for group in user_groups(user)]
        return archive_config.resource_group_id in group_ids
    return False


def _archive_can_manage(user, archive_config):
    if user.is_superuser:
        return True
    if not user.has_perm("sql.archive_mgt"):
        return False
    group_ids = [group.group_id for group in user_groups(user)]
    return archive_config.resource_group_id in group_ids


def _archive_queryset_for_user(user):
    queryset = ArchiveConfig.objects.select_related(
        "resource_group",
        "src_instance",
    ).all()
    if user.is_superuser:
        return queryset
    if user.has_perm("sql.archive_review") or user.has_perm("sql.archive_mgt"):
        group_ids = [group.group_id for group in user_groups(user)]
        return queryset.filter(resource_group_id__in=group_ids)
    return queryset.filter(user_name=user.username)


def _archive_capable_instances(user):
    return (
        user_instances(user, tag_codes=["can_write"])
        .filter(db_type__in=ARCHIVE_SUPPORTED_DB_TYPES)
        .prefetch_related("resource_group")
        .order_by("instance_name", "id")
    )


def _archive_submission_scope(user):
    can_submit_directly = user.is_superuser or user.has_perm("sql.archive_apply")
    instances = _archive_capable_instances(user)
    direct_group_ids = (
        {group.group_id for group in user_groups(user) if group.is_deleted == 0}
        if can_submit_directly
        else set()
    )
    temporary_groups_by_instance = {}

    for grant in (
        active_instance_grants(user)
        .filter(
            access_level__in=WRITE_ACCESS_LEVELS,
            resource_group__is_deleted=0,
        )
        .select_related("resource_group")
    ):
        groups = temporary_groups_by_instance.setdefault(grant.instance_id, {})
        groups[grant.resource_group_id] = grant.resource_group

    resource_groups = {}
    instance_payload = []
    for instance in instances:
        allowed_groups = {}

        if can_submit_directly and user_has_group_instance_access(
            user, instance, tag_codes=["can_write"]
        ):
            direct_groups = {
                group_id: group_name
                for group_id, group_name in instance.resource_group.filter(
                    is_deleted=0, group_id__in=direct_group_ids
                ).values_list("group_id", "group_name")
            }
            allowed_groups.update(direct_groups)

        for group_id, group in temporary_groups_by_instance.get(
            instance.id, {}
        ).items():
            allowed_groups[group_id] = group.group_name

        if not allowed_groups:
            continue

        sorted_groups = sorted(
            allowed_groups.items(), key=lambda item: (item[1], item[0])
        )
        for group_id, group_name in sorted_groups:
            resource_groups[group_id] = group_name
        instance_payload.append(
            {
                "id": instance.id,
                "instance_name": instance.instance_name,
                "db_type": instance.db_type,
                "type": instance.type,
                "label": f"{instance.instance_name} | {instance.db_type} | {instance.host}",
                "group_ids": [group_id for group_id, _ in sorted_groups],
                "group_names": [group_name for _, group_name in sorted_groups],
                "available_archive_methods": (
                    [ARCHIVE_METHOD_DML, ARCHIVE_METHOD_PT_ARCHIVER]
                    if instance.db_type == "mysql"
                    else [ARCHIVE_METHOD_DML]
                ),
            }
        )

    return {
        "resource_groups": [
            {
                "group_id": group_id,
                "group_name": group_name,
                "label": group_name,
            }
            for group_id, group_name in sorted(
                resource_groups.items(), key=lambda item: (item[1], item[0])
            )
        ],
        "instances": instance_payload,
    }


def _archive_resource_groups(user):
    return _archive_submission_scope(user)["resource_groups"]


def _serialize_archive_review_info(archive_config):
    audit = Audit.detail_by_workflow_id(archive_config.id, WorkflowType.ARCHIVE)
    audit_auth_groups = (
        audit.audit_auth_groups if audit else archive_config.audit_auth_groups or ""
    )
    current_status = audit.current_status if audit else archive_config.status
    current_group_id = None
    if (
        audit
        and current_status == WorkflowStatus.WAITING
        and str(audit.current_audit).strip()
    ):
        current_group_id = int(audit.current_audit)

    review_info = []
    has_met_current_node = False
    for auth_group_id in str(audit_auth_groups).split(","):
        token = str(auth_group_id).strip()
        if not token:
            review_info.append(
                {
                    "group_name": "Auto",
                    "is_current_node": False,
                    "is_passed_node": current_status == WorkflowStatus.PASSED,
                }
            )
            continue

        group = Group.objects.get(id=int(token))
        is_current_node = (
            current_status == WorkflowStatus.WAITING and current_group_id == group.id
        )
        if current_status == WorkflowStatus.WAITING:
            is_passed_node = not has_met_current_node and current_group_id != group.id
            if is_current_node:
                has_met_current_node = True
                is_passed_node = False
            elif not is_passed_node:
                has_met_current_node = True
        else:
            is_passed_node = current_status == WorkflowStatus.PASSED

        review_info.append(
            {
                "group_name": group.name,
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

    current_group_id = int(audit.current_audit)
    reviewers = []
    seen_usernames = set()

    for user in Group.objects.get(id=current_group_id).user_set.filter(is_active=1):
        group_names = [group.group_name for group in user_member_groups(user)]
        if (
            archive_config.resource_group.group_name not in group_names
            or user.username in seen_usernames
        ):
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
        "resource_group": {
            "group_id": archive_config.resource_group.group_id,
            "group_name": archive_config.resource_group.group_name,
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
    resource_groups = serializers.ListField()
    instances = serializers.ListField()
    schedule_frequencies = serializers.ListField()
    weekdays = serializers.ListField()


class ArchiveCreateSerializer(serializers.Serializer):
    title = serializers.CharField(max_length=50)
    group_id = serializers.IntegerField()
    instance_id = serializers.IntegerField()
    db_name = serializers.CharField(max_length=64)
    table_name = serializers.CharField(max_length=64)
    condition = serializers.CharField(max_length=1000)
    archive_method = serializers.ChoiceField(
        choices=[ARCHIVE_METHOD_DML, ARCHIVE_METHOD_PT_ARCHIVER],
        default=ARCHIVE_METHOD_DML,
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
        if instance.db_type != "mysql" and archive_method == ARCHIVE_METHOD_PT_ARCHIVER:
            raise serializers.ValidationError(
                {"errors": "pt-archiver is only available for MySQL instances."}
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
    resource_group_name = serializers.CharField(
        source="resource_group.group_name", read_only=True
    )
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
            "resource_group_name",
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
        description="Return resource groups, archive-capable instances, and scheduler options for the archive SPA.",
    )
    def get(self, request):
        _require_archive_module_access(request.user)
        submission_scope = _archive_submission_scope(request.user)
        payload = {
            "resource_groups": submission_scope["resource_groups"],
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
                name="group_id",
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
        group_id = request.query_params.get("group_id")
        if not group_id:
            raise serializers.ValidationError({"errors": "group_id is required."})
        try:
            group_id = int(group_id)
        except (TypeError, ValueError):
            raise serializers.ValidationError(
                {"errors": "group_id must be an integer."}
            )

        allowed_group_ids = {
            group["group_id"]
            for group in _archive_submission_scope(request.user)["resource_groups"]
        }
        if not request.user.is_superuser and group_id not in allowed_group_ids:
            raise PermissionDenied("You do not have access to this resource group.")

        resource_group = get_object_or_404(ResourceGroup, pk=group_id, is_deleted=0)
        audit_auth_groups = Audit.settings(group_id, WorkflowType.ARCHIVE)
        if audit_auth_groups is None:
            raise serializers.ValidationError(
                {"errors": "Approval flow is not configured for this resource group."}
            )

        if audit_auth_groups == "":
            readable = "No approval required"
            review_info = [
                {
                    "group_name": "Auto",
                    "is_auto_pass": True,
                    "is_current_node": False,
                    "is_passed_node": True,
                }
            ]
        else:
            readable_groups = []
            review_info = []
            for auth_group_id in audit_auth_groups.split(","):
                group = Group.objects.get(id=int(auth_group_id))
                readable_groups.append(group.name)
                review_info.append(
                    {
                        "group_name": group.name,
                        "is_auto_pass": False,
                        "is_current_node": False,
                        "is_passed_node": False,
                    }
                )
            readable = " -> ".join(readable_groups)

        return success_response(
            data={
                "group_id": resource_group.group_id,
                "group_name": resource_group.group_name,
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
        group_id = params.get("group_id", "").strip()

        if status_filter:
            queryset = queryset.filter(status=status_filter)
        if execution_mode:
            queryset = queryset.filter(execution_mode=execution_mode)
        if instance_id:
            queryset = queryset.filter(src_instance_id=instance_id)
        if group_id:
            queryset = queryset.filter(resource_group_id=group_id)
        if search:
            queryset = queryset.filter(
                Q(title__icontains=search)
                | Q(user_display__icontains=search)
                | Q(src_db_name__icontains=search)
                | Q(src_table_name__icontains=search)
                | Q(src_instance__instance_name__icontains=search)
                | Q(resource_group__group_name__icontains=search)
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
        scoped_group_ids = {
            group["group_id"] for group in submission_scope["resource_groups"]
        }

        instance = data["instance"]
        instance_scope = scoped_instances.get(instance.id)
        if instance_scope is None:
            raise PermissionDenied(
                "The selected instance is not associated with your writable scope."
            )
        if not request.user.is_superuser and data["group_id"] not in scoped_group_ids:
            raise PermissionDenied("You do not have access to this resource group.")

        resource_group = get_object_or_404(
            ResourceGroup, pk=data["group_id"], is_deleted=0
        )
        instance_group_ids = set(instance_scope["group_ids"])
        if (
            resource_group.group_id not in instance_group_ids
            and not request.user.is_superuser
        ):
            raise serializers.ValidationError(
                {
                    "errors": "The selected resource group is not available for this instance."
                }
            )

        next_run_at = None
        archive_info = ArchiveConfig(
            title=data["title"],
            resource_group=resource_group,
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
                resource_group=resource_group.group_name,
                resource_group_id=resource_group.group_id,
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
            ArchiveConfig.objects.select_related("resource_group", "src_instance"),
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
            resource_group=archive_config.resource_group.group_name,
            resource_group_id=archive_config.resource_group_id,
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
                raise serializers.ValidationError({"errors": str(exc)})

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
