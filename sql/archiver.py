# -*- coding: UTF-8 -*-
"""
@author: hhyo
@license: Apache Licence
@file: archive.py
@time: 2020/01/10
"""

import datetime
import logging
import os
import re
import traceback
import time

import simplejson as json
import sqlparse
from django.conf import settings
from django.contrib.auth.decorators import permission_required
from django.db import transaction, connection, close_old_connections
from django.db.models import Q, Value as V, TextField
from django.db.models.functions import Concat
from django.http import HttpResponse, JsonResponse, HttpResponseRedirect
from django.shortcuts import render
from django.urls import reverse
from django.utils import timezone

from common.task_queue import async_task, schedule, delete_schedule, task_info

from common.utils.const import WorkflowStatus, WorkflowType, WorkflowAction
from common.utils.extend_json_encoder import ExtendJSONEncoder
from common.utils.timer import FuncTimer
from sql.engines import get_engine
from sql.engines.models import ReviewSet, ReviewResult
from sql.notify import notify_for_audit
from sql.plugins.pt_archiver import PtArchiver
from sql.utils.resource_group import user_instances, user_groups
from sql.models import ArchiveConfig, ArchiveLog, Instance, ResourceGroup
from sql.utils.workflow_audit import get_auditor, AuditException, Audit

logger = logging.getLogger("default")
__author__ = "hhyo"

ARCHIVE_METHOD_DML = "dml"
ARCHIVE_METHOD_PT_ARCHIVER = "pt_archiver"
ARCHIVE_EXECUTION_ONE_TIME = "one_time"
ARCHIVE_EXECUTION_SCHEDULED = "scheduled"
ARCHIVE_SCHEDULE_DAILY = "daily"
ARCHIVE_SCHEDULE_WEEKLY = "weekly"
ARCHIVE_WEEKDAY_ORDER = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
ARCHIVE_WEEKDAY_INDEX = {
    value: index for index, value in enumerate(ARCHIVE_WEEKDAY_ORDER)
}
ARCHIVE_SCHEDULE_PREFIX = "archive-timing"
ARCHIVE_CONDITION_PATTERN = re.compile(r"\{\{\s*([a-z_]+)\s*\}\}")
ARCHIVE_SAFE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_$]+$")


def archive_schedule_name(archive_id):
    return f"{ARCHIVE_SCHEDULE_PREFIX}-{archive_id}"


def _schedule_datetime(date_value, time_value):
    candidate = datetime.datetime.combine(date_value, time_value)
    if settings.USE_TZ:
        return timezone.make_aware(candidate, timezone.get_current_timezone())
    return candidate


def _local_reference_time(value=None):
    current = value or timezone.now()
    if settings.USE_TZ:
        if timezone.is_naive(current):
            current = timezone.make_aware(current, timezone.get_current_timezone())
        return timezone.localtime(current, timezone.get_current_timezone())
    if timezone.is_aware(current):
        return timezone.make_naive(current, timezone.get_current_timezone())
    return current


def normalize_archive_weekdays(value):
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        candidates = value
    else:
        candidates = str(value).split(",")

    normalized = []
    for candidate in candidates:
        token = str(candidate).strip().lower()
        if not token:
            continue
        token = token[:3]
        if token not in ARCHIVE_WEEKDAY_INDEX:
            raise ValueError(f"Unsupported weekday value: {candidate}")
        if token not in normalized:
            normalized.append(token)
    normalized.sort(key=lambda item: ARCHIVE_WEEKDAY_INDEX[item])
    return normalized


def serialize_archive_weekdays(value):
    return ",".join(normalize_archive_weekdays(value))


def render_archive_condition(condition, now=None):
    if not condition or not str(condition).strip():
        raise ValueError("Archive condition cannot be empty.")

    local_now = _local_reference_time(now)
    local_date = local_now.date()
    variable_map = {
        "today": f"'{local_date.isoformat()}'",
        "yesterday": f"'{(local_date - datetime.timedelta(days=1)).isoformat()}'",
        "tomorrow": f"'{(local_date + datetime.timedelta(days=1)).isoformat()}'",
        "now": f"'{local_now.strftime('%Y-%m-%d %H:%M:%S')}'",
    }

    def _replace(match):
        name = match.group(1).lower()
        if name not in variable_map:
            raise ValueError(f"Unsupported archive condition variable: {name}")
        return variable_map[name]

    return ARCHIVE_CONDITION_PATTERN.sub(_replace, condition).strip()


def build_archive_delete_sql(archive_info, now=None):
    table_name = str(archive_info.src_table_name or "").strip()
    if not ARCHIVE_SAFE_IDENTIFIER_PATTERN.match(table_name):
        raise ValueError("Source table name contains unsupported characters.")

    rendered_condition = render_archive_condition(archive_info.condition, now=now)
    statement = f"DELETE FROM {table_name} WHERE {rendered_condition}"
    statements = [part.strip() for part in sqlparse.split(statement) if part.strip()]
    if len(statements) != 1:
        raise ValueError("Archive condition must resolve to a single DELETE statement.")
    normalized_statement = statements[0].rstrip(";").strip()
    if not re.match(r"^delete\s+from\b", normalized_statement, re.IGNORECASE):
        raise ValueError("Archive statement must resolve to a DELETE FROM statement.")
    return normalized_statement


def calculate_next_archive_run(archive_info, from_time=None):
    if archive_info.execution_mode != ARCHIVE_EXECUTION_SCHEDULED:
        return None
    if not archive_info.schedule_time:
        raise ValueError("Scheduled archives require schedule_time.")

    current = from_time or timezone.now()
    local_current = _local_reference_time(current)

    if archive_info.schedule_frequency == ARCHIVE_SCHEDULE_DAILY:
        for offset in range(0, 2):
            candidate = _schedule_datetime(
                local_current.date() + datetime.timedelta(days=offset),
                archive_info.schedule_time,
            )
            if candidate > current:
                return candidate
    elif archive_info.schedule_frequency == ARCHIVE_SCHEDULE_WEEKLY:
        weekdays = normalize_archive_weekdays(archive_info.schedule_weekdays)
        if not weekdays:
            raise ValueError("Weekly scheduled archives require schedule_weekdays.")
        for offset in range(0, 8):
            candidate_date = local_current.date() + datetime.timedelta(days=offset)
            weekday_token = ARCHIVE_WEEKDAY_ORDER[candidate_date.weekday()]
            if weekday_token not in weekdays:
                continue
            candidate = _schedule_datetime(candidate_date, archive_info.schedule_time)
            if candidate > current:
                return candidate
    else:
        raise ValueError("Scheduled archives require a supported schedule_frequency.")

    raise ValueError("Unable to calculate next archive run.")


def get_archive_schedule(archive_id):
    return task_info(archive_schedule_name(archive_id))


def schedule_archive(archive_info, run_at=None):
    if archive_info.execution_mode != ARCHIVE_EXECUTION_SCHEDULED:
        raise ValueError(
            "Only scheduled archives can be armed for recurring execution."
        )

    next_run = run_at or calculate_next_archive_run(archive_info)
    schedule_name = archive_schedule_name(archive_info.id)
    delete_schedule(schedule_name)
    schedule(
        "sql.archiver.archive",
        archive_info.id,
        "scheduled",
        hook="sql.archiver.archive_task_callback",
        name=schedule_name,
        schedule_type="O",
        next_run=next_run,
        repeats=1,
        timeout=-1,
    )
    ArchiveConfig.objects.filter(id=archive_info.id).update(
        next_run_at=next_run,
        state=True,
    )
    archive_info.next_run_at = next_run
    archive_info.state = True
    return next_run


def cancel_archive_schedule(archive_id):
    delete_schedule(archive_schedule_name(archive_id))
    ArchiveConfig.objects.filter(id=archive_id).update(next_run_at=None)


def _get_source_charset(archive_info):
    source_engine = get_engine(archive_info.src_instance)
    source_db = source_engine.schema_object.databases[archive_info.src_db_name]
    source_tb = source_db.tables[archive_info.src_table_name]
    source_charset = source_tb.options["charset"].value
    if source_charset is None:
        source_charset = source_db.options["charset"].value
    return source_engine, source_charset


def _build_pt_archiver_args(archive_info):
    source_engine, source_charset = _get_source_charset(archive_info)
    source = (
        rf"h={archive_info.src_instance.host},u={archive_info.src_instance.user},"
        rf"p={archive_info.src_instance.password},P={archive_info.src_instance.port},"
        rf"D={archive_info.src_db_name},t={archive_info.src_table_name},A={source_charset}"
    )
    args = {
        "no-version-check": True,
        "source": source,
        "where": render_archive_condition(archive_info.condition),
        "progress": 5000,
        "statistics": True,
        "charset": "utf8",
        "limit": 10000,
        "txn-size": 1000,
        "sleep": archive_info.sleep,
    }

    if archive_info.mode == "dest":
        dest_instance = archive_info.dest_instance
        schema_object = get_engine(dest_instance).schema_object
        dest_db = schema_object.databases[archive_info.dest_db_name]
        dest_tb = dest_db.tables[archive_info.dest_table_name]
        dest_charset = dest_tb.options["charset"].value
        if dest_charset is None:
            dest_charset = dest_db.options["charset"].value
        schema_object.connection.close()
        args["dest"] = (
            rf"h={dest_instance.host},u={dest_instance.user},p={dest_instance.password},"
            rf"P={dest_instance.port},D={archive_info.dest_db_name},"
            rf"t={archive_info.dest_table_name},A={dest_charset}"
        )
        if archive_info.no_delete:
            args["no-delete"] = True
    elif archive_info.mode == "file":
        output_directory = os.path.join(settings.BASE_DIR, "downloads/archiver")
        os.makedirs(output_directory, exist_ok=True)
        args["file"] = (
            f"{output_directory}/"
            f"{archive_info.src_instance.instance_name}-"
            f"{archive_info.src_db_name}-"
            f"{archive_info.src_table_name}.txt"
        )
        if archive_info.no_delete:
            args["no-delete"] = True
    else:
        args["purge"] = True

    source_engine.close()
    return args


def _record_archive_log(
    archive_info,
    cmd,
    select_cnt,
    insert_cnt,
    delete_cnt,
    statistics,
    success,
    error_info,
    start_time,
    end_time,
    condition=None,
    archive_method=None,
):
    if connection.connection and not connection.is_usable():
        close_old_connections()
    ArchiveConfig.objects.filter(id=archive_info.id).update(last_archive_time=end_time)
    ArchiveLog.objects.create(
        archive=archive_info,
        cmd=cmd,
        condition=condition or archive_info.condition,
        archive_method=archive_method or archive_info.archive_method,
        mode=archive_info.mode,
        no_delete=archive_info.no_delete,
        sleep=archive_info.sleep,
        select_cnt=select_cnt,
        insert_cnt=insert_cnt,
        delete_cnt=delete_cnt,
        statistics=statistics,
        success=success,
        error_info=error_info,
        start_time=start_time,
        end_time=end_time,
    )


def _execute_pt_archiver_archive(archive_info):
    pt_archiver = PtArchiver()
    args = _build_pt_archiver_args(archive_info)
    args_check_result = pt_archiver.check_args(args)
    if args_check_result["status"] == 1:
        raise RuntimeError(args_check_result["msg"])

    cmd_args = pt_archiver.generate_args2cmd(args)
    select_cnt = 0
    insert_cnt = 0
    delete_cnt = 0
    with FuncTimer() as timer:
        process = pt_archiver.execute_cmd(cmd_args)
        stdout = ""
        for line in iter(process.stdout.readline, ""):
            if re.match(r"^SELECT\s(\d+)$", line, re.I):
                select_cnt = re.findall(r"^SELECT\s(\d+)$", line)
            elif re.match(r"^INSERT\s(\d+)$", line, re.I):
                insert_cnt = re.findall(r"^INSERT\s(\d+)$", line)
            elif re.match(r"^DELETE\s(\d+)$", line, re.I):
                delete_cnt = re.findall(r"^DELETE\s(\d+)$", line)
            stdout += f"{line}\n"
        stderr = process.stderr.read()

    statistics = stdout + stderr if stderr else stdout
    select_cnt = int(select_cnt[0]) if select_cnt else 0
    insert_cnt = int(insert_cnt[0]) if insert_cnt else 0
    delete_cnt = int(delete_cnt[0]) if delete_cnt else 0
    error_info = ""
    success = True

    if stderr:
        error_info = f"Command execution error: {stderr}"
        success = False
    if archive_info.mode == "dest" and not archive_info.no_delete:
        if insert_cnt != delete_cnt:
            error_info = (
                f"Delete and insert counts do not match: {insert_cnt}!={delete_cnt}"
            )
            success = False
    elif archive_info.mode == "file" and not archive_info.no_delete:
        if select_cnt != delete_cnt:
            error_info = (
                f"Select and delete counts do not match: {select_cnt}!={delete_cnt}"
            )
            success = False
    elif archive_info.mode == "purge" and select_cnt != delete_cnt:
        error_info = (
            f"Select and delete counts do not match: {select_cnt}!={delete_cnt}"
        )
        success = False

    source_password = archive_info.src_instance.password or ""
    shell_cmd = " ".join(cmd_args).replace(source_password, "***")
    if archive_info.dest_instance and archive_info.dest_instance.password:
        shell_cmd = shell_cmd.replace(archive_info.dest_instance.password, "***")
    _record_archive_log(
        archive_info=archive_info,
        cmd=shell_cmd,
        select_cnt=select_cnt,
        insert_cnt=insert_cnt,
        delete_cnt=delete_cnt,
        statistics=statistics,
        success=success,
        error_info=error_info,
        start_time=timer.start,
        end_time=timer.end,
        archive_method=ARCHIVE_METHOD_PT_ARCHIVER,
    )
    if not success:
        raise RuntimeError(f"{error_info}\n{statistics}")
    return {
        "statement": shell_cmd,
        "select_cnt": select_cnt,
        "insert_cnt": insert_cnt,
        "delete_cnt": delete_cnt,
        "success": True,
        "statistics": statistics,
    }


def _execute_dml_archive(archive_info):
    delete_sql = build_archive_delete_sql(archive_info)
    query_engine = get_engine(instance=archive_info.src_instance)
    query_check = getattr(query_engine, "query_check", None)
    if callable(query_check):
        check_result = query_check(db_name=archive_info.src_db_name, sql=delete_sql)
        if check_result.get("bad_query"):
            raise RuntimeError(
                check_result.get("msg", "Blocked archive delete statement.")
            )

    with FuncTimer() as timer:
        execute_result = query_engine.execute(
            db_name=archive_info.src_db_name,
            sql=delete_sql,
        )

    success = not bool(getattr(execute_result, "error", None))
    delete_cnt = int(getattr(execute_result, "affected_rows", 0) or 0)
    statistics = (
        execute_result.json()
        if hasattr(execute_result, "json")
        else json.dumps(getattr(execute_result, "__dict__", {}), default=str)
    )
    error_info = getattr(execute_result, "error", "") or ""
    _record_archive_log(
        archive_info=archive_info,
        cmd=delete_sql,
        select_cnt=0,
        insert_cnt=0,
        delete_cnt=delete_cnt,
        statistics=statistics,
        success=success,
        error_info=error_info,
        start_time=timer.start,
        end_time=timer.end,
        condition=render_archive_condition(archive_info.condition),
        archive_method=ARCHIVE_METHOD_DML,
    )
    if not success:
        raise RuntimeError(error_info or "Archive execution failed.")
    return {
        "statement": delete_sql,
        "select_cnt": 0,
        "insert_cnt": 0,
        "delete_cnt": delete_cnt,
        "success": True,
        "statistics": statistics,
    }


def queue_archive_execution(archive_id, trigger="manual"):
    async_task(
        "sql.archiver.archive",
        archive_id,
        trigger,
        hook="sql.archiver.archive_task_callback",
        timeout=-1,
        task_name=f"archive-{archive_id}",
    )


def archive_task_callback(task):
    archive_id = task.args[0]
    trigger = task.args[1] if len(task.args) > 1 else "manual"
    archive_info = ArchiveConfig.objects.get(id=archive_id)
    audit = Audit.detail_by_workflow_id(archive_id, WorkflowType.ARCHIVE)

    if audit:
        status_desc = "finished successfully" if task.success else "failed"
        operation_info = f"Archive execution result: {status_desc}"
        if not task.success:
            operation_info = f"{operation_info}. {task.error or task.result}"
        Audit.add_log(
            audit_id=audit.audit_id,
            operation_type=WorkflowAction.EXECUTE_END,
            operation_type_desc="Archive Execution Finished",
            operation_info=operation_info,
            operator="",
            operator_display="System" if trigger == "scheduled" else "Archive Manager",
        )

    if archive_info.execution_mode == ARCHIVE_EXECUTION_ONE_TIME:
        if task.success:
            ArchiveConfig.objects.filter(id=archive_id).update(
                state=False,
                next_run_at=None,
            )
        return

    if not archive_info.state or archive_info.status != WorkflowStatus.PASSED:
        ArchiveConfig.objects.filter(id=archive_id).update(next_run_at=None)
        return

    next_run = calculate_next_archive_run(archive_info, from_time=timezone.now())
    schedule_archive(archive_info, run_at=next_run)


def _archive_enabled_queryset(archive_ids=None):
    archive_ids = archive_ids or []
    queryset = ArchiveConfig.objects.filter(
        state=True,
        status=WorkflowStatus.PASSED,
    )
    if archive_ids:
        queryset = queryset.filter(id__in=list(archive_ids))
    return queryset


@permission_required("sql.menu_archive", raise_exception=True)
def archive_list(request):
    """
    Get archive request list.
    :param request:
    :return:
    """
    user = request.user
    filter_instance_id = request.GET.get("filter_instance_id")
    state = request.GET.get("state")
    limit = int(request.GET.get("limit", 0))
    offset = int(request.GET.get("offset", 0))
    limit = offset + limit
    search = request.GET.get("search", "")

    # Build filter options.
    filter_dict = dict()
    if filter_instance_id:
        filter_dict["src_instance"] = filter_instance_id
    if state == "true":
        filter_dict["state"] = True
    elif state == "false":
        filter_dict["state"] = False

    # Admin users can view all records.
    if user.is_superuser:
        pass
    # Users with review permission can view all workflows in their groups.
    elif user.has_perm("sql.archive_review"):
        # Get the user's resource groups first.
        group_list = user_groups(user)
        group_ids = [group.group_id for group in group_list]
        filter_dict["resource_group__in"] = group_ids
    # Others can only view workflows they submitted.
    else:
        filter_dict["user_name"] = user.username

    # Apply combined filters.
    archive_config = ArchiveConfig.objects.filter(**filter_dict)

    # Apply search filter (title/user fuzzy match).
    if search:
        archive_config = archive_config.filter(
            Q(title__icontains=search) | Q(user_display__icontains=search)
        )

    count = archive_config.count()
    lists = archive_config.order_by("-id")[offset:limit].values(
        "id",
        "title",
        "src_instance__instance_name",
        "src_db_name",
        "src_table_name",
        "dest_instance__instance_name",
        "dest_db_name",
        "dest_table_name",
        "sleep",
        "mode",
        "no_delete",
        "status",
        "state",
        "user_display",
        "create_time",
        "resource_group__group_name",
    )

    # Serialize QuerySet.
    rows = [row for row in lists]

    result = {"total": count, "rows": rows}
    # Return query result.
    return HttpResponse(
        json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
        content_type="application/json",
    )


@permission_required("sql.archive_apply", raise_exception=True)
def archive_apply(request):
    """Submit archive request for instance data."""
    user = request.user
    title = request.POST.get("title")
    group_name = request.POST.get("group_name")
    src_instance_name = request.POST.get("src_instance_name")
    src_db_name = request.POST.get("src_db_name")
    src_table_name = request.POST.get("src_table_name")
    mode = request.POST.get("mode")
    dest_instance_name = request.POST.get("dest_instance_name")
    dest_db_name = request.POST.get("dest_db_name")
    dest_table_name = request.POST.get("dest_table_name")
    condition = request.POST.get("condition")
    no_delete = True if request.POST.get("no_delete") == "true" else False
    sleep = request.POST.get("sleep") or 0
    result = {"status": 0, "msg": "ok", "data": {}}

    # Validate parameters.
    if (
        not all(
            [
                title,
                group_name,
                src_instance_name,
                src_db_name,
                src_table_name,
                mode,
                condition,
            ]
        )
        or no_delete is None
    ):
        return JsonResponse(
            {"status": 1, "msg": "Please complete all required fields!", "data": {}}
        )
    if mode == "dest" and not all([dest_instance_name, dest_db_name, dest_table_name]):
        return JsonResponse(
            {
                "status": 1,
                "msg": "Destination instance info is required for destination mode!",
                "data": {},
            }
        )

    # Get source instance info.
    try:
        s_ins = user_instances(request.user, db_type=["mysql"]).get(
            instance_name=src_instance_name
        )
    except Instance.DoesNotExist:
        return JsonResponse(
            {
                "status": 1,
                "msg": "Your group is not associated with this instance!",
                "data": {},
            }
        )

    # Get destination instance info.
    if mode == "dest":
        try:
            d_ins = user_instances(request.user, db_type=["mysql"]).get(
                instance_name=dest_instance_name
            )
        except Instance.DoesNotExist:
            return JsonResponse(
                {
                    "status": 1,
                    "msg": "Your group is not associated with this instance!",
                    "data": {},
                }
            )
    else:
        d_ins = None

    # Get resource group and audit settings.
    res_group = ResourceGroup.objects.get(group_name=group_name)
    # Keep data consistent using a transaction.
    with transaction.atomic():
        # Save request into database.
        archive_info = ArchiveConfig(
            title=title,
            resource_group=res_group,
            audit_auth_groups="",
            src_instance=s_ins,
            src_db_name=src_db_name,
            src_table_name=src_table_name,
            dest_instance=d_ins,
            dest_db_name=dest_db_name,
            dest_table_name=dest_table_name,
            condition=condition,
            mode=mode,
            no_delete=no_delete,
            sleep=sleep,
            status=WorkflowStatus.WAITING,
            state=False,
            user_name=user.username,
            user_display=user.display,
        )
        audit_handler = get_auditor(
            workflow=archive_info,
            resource_group=res_group.group_name,
            resource_group_id=res_group.group_id,
        )

        try:
            audit_handler.create_audit()
        except AuditException as e:
            logger.error(f"Failed to create approval flow: {str(e)}")
            return JsonResponse(
                {
                    "status": 1,
                    "msg": "Failed to create approval flow. Contact admin.",
                    "data": {},
                }
            )
        audit_handler.workflow.status = audit_handler.audit.current_status
        if audit_handler.audit.current_status == WorkflowStatus.PASSED:
            audit_handler.workflow.state = True
        audit_handler.workflow.save()
        async_task(
            notify_for_audit,
            workflow_audit=audit_handler.audit,
            timeout=60,
            task_name=f"archive-apply-{audit_handler.workflow.id}",
        )
    return JsonResponse(
        {
            "status": 0,
            "msg": "",
            "data": {
                "workflow_status": audit_handler.audit.current_status,
                "audit_id": audit_handler.audit.audit_id,
                "archive_id": audit_handler.workflow.id,
            },
        }
    )


@permission_required("sql.archive_review", raise_exception=True)
def archive_audit(request):
    """
    Review archive request.
    :param request:
    :return:
    """
    # Get user input.
    archive_id = int(request.POST["archive_id"])
    try:
        audit_status = WorkflowAction(int(request.POST["audit_status"]))
    except ValueError as e:
        return render(
            request,
            "error.html",
            {
                "errMsg": (
                    f"Data error, operation not allowed. "
                    f"Please check audit_status. Error: {str(e)}"
                )
            },
        )
    audit_remark = request.POST.get("audit_remark")

    if audit_remark is None:
        audit_remark = ""
    try:
        archive_workflow = ArchiveConfig.objects.get(id=archive_id)
    except ArchiveConfig.DoesNotExist:
        return render(request, "error.html", {"errMsg": "Workflow does not exist"})

    resource_group = archive_workflow.resource_group
    auditor = get_auditor(workflow=archive_workflow, resource_group=resource_group)

    # Keep data consistent using a transaction.
    with transaction.atomic():
        try:
            workflow_audit_detail = auditor.operate(
                audit_status, request.user, audit_remark
            )
        except AuditException as e:
            return render(request, "error.html", {"errMsg": f"Review failed: {str(e)}"})
        auditor.workflow.status = auditor.audit.current_status
        if auditor.audit.current_status == WorkflowStatus.PASSED:
            auditor.workflow.state = True
            if auditor.workflow.execution_mode == ARCHIVE_EXECUTION_SCHEDULED:
                auditor.workflow.next_run_at = calculate_next_archive_run(
                    auditor.workflow
                )
        else:
            auditor.workflow.next_run_at = None
            cancel_archive_schedule(auditor.workflow.id)
        auditor.workflow.save()
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
        task_name=f"archive-audit-{archive_id}",
    )

    return HttpResponseRedirect(reverse("sql:archive_detail", args=(archive_id,)))


def add_archive_task(archive_ids=None):
    """
    Add async archive tasks and only process valid archive records.
    :param archive_ids: archive task id list
    :return:
    """
    for archive_info in _archive_enabled_queryset(archive_ids):
        queue_archive_execution(archive_info.id, trigger="manual")


def archive(archive_id, trigger="manual"):
    """
    Execute database archive.
    :return:
    """
    with transaction.atomic():
        archive_info = ArchiveConfig.objects.select_for_update().get(id=archive_id)
        if archive_info.status != WorkflowStatus.PASSED:
            raise RuntimeError("Archive workflow is not approved.")
        if (
            archive_info.execution_mode == ARCHIVE_EXECUTION_SCHEDULED
            and not archive_info.state
        ):
            raise RuntimeError("Scheduled archive is disabled.")
        if (
            archive_info.execution_mode == ARCHIVE_EXECUTION_ONE_TIME
            and not archive_info.state
            and archive_info.last_archive_time is not None
        ):
            raise RuntimeError("One-time archive has already completed.")

    audit = Audit.detail_by_workflow_id(archive_id, WorkflowType.ARCHIVE)
    if audit:
        operation_info = (
            "System scheduled archive execution"
            if trigger == "scheduled"
            else "Archive execution started"
        )
        Audit.add_log(
            audit_id=audit.audit_id,
            operation_type=WorkflowAction.EXECUTE_START,
            operation_type_desc="Archive Execution Started",
            operation_info=operation_info,
            operator="",
            operator_display="System" if trigger == "scheduled" else "Archive Manager",
        )

    if (
        archive_info.mode in ("file", "dest")
        or archive_info.archive_method == ARCHIVE_METHOD_PT_ARCHIVER
    ):
        return _execute_pt_archiver_archive(archive_info)
    return _execute_dml_archive(archive_info)


@permission_required("sql.menu_archive", raise_exception=True)
def archive_log(request):
    """Get archive log list."""
    limit = int(request.GET.get("limit", 0))
    offset = int(request.GET.get("offset", 0))
    limit = offset + limit
    archive_id = request.GET.get("archive_id")

    archive_logs = ArchiveLog.objects.filter(archive=archive_id).annotate(
        info=Concat("cmd", V("\n"), "statistics", output_field=TextField())
    )
    count = archive_logs.count()
    lists = archive_logs.order_by("-id")[offset:limit].values(
        "cmd",
        "info",
        "condition",
        "mode",
        "no_delete",
        "select_cnt",
        "insert_cnt",
        "delete_cnt",
        "success",
        "error_info",
        "start_time",
        "end_time",
    )
    # Serialize QuerySet.
    rows = [row for row in lists]
    result = {"total": count, "rows": rows}
    # Return query result.
    return HttpResponse(
        json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
        content_type="application/json",
    )


@permission_required("sql.archive_mgt", raise_exception=True)
def archive_switch(request):
    """Enable or disable archive task."""
    archive_id = request.POST.get("archive_id")
    state = True if request.POST.get("state") == "true" else False
    # Update enabled state.
    try:
        archive_info = ArchiveConfig.objects.get(id=archive_id)
        archive_info.state = state
        if not state:
            archive_info.next_run_at = None
            cancel_archive_schedule(archive_info.id)
        elif archive_info.execution_mode == ARCHIVE_EXECUTION_SCHEDULED:
            archive_info.next_run_at = calculate_next_archive_run(archive_info)
            archive_info.save(update_fields=["state", "next_run_at"])
            schedule_archive(archive_info, run_at=archive_info.next_run_at)
            return JsonResponse({"status": 0, "msg": "ok", "data": {}})
        archive_info.save(
            update_fields=(
                ["state", "next_run_at"]
                if archive_info.execution_mode == ARCHIVE_EXECUTION_SCHEDULED
                else ["state"]
            )
        )
        return JsonResponse({"status": 0, "msg": "ok", "data": {}})
    except Exception as msg:
        return JsonResponse({"status": 1, "msg": f"{msg}", "data": {}})


@permission_required("sql.archive_mgt", raise_exception=True)
def archive_once(request):
    """Trigger archive task once immediately."""
    archive_id = request.GET.get("archive_id")
    queue_archive_execution(archive_id, trigger="manual")
    return JsonResponse({"status": 0, "msg": "ok", "data": {}})
