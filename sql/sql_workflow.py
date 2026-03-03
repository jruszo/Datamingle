# -*- coding: UTF-8 -*-
import datetime
import logging
import traceback

import simplejson as json
from django.contrib.auth.decorators import permission_required
from django.contrib.auth.models import Group
from django.core.exceptions import PermissionDenied
from django.db import transaction
from django.db.models import Q
from django.http import HttpResponse, HttpResponseRedirect, JsonResponse
from django.shortcuts import render, get_object_or_404
from django.urls import reverse
from django_q.tasks import async_task

from common.config import SysConfig
from common.utils.const import WorkflowStatus, WorkflowType, WorkflowAction
from common.utils.extend_json_encoder import ExtendJSONEncoder
from sql.engines import get_engine
from sql.engines.models import ReviewResult, ReviewSet
from sql.notify import notify_for_audit, EventType, notify_for_execute
from sql.utils.resource_group import user_groups
from sql.utils.sql_review import (
    can_timingtask,
    can_cancel,
    can_execute,
    on_correct_time_period,
    can_view,
    can_rollback,
)
from sql.utils.tasks import add_sql_schedule, del_schedule
from sql.utils.workflow_audit import Audit, get_auditor, AuditException
from .models import SqlWorkflow, WorkflowAudit

logger = logging.getLogger("default")


@permission_required("sql.menu_sqlworkflow", raise_exception=True)
def sql_workflow_list(request):
    return _sql_workflow_list(request)


@permission_required("sql.audit_user", raise_exception=True)
def sql_workflow_list_audit(request):
    return _sql_workflow_list(request)


def _sql_workflow_list(request):
    """
    Get review list.
    :param request:
    :return:
    """
    nav_status = request.POST.get("navStatus")
    instance_id = request.POST.get("instance_id")
    resource_group_id = request.POST.get("group_id")
    start_date = request.POST.get("start_date")
    end_date = request.POST.get("end_date")
    limit = int(request.POST.get("limit", 0))
    offset = int(request.POST.get("offset", 0))
    limit = offset + limit
    limit = limit if limit else None
    search = request.POST.get("search")
    user = request.user
    syntax_type = request.POST.getlist("syntax_type[]")

    # Build filter options.
    filter_dict = dict()
    # Workflow type
    if syntax_type:
        filter_dict["syntax_type__in"] = syntax_type
    # Workflow status
    if nav_status:
        filter_dict["status"] = nav_status
    # Instance
    if instance_id:
        filter_dict["instance_id"] = instance_id
    # Resource group
    if resource_group_id:
        filter_dict["group_id"] = resource_group_id
    # Time range
    if start_date and end_date:
        end_date = datetime.datetime.strptime(
            end_date, "%Y-%m-%d"
        ) + datetime.timedelta(days=1)
        filter_dict["create_time__range"] = (start_date, end_date)
    # Admin and auditors can view all workflows.
    if user.is_superuser or user.has_perm("sql.audit_user"):
        pass
    # Non-admin users with review or group-execution permission
    # can view workflows in their groups.
    elif user.has_perm("sql.sql_review") or user.has_perm(
        "sql.sql_execute_for_resource_group"
    ):
        # Get user's resource groups first.
        group_list = user_groups(user)
        group_ids = [group.group_id for group in group_list]
        filter_dict["group_id__in"] = group_ids
    # Others can only view workflows they submitted.
    else:
        filter_dict["engineer"] = user.username

    # Apply combined filters.
    workflow = SqlWorkflow.objects.filter(**filter_dict)

    # Apply search filter (submitter/workflow name fuzzy match).
    if search:
        workflow = workflow.filter(
            Q(engineer_display__icontains=search) | Q(workflow_name__icontains=search)
        )

    count = workflow.count()
    workflow_list = workflow.order_by("-create_time")[offset:limit].values(
        "id",
        "workflow_name",
        "engineer_display",
        "status",
        "is_backup",
        "create_time",
        "instance__instance_name",
        "db_name",
        "group_name",
        "syntax_type",
        "export_format",
    )

    # Serialize QuerySet.
    rows = [row for row in workflow_list]
    result = {"total": count, "rows": rows}
    # Return query result.
    return HttpResponse(
        json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
        content_type="application/json",
    )


def detail_content(request):
    """Get workflow content."""
    workflow_id = request.GET.get("workflow_id")
    workflow_detail = get_object_or_404(SqlWorkflow, pk=workflow_id)
    if not can_view(request.user, workflow_id):
        raise PermissionDenied
    if workflow_detail.status in ["workflow_finish", "workflow_exception"]:
        rows = workflow_detail.sqlworkflowcontent.execute_result
    else:
        rows = workflow_detail.sqlworkflowcontent.review_content

    review_result = ReviewSet()
    if rows:
        try:
            # Check whether rows can be parsed correctly.
            loaded_rows = json.loads(rows)
            # Backward-compatible conversion from old '[[]]' to new '[{}]' format.
            if isinstance(loaded_rows[-1], list):
                for r in loaded_rows:
                    review_result.rows += [ReviewResult(inception_result=r)]
                rows = review_result.json()
        except IndexError:
            review_result.rows += [
                ReviewResult(
                    id=1,
                    sql=workflow_detail.sqlworkflowcontent.sql_content,
                    errormessage=(
                        "Json decode failed. Execution result JSON parsing failed. "
                        "Please contact admin."
                    ),
                )
            ]
            rows = review_result.json()
        except json.decoder.JSONDecodeError:
            review_result.rows += [
                ReviewResult(
                    id=1,
                    sql=workflow_detail.sqlworkflowcontent.sql_content,
                    # Keep explicit English error text for reliable unit testing.
                    errormessage=(
                        "Json decode failed. Execution result JSON parsing failed. "
                        "Please contact admin."
                    ),
                )
            ]
            rows = review_result.json()
    else:
        rows = workflow_detail.sqlworkflowcontent.review_content

    result = {"rows": json.loads(rows)}
    return HttpResponse(json.dumps(result), content_type="application/json")


def backup_sql(request):
    """Get rollback SQL."""
    workflow_id = request.GET.get("workflow_id")
    if not can_rollback(request.user, workflow_id):
        raise PermissionDenied
    workflow = get_object_or_404(SqlWorkflow, pk=workflow_id)

    try:
        query_engine = get_engine(instance=workflow.instance)
        list_backup_sql = query_engine.get_rollback(workflow=workflow)
    except Exception as msg:
        logger.error(traceback.format_exc())
        return JsonResponse({"status": 1, "msg": f"{msg}", "rows": []})

    result = {"status": 0, "msg": "", "rows": list_backup_sql}
    return HttpResponse(json.dumps(result), content_type="application/json")


@permission_required("sql.sql_review", raise_exception=True)
def alter_run_date(request):
    """
    Allow reviewer to modify executable time window.
    :param request:
    :return:
    """
    workflow_id = int(request.POST.get("workflow_id", 0))
    run_date_start = request.POST.get("run_date_start")
    run_date_end = request.POST.get("run_date_end")
    if workflow_id == 0:
        context = {"errMsg": "workflow_id parameter is empty."}
        return render(request, "error.html", context)

    user = request.user
    if Audit.can_review(user, workflow_id, 2) is False:
        context = {"errMsg": "You are not allowed to operate on this workflow!"}
        return render(request, "error.html", context)

    try:
        # Save into database.
        SqlWorkflow(
            id=workflow_id,
            run_date_start=run_date_start or None,
            run_date_end=run_date_end or None,
        ).save(update_fields=["run_date_start", "run_date_end"])
    except Exception as msg:
        context = {"errMsg": msg}
        return render(request, "error.html", context)

    return HttpResponseRedirect(reverse("sql:detail", args=(workflow_id,)))


@permission_required("sql.sql_review", raise_exception=True)
def passed(request):
    """
    Approve workflow without execution.
    :param request:
    :return:
    """
    workflow_id = int(request.POST.get("workflow_id", 0))
    audit_remark = request.POST.get("audit_remark", "")
    if workflow_id == 0:
        context = {"errMsg": "workflow_id parameter is empty."}
        return render(request, "error.html", context)
    try:
        sql_workflow = SqlWorkflow.objects.get(id=workflow_id)
    except SqlWorkflow.DoesNotExist:
        return render(request, "error.html", {"errMsg": "Workflow does not exist"})

    sys_config = SysConfig()
    auditor = get_auditor(workflow=sql_workflow, sys_config=sys_config)
    # Keep data consistent using a transaction.
    with transaction.atomic():
        try:
            workflow_audit_detail = auditor.operate(
                WorkflowAction.PASS, request.user, audit_remark
            )
        except AuditException as e:
            return render(
                request,
                "error.html",
                {"errMsg": f"Audit failed, error details: {str(e)}"},
            )
        if auditor.audit.current_status == WorkflowStatus.PASSED:
            # If approval flow finished, mark workflow as review-passed.
            auditor.workflow.status = "workflow_review_pass"
            auditor.workflow.save()

    # Send notifications only if Pass phase is enabled.
    is_notified = (
        "Pass" in sys_config.get("notify_phase_control").split(",")
        if sys_config.get("notify_phase_control")
        else True
    )
    if is_notified:
        async_task(
            notify_for_audit,
            workflow_audit=auditor.audit,
            workflow_audit_detail=workflow_audit_detail,
            timeout=60,
            task_name=f"sqlreview-pass-{workflow_id}",
        )

    return HttpResponseRedirect(reverse("sql:detail", args=(workflow_id,)))


def execute(request):
    """
    Execute SQL.
    :param request:
    :return:
    """
    # Validate execute permissions.
    if not (
        request.user.has_perm("sql.sql_execute")
        or request.user.has_perm("sql.sql_execute_for_resource_group")
    ):
        raise PermissionDenied
    workflow_id = int(request.POST.get("workflow_id", 0))
    if workflow_id == 0:
        context = {"errMsg": "workflow_id parameter is empty."}
        return render(request, "error.html", context)

    if can_execute(request.user, workflow_id) is False:
        context = {"errMsg": "You are not allowed to operate on this workflow!"}
        return render(request, "error.html", context)

    if on_correct_time_period(workflow_id) is False:
        context = {
            "errMsg": "Not within executable time range. Resubmit workflow to change execution time!"
        }
        return render(request, "error.html", context)
    # Get audit information.
    audit_id = Audit.detail_by_workflow_id(
        workflow_id=workflow_id, workflow_type=WorkflowType.SQL_REVIEW
    ).audit_id
    # Apply updates according to execution mode.
    mode = request.POST.get("mode")
    # System execution mode.
    if mode == "auto":
        # Set workflow status to queued.
        SqlWorkflow(id=workflow_id, status="workflow_queuing").save(
            update_fields=["status"]
        )
        # Delete scheduled execution task.
        schedule_name = f"sqlreview-timing-{workflow_id}"
        del_schedule(schedule_name)
        # Add to execution queue.
        async_task(
            "sql.utils.execute_sql.execute",
            workflow_id,
            request.user,
            hook="sql.utils.execute_sql.execute_callback",
            timeout=-1,
            task_name=f"sqlreview-execute-{workflow_id}",
        )
        # Add workflow log.
        Audit.add_log(
            audit_id=audit_id,
            operation_type=5,
            operation_type_desc="Execute Workflow",
            operation_info="Workflow queued for execution",
            operator=request.user.username,
            operator_display=request.user.display,
        )

    # Offline manual execution mode.
    elif mode == "manual":
        # Set workflow status to finished.
        SqlWorkflow(
            id=workflow_id,
            status="workflow_finish",
            finish_time=datetime.datetime.now(),
        ).save(update_fields=["status", "finish_time"])
        # Add workflow log.
        Audit.add_log(
            audit_id=audit_id,
            operation_type=6,
            operation_type_desc="Manual Workflow",
            operation_info="Manual execution confirmed complete",
            operator=request.user.username,
            operator_display=request.user.display,
        )
        # Send notifications only if Execute phase is enabled.
        sys_config = SysConfig()
        is_notified = (
            "Execute" in sys_config.get("notify_phase_control").split(",")
            if sys_config.get("notify_phase_control")
            else True
        )
        if is_notified:
            notify_for_execute(workflow=SqlWorkflow.objects.get(id=workflow_id))
    return HttpResponseRedirect(reverse("sql:detail", args=(workflow_id,)))


def timing_task(request):
    """
    Schedule SQL execution.
    :param request:
    :return:
    """
    # Validate execute permissions.
    if not (
        request.user.has_perm("sql.sql_execute")
        or request.user.has_perm("sql.sql_execute_for_resource_group")
    ):
        raise PermissionDenied
    workflow_id = request.POST.get("workflow_id")
    run_date = request.POST.get("run_date")
    if run_date is None or workflow_id is None:
        context = {"errMsg": "Time cannot be empty"}
        return render(request, "error.html", context)
    elif run_date < datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"):
        context = {"errMsg": "Time cannot be earlier than current time"}
        return render(request, "error.html", context)
    workflow_detail = SqlWorkflow.objects.get(id=workflow_id)

    if can_timingtask(request.user, workflow_id) is False:
        context = {"errMsg": "You are not allowed to operate on this workflow!"}
        return render(request, "error.html", context)

    run_date = datetime.datetime.strptime(run_date, "%Y-%m-%d %H:%M")
    schedule_name = f"sqlreview-timing-{workflow_id}"

    if on_correct_time_period(workflow_id, run_date) is False:
        context = {
            "errMsg": (
                "Not within executable time range. "
                "Resubmit workflow to change execution time!"
            )
        }
        return render(request, "error.html", context)

    # Keep data consistent using a transaction.
    try:
        with transaction.atomic():
            # Set workflow status to scheduled execution.
            workflow_detail.status = "workflow_timingtask"
            workflow_detail.save()
            # Add schedule task.
            add_sql_schedule(schedule_name, run_date, workflow_id)
            # Add workflow log.
            audit_id = Audit.detail_by_workflow_id(
                workflow_id=workflow_id,
                workflow_type=WorkflowType.SQL_REVIEW,
            ).audit_id
            Audit.add_log(
                audit_id=audit_id,
                operation_type=4,
                operation_type_desc="Scheduled Execution",
                operation_info="Scheduled execution time: {}".format(run_date),
                operator=request.user.username,
                operator_display=request.user.display,
            )
    except Exception as msg:
        logger.error(
            f"Scheduled workflow execution failed, error details: {traceback.format_exc()}"
        )
        context = {"errMsg": msg}
        return render(request, "error.html", context)
    return HttpResponseRedirect(reverse("sql:detail", args=(workflow_id,)))


def cancel(request):
    """
    Cancel workflow.
    :param request:
    :return:
    """
    workflow_id = int(request.POST.get("workflow_id", 0))
    if workflow_id == 0:
        context = {"errMsg": "workflow_id parameter is empty."}
        return render(request, "error.html", context)
    sql_workflow = SqlWorkflow.objects.get(id=workflow_id)
    audit_remark = request.POST.get("cancel_remark")
    if audit_remark is None:
        context = {"errMsg": "Cancellation reason cannot be empty"}
        return render(request, "error.html", context)

    user = request.user
    if can_cancel(request.user, workflow_id) is False:
        context = {"errMsg": "You are not allowed to operate on this workflow!"}
        return render(request, "error.html", context)

    # Keep data consistent using a transaction.
    if user.username == sql_workflow.engineer:
        action = WorkflowAction.ABORT
    elif user.has_perm("sql.sql_review"):
        action = WorkflowAction.REJECT
    else:
        raise PermissionDenied
    with transaction.atomic():
        auditor = get_auditor(workflow=sql_workflow)
        try:
            workflow_audit_detail = auditor.operate(action, request.user, audit_remark)
        except AuditException as e:
            logger.error(
                f"Workflow cancellation failed, error details: {traceback.format_exc()}"
            )
            return render(request, "error.html", {"errMsg": f"{str(e)}"})
        # Set workflow status to manually aborted.
        sql_workflow.status = "workflow_abort"
        sql_workflow.save()
    # Delete scheduled task.
    if sql_workflow.status == "workflow_timingtask":
        del_schedule(f"sqlreview-timing-{workflow_id}")
    # Send cancel/reject notification only if Cancel phase is enabled.
    sys_config = SysConfig()
    is_notified = (
        "Cancel" in sys_config.get("notify_phase_control").split(",")
        if sys_config.get("notify_phase_control")
        else True
    )
    if is_notified:
        async_task(
            notify_for_audit,
            workflow_audit=auditor.audit,
            workflow_audit_detail=workflow_audit_detail,
            timeout=60,
            task_name=f"sqlreview-cancel-{workflow_id}",
        )
    return HttpResponseRedirect(reverse("sql:detail", args=(workflow_id,)))


def get_workflow_status(request):
    """
    Get current status for a workflow.
    """
    workflow_id = request.POST["workflow_id"]
    if workflow_id == "" or workflow_id is None:
        context = {"status": -1, "msg": "workflow_id parameter is empty.", "data": ""}
        return HttpResponse(json.dumps(context), content_type="application/json")

    workflow_id = int(workflow_id)
    workflow_detail = get_object_or_404(SqlWorkflow, pk=workflow_id)
    result = {"status": workflow_detail.status, "msg": "", "data": ""}
    return JsonResponse(result)


def osc_control(request):
    """Control OSC execution for MySQL."""
    workflow_id = request.POST.get("workflow_id")
    sqlsha1 = request.POST.get("sqlsha1")
    command = request.POST.get("command")
    workflow = SqlWorkflow.objects.get(id=workflow_id)
    execute_engine = get_engine(workflow.instance)
    try:
        execute_result = execute_engine.osc_control(command=command, sqlsha1=sqlsha1)
        rows = execute_result.to_dict()
        error = execute_result.error
    except Exception as e:
        rows = []
        error = str(e)
    result = {"total": len(rows), "rows": rows, "msg": error}
    return HttpResponse(
        json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
        content_type="application/json",
    )
