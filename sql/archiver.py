# -*- coding: UTF-8 -*-
"""
@author: hhyo
@license: Apache Licence
@file: archive.py
@time: 2020/01/10
"""

import logging
import os
import re
import traceback
import time

import simplejson as json
from django.conf import settings
from django.contrib.auth.decorators import permission_required
from django.db import transaction, connection, close_old_connections
from django.db.models import Q, Value as V, TextField
from django.db.models.functions import Concat
from django.http import HttpResponse, JsonResponse, HttpResponseRedirect
from django.shortcuts import render
from django.urls import reverse
from django_q.tasks import async_task

from common.utils.const import WorkflowStatus, WorkflowType, WorkflowAction
from common.utils.extend_json_encoder import ExtendJSONEncoder
from common.utils.timer import FuncTimer
from sql.engines import get_engine
from sql.notify import notify_for_audit
from sql.plugins.pt_archiver import PtArchiver
from sql.utils.resource_group import user_instances, user_groups
from sql.models import ArchiveConfig, ArchiveLog, Instance, ResourceGroup
from sql.utils.workflow_audit import get_auditor, AuditException, Audit

logger = logging.getLogger("default")
__author__ = "hhyo"


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
        return JsonResponse({"status": 1, "msg": "Please complete all required fields!", "data": {}})
    if mode == "dest" and not all([dest_instance_name, dest_db_name, dest_table_name]):
        return JsonResponse(
            {"status": 1, "msg": "Destination instance info is required for destination mode!", "data": {}}
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
                {"status": 1, "msg": "Failed to create approval flow. Contact admin.", "data": {}}
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
        auditor.workflow.save()
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
    archive_ids = archive_ids or []
    if not isinstance(archive_ids, list):
        archive_ids = list(archive_ids)
    # If no archive_id is passed, schedule all enabled archive tasks.
    if archive_ids:
        archive_cnf_list = ArchiveConfig.objects.filter(
            id__in=archive_ids,
            state=True,
            status=WorkflowStatus.PASSED,
        )
    else:
        archive_cnf_list = ArchiveConfig.objects.filter(
            state=True, status=WorkflowStatus.PASSED
        )

    # Add async tasks.
    for archive_info in archive_cnf_list:
        archive_id = archive_info.id
        async_task(
            "sql.archiver.archive",
            archive_id,
            group=f'archive-{time.strftime("%Y-%m-%d %H:%M:%S ")}',
            timeout=-1,
            task_name=f"archive-{archive_id}",
        )


def archive(archive_id):
    """
    Execute database archive.
    :return:
    """
    archive_info = ArchiveConfig.objects.get(id=archive_id)
    s_ins = archive_info.src_instance
    src_db_name = archive_info.src_db_name
    src_table_name = archive_info.src_table_name
    condition = archive_info.condition
    no_delete = archive_info.no_delete
    sleep = archive_info.sleep
    mode = archive_info.mode

    # Get source table charset info.
    s_engine = get_engine(s_ins)
    s_db = s_engine.schema_object.databases[src_db_name]
    s_tb = s_db.tables[src_table_name]
    s_charset = s_tb.options["charset"].value
    if s_charset is None:
        s_charset = s_db.options["charset"].value

    pt_archiver = PtArchiver()
    # Prepare parameters.
    source = (
        rf"h={s_ins.host},u={s_ins.user},p={s_ins.password},"
        rf"P={s_ins.port},D={src_db_name},t={src_table_name},A={s_charset}"
    )
    args = {
        "no-version-check": True,
        "source": source,
        "where": condition,
        "progress": 5000,
        "statistics": True,
        "charset": "utf8",
        "limit": 10000,
        "txn-size": 1000,
        "sleep": sleep,
    }

    # Archive into destination instance.
    if mode == "dest":
        d_ins = archive_info.dest_instance
        dest_db_name = archive_info.dest_db_name
        dest_table_name = archive_info.dest_table_name
        # Destination table charset info.
        schema_object = get_engine(d_ins).schema_object
        d_db = schema_object.databases[dest_db_name]
        d_tb = d_db.tables[dest_table_name]
        d_charset = d_tb.options["charset"].value
        if d_charset is None:
            d_charset = d_db.options["charset"].value
        schema_object.connection.close()
        # dest
        dest = (
            rf"h={d_ins.host},u={d_ins.user},p={d_ins.password},P={d_ins.port},"
            rf"D={dest_db_name},t={dest_table_name},A={d_charset}"
        )
        args["dest"] = dest
        if no_delete:
            args["no-delete"] = True
    elif mode == "file":
        output_directory = os.path.join(settings.BASE_DIR, "downloads/archiver")
        os.makedirs(output_directory, exist_ok=True)
        args["file"] = (
            f"{output_directory}/{s_ins.instance_name}-{src_db_name}-{src_table_name}.txt"
        )
        if no_delete:
            args["no-delete"] = True
    elif mode == "purge":
        args["purge"] = True

    # Validate parameters.
    args_check_result = pt_archiver.check_args(args)
    if args_check_result["status"] == 1:
        return JsonResponse(args_check_result)
    # Convert parameters.
    cmd_args = pt_archiver.generate_args2cmd(args)
    # Execute command and collect results.
    select_cnt = 0
    insert_cnt = 0
    delete_cnt = 0
    with FuncTimer() as t:
        p = pt_archiver.execute_cmd(cmd_args)
        stdout = ""
        for line in iter(p.stdout.readline, ""):
            if re.match(r"^SELECT\s(\d+)$", line, re.I):
                select_cnt = re.findall(r"^SELECT\s(\d+)$", line)
            elif re.match(r"^INSERT\s(\d+)$", line, re.I):
                insert_cnt = re.findall(r"^INSERT\s(\d+)$", line)
            elif re.match(r"^DELETE\s(\d+)$", line, re.I):
                delete_cnt = re.findall(r"^DELETE\s(\d+)$", line)
            stdout += f"{line}\n"
    statistics = stdout
    # Get error output.
    stderr = p.stderr.read()
    if stderr:
        statistics = stdout + stderr

    # Evaluate archive result.
    select_cnt = int(select_cnt[0]) if select_cnt else 0
    insert_cnt = int(insert_cnt[0]) if insert_cnt else 0
    delete_cnt = int(delete_cnt[0]) if delete_cnt else 0
    error_info = ""
    success = True
    if stderr:
        error_info = f"Command execution error: {stderr}"
        success = False
    if mode == "dest":
        # If deleting source data, check delete/write counts.
        if not no_delete and (insert_cnt != delete_cnt):
            error_info = f"Delete and insert counts do not match: {insert_cnt}!={delete_cnt}"
            success = False
    elif mode == "file":
        # If deleting source data, check select/delete counts.
        if not no_delete and (select_cnt != delete_cnt):
            error_info = f"Select and delete counts do not match: {select_cnt}!={delete_cnt}"
            success = False
    elif mode == "purge":
        # Purge mode: check select/delete counts.
        if select_cnt != delete_cnt:
            error_info = f"Select and delete counts do not match: {select_cnt}!={delete_cnt}"
            success = False

    # Save execution details to database.
    if connection.connection and not connection.is_usable():
        close_old_connections()
    # Update last archive timestamp.
    ArchiveConfig(id=archive_id, last_archive_time=t.end).save(
        update_fields=["last_archive_time"]
    )
    # Mask passwords before storing command.
    shell_cmd = " ".join(cmd_args)
    ArchiveLog.objects.create(
        archive=archive_info,
        cmd=(
            shell_cmd.replace(s_ins.password, "***").replace(d_ins.password, "***")
            if mode == "dest"
            else shell_cmd.replace(s_ins.password, "***")
        ),
        condition=condition,
        mode=mode,
        no_delete=no_delete,
        sleep=sleep,
        select_cnt=select_cnt,
        insert_cnt=insert_cnt,
        delete_cnt=delete_cnt,
        statistics=statistics,
        success=success,
        error_info=error_info,
        start_time=t.start,
        end_time=t.end,
    )
    if not success:
        raise Exception(f"{error_info}\n{statistics}")


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
        ArchiveConfig(id=archive_id, state=state).save(update_fields=["state"])
        return JsonResponse({"status": 0, "msg": "ok", "data": {}})
    except Exception as msg:
        return JsonResponse({"status": 1, "msg": f"{msg}", "data": {}})


@permission_required("sql.archive_mgt", raise_exception=True)
def archive_once(request):
    """Trigger archive task once immediately."""
    archive_id = request.GET.get("archive_id")
    async_task(
        "sql.archiver.archive",
        archive_id,
        timeout=-1,
        task_name=f"archive-{archive_id}",
    )
    return JsonResponse({"status": 0, "msg": "ok", "data": {}})
