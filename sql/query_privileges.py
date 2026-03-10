# -*- coding: UTF-8 -*-
"""
@author: hhyo
@license: Apache Licence
@file: query_privileges.py
@time: 2019/03/24
"""

import logging
import datetime
import re
import traceback

import simplejson as json
from django.contrib.auth.decorators import permission_required
from django.db import transaction
from django.db.models import Q
from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render
from django.urls import reverse
from django_q.tasks import async_task

from common.config import SysConfig
from common.utils.const import WorkflowStatus, WorkflowType, WorkflowAction
from common.utils.extend_json_encoder import ExtendJSONEncoder
from sql.engines.goinception import GoInceptionEngine
from sql.models import QueryPrivilegesApply, QueryPrivileges, Instance, ResourceGroup
from sql.notify import notify_for_audit
from sql.utils.resource_group import user_groups, user_instances, user_member_groups
from sql.utils.workflow_audit import Audit, AuditException, get_auditor
from sql.utils.sql_utils import extract_tables

logger = logging.getLogger("default")

__author__ = "hhyo"


# TODO: move syntax parsing/validation in permission checks into each engine.
def query_priv_check(user, instance, db_name, sql_content, limit_num):
    """
    Query permission validation.
    :param user:
    :param instance:
    :param db_name:
    :param sql_content:
    :param limit_num:
    :return:
    """
    result = {"status": 0, "msg": "ok", "data": {"priv_check": True, "limit_num": 0}}
    # If user has query_all_instances, treat as admin and only apply limit.
    # superuser already has all privileges.
    if user.has_perm("sql.query_all_instances"):
        priv_limit = int(SysConfig().get("admin_query_limit", 5000))
        result["data"]["limit_num"] = (
            min(priv_limit, limit_num) if limit_num else priv_limit
        )
        return result
    # If user has query_resource_group_instance, treat as group admin.
    if user.has_perm("sql.query_resource_group_instance"):
        if user_instances(user, tag_codes=["can_read"]).filter(pk=instance.pk).exists():
            priv_limit = int(SysConfig().get("admin_query_limit", 5000))
            result["data"]["limit_num"] = (
                min(priv_limit, limit_num) if limit_num else priv_limit
            )
            return result

    # Only MySQL performs table-level permission checks.
    if instance.db_type == "mysql":
        try:
            # Skip permission check for EXPLAIN and SHOW CREATE.
            if re.match(r"^explain|^show\s+create", sql_content, re.I):
                return result
            # Validate permissions for other statements.
            table_ref = _table_ref(sql_content, instance, db_name)
            # Loop-based checks may be slower, but query table count is usually small.
            for table in table_ref:
                # Fail if user has neither db-level nor table-level permission.
                if not _db_priv(user, instance, table["schema"]) and not _tb_priv(
                    user, instance, table["schema"], table["name"]
                ):
                    # Status 2 indicates permission denial.
                    result["status"] = 2
                    result["msg"] = (
                        f"You do not have query permission on "
                        f"{table['schema']}.{table['name']}. "
                        "Please apply in Query Permission Management first."
                    )
                    return result
            # Use the smallest limit among involved db/table privileges.
            for table in table_ref:
                priv_limit = _priv_limit(
                    user, instance, db_name=table["schema"], tb_name=table["name"]
                )
                limit_num = min(priv_limit, limit_num) if limit_num else priv_limit
            result["data"]["limit_num"] = limit_num
        except Exception as msg:
            logger.error(
                "Unable to validate query permission, "
                f"{instance.instance_name}, {sql_content}, {traceback.format_exc()}"
            )
            result["status"] = 1
            result["msg"] = (
                f"Unable to validate query permission. "
                f"Contact admin. Error details: {msg}"
            )
    # For other instance types, only database-level permissions are checked.
    else:
        # Get databases referenced by SQL.
        # redis/mssql/pgsql only check the currently selected database.
        if instance.db_type in ["redis", "mssql", "pgsql"]:
            dbs = [db_name]
        else:
            dbs = [
                i["schema"].strip("`")
                for i in extract_tables(sql_content)
                if i["schema"] is not None
            ]
            dbs.append(db_name)
        # Deduplicate database list.
        dbs = list(set(dbs))
        # Sort database names.
        dbs.sort()
        # Validate db permission and fail fast.
        for db_name in dbs:
            if not _db_priv(user, instance, db_name):
                # Status 2 indicates permission denial.
                result["status"] = 2
                result["msg"] = (
                    f"You do not have query permission on database {db_name}. "
                    "Please apply in Query Permission Management first."
                )
                return result
        # With all db permissions, choose the minimum limit.
        for db_name in dbs:
            priv_limit = _priv_limit(user, instance, db_name=db_name)
            limit_num = min(priv_limit, limit_num) if limit_num else priv_limit
        result["data"]["limit_num"] = limit_num
    return result


@permission_required("sql.menu_queryapplylist", raise_exception=True)
def query_priv_apply_list(request):
    """
    Get query permission request list.
    :param request:
    :return:
    """
    user = request.user
    limit = int(request.POST.get("limit", 0))
    offset = int(request.POST.get("offset", 0))
    limit = offset + limit
    search = request.POST.get("search", "")

    query_privs = QueryPrivilegesApply.objects.all()
    # Apply search filter (title/user fuzzy match).
    if search:
        query_privs = query_privs.filter(
            Q(title__icontains=search) | Q(user_display__icontains=search)
        )
    # Admin users can view all records.
    if user.is_superuser:
        query_privs = query_privs
    # Users with review permission can view all workflows in their groups.
    elif user.has_perm("sql.query_review"):
        # Get user's directly assigned resource groups first.
        group_list = user_member_groups(user)
        group_ids = [group.group_id for group in group_list]
        query_privs = query_privs.filter(group_id__in=group_ids)
    # Others can only view workflows they submitted.
    else:
        query_privs = query_privs.filter(user_name=user.username)

    count = query_privs.count()
    lists = query_privs.order_by("-apply_id")[offset:limit].values(
        "apply_id",
        "title",
        "instance__instance_name",
        "db_list",
        "priv_type",
        "table_list",
        "limit_num",
        "valid_date",
        "user_display",
        "status",
        "create_time",
        "group_name",
    )

    # Serialize QuerySet.
    rows = [row for row in lists]

    result = {"total": count, "rows": rows}
    # Return query result.
    return HttpResponse(
        json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
        content_type="application/json",
    )


@permission_required("sql.query_applypriv", raise_exception=True)
def query_priv_apply(request):
    """
    Apply for query permission.
    :param request:
    :return:
    """
    title = request.POST["title"]
    instance_name = request.POST.get("instance_name")
    group_name = request.POST.get("group_name")
    group_id = ResourceGroup.objects.get(group_name=group_name).group_id
    priv_type = request.POST.get("priv_type")
    db_name = request.POST.get("db_name")
    db_list = request.POST.getlist("db_list[]")
    table_list = request.POST.getlist("table_list[]")
    valid_date = request.POST.get("valid_date")
    limit_num = request.POST.get("limit_num")

    # Get user info.
    user = request.user

    # Server-side parameter validation.
    result = {"status": 0, "msg": "ok", "data": []}
    if int(priv_type) == 1:
        if not (title and instance_name and db_list and valid_date and limit_num):
            result["status"] = 1
            result["msg"] = "Please fill in all required fields"
            return HttpResponse(json.dumps(result), content_type="application/json")
    elif int(priv_type) == 2:
        if not (
            title
            and instance_name
            and db_name
            and valid_date
            and table_list
            and limit_num
        ):
            result["status"] = 1
            result["msg"] = "Please fill in all required fields"
            return HttpResponse(json.dumps(result), content_type="application/json")
    try:
        user_instances(request.user, tag_codes=["can_read"]).get(
            instance_name=instance_name
        )
    except Instance.DoesNotExist:
        result["status"] = 1
        result["msg"] = "Your group is not associated with this instance!"
        return HttpResponse(json.dumps(result), content_type="application/json")

    # Database-level permission.
    ins = Instance.objects.get(instance_name=instance_name)
    if int(priv_type) == 1:
        # Check whether user already has database query permission.
        for db_name in db_list:
            if _db_priv(user, ins, db_name):
                result["status"] = 1
                result["msg"] = (
                    f"You already have database permission for {db_name} "
                    f"on instance {instance_name}; duplicate request is not allowed."
                )
                return HttpResponse(json.dumps(result), content_type="application/json")

    # Table-level permission.
    elif int(priv_type) == 2:
        # First check whether user already has database permission.
        if _db_priv(user, ins, db_name):
            result["status"] = 1
            result["msg"] = (
                f"You already have full database permission for {db_name} "
                f"on instance {instance_name}; duplicate request is not allowed."
            )
            return HttpResponse(json.dumps(result), content_type="application/json")
        # Check whether user already has query permission on the table.
        for tb_name in table_list:
            if _tb_priv(user, ins, db_name, tb_name):
                result["status"] = 1
                result["msg"] = (
                    f"You already have query permission on {db_name}.{tb_name} "
                    f"for instance {instance_name}; duplicate request is not allowed."
                )
                return HttpResponse(json.dumps(result), content_type="application/json")

    apply_info = QueryPrivilegesApply(
        title=title,
        group_id=group_id,
        group_name=group_name,
        # audit_auth_groups is temporarily empty here.
        audit_auth_groups="",
        user_name=user.username,
        user_display=user.display,
        instance=ins,
        priv_type=int(priv_type),
        valid_date=valid_date,
        status=WorkflowStatus.WAITING,
        limit_num=limit_num,
    )
    if int(priv_type) == 1:
        apply_info.db_list = ",".join(db_list)
        apply_info.table_list = ""
    elif int(priv_type) == 2:
        apply_info.db_list = db_name
        apply_info.table_list = ",".join(table_list)
    audit_handler = get_auditor(workflow=apply_info)
    # Keep data consistent using a transaction.
    try:
        with transaction.atomic():
            audit_handler.create_audit()
    except AuditException as e:
        logger.error(f"Failed to create approval flow, {str(e)}")
        result["status"] = 1
        result["msg"] = "Failed to create approval flow, please contact admin"
        return HttpResponse(json.dumps(result), content_type="application/json")
    _query_apply_audit_call_back(
        audit_handler.workflow.apply_id, audit_handler.audit.current_status
    )
    # Send notification.
    async_task(
        notify_for_audit,
        workflow_audit=audit_handler.audit,
        timeout=60,
        task_name=f"query-priv-apply-{audit_handler.workflow.apply_id}",
    )
    return HttpResponse(json.dumps(result), content_type="application/json")


@permission_required("sql.menu_queryapplylist", raise_exception=True)
def user_query_priv(request):
    """
    User query permission management.
    :param request:
    :return:
    """
    user = request.user
    user_display = request.POST.get("user_display", "all")
    limit = int(request.POST.get("limit"))
    offset = int(request.POST.get("offset"))
    limit = offset + limit
    search = request.POST.get("search", "")

    user_query_privs = QueryPrivileges.objects.filter(
        is_deleted=0, valid_date__gte=datetime.datetime.now()
    )
    # Apply search filter (user/db/table fuzzy match).
    if search:
        user_query_privs = user_query_privs.filter(
            Q(user_display__icontains=search)
            | Q(db_name__icontains=search)
            | Q(table_name__icontains=search)
        )
    # Filter user.
    if user_display != "all":
        user_query_privs = user_query_privs.filter(user_display=user_display)
    # Admin users can view all records.
    if user.is_superuser:
        user_query_privs = user_query_privs
    # Users with management permission can view workflows in their groups.
    elif user.has_perm("sql.query_mgtpriv"):
        # Get user's directly assigned resource groups first.
        group_list = user_member_groups(user)
        group_ids = [group.group_id for group in group_list]
        user_query_privs = user_query_privs.filter(
            instance__queryprivilegesapply__group_id__in=group_ids
        )
    # Others can only view workflows they submitted.
    else:
        user_query_privs = user_query_privs.filter(user_name=user.username)

    privileges_count = user_query_privs.distinct().count()
    privileges_list = (
        user_query_privs.distinct()
        .order_by("-privilege_id")[offset:limit]
        .values(
            "privilege_id",
            "user_display",
            "instance__instance_name",
            "db_name",
            "priv_type",
            "table_name",
            "limit_num",
            "valid_date",
        )
    )

    # Serialize QuerySet.
    rows = [row for row in privileges_list]

    result = {"total": privileges_count, "rows": rows}
    # Return query result.
    return HttpResponse(
        json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
        content_type="application/json",
    )


@permission_required("sql.query_mgtpriv", raise_exception=True)
def query_priv_modify(request):
    """
    Modify privilege information.
    :param request:
    :return:
    """
    privilege_id = request.POST.get("privilege_id")
    type = request.POST.get("type")
    result = {"status": 0, "msg": "ok", "data": []}

    # type=1 delete privilege, type=2 modify privilege
    try:
        privilege = QueryPrivileges.objects.get(privilege_id=int(privilege_id))
    except QueryPrivileges.DoesNotExist:
        result["msg"] = "Target privilege does not exist"
        result["status"] = 1
        return HttpResponse(json.dumps(result), content_type="application/json")

    if int(type) == 1:
        # Delete privilege.
        privilege.is_deleted = 1
        privilege.save(update_fields=["is_deleted"])
        return HttpResponse(json.dumps(result), content_type="application/json")
    elif int(type) == 2:
        # Modify privilege.
        valid_date = request.POST.get("valid_date")
        limit_num = request.POST.get("limit_num")
        privilege.valid_date = valid_date
        privilege.limit_num = limit_num
        privilege.save(update_fields=["valid_date", "limit_num"])
        return HttpResponse(json.dumps(result), content_type="application/json")


@permission_required("sql.query_review", raise_exception=True)
def query_priv_audit(request):
    """
    Query permission audit.
    :param request:
    :return:
    """
    # Get user input.
    apply_id = int(request.POST["apply_id"])
    try:
        audit_status = WorkflowAction(int(request.POST["audit_status"]))
    except ValueError as e:
        return render(
            request,
            "error.html",
            {"errMsg": f"Invalid audit_status parameter, {str(e)}"},
        )
    audit_remark = request.POST.get("audit_remark")

    if not audit_remark:
        audit_remark = ""

    try:
        sql_query_apply = QueryPrivilegesApply.objects.get(apply_id=apply_id)
    except QueryPrivilegesApply.DoesNotExist:
        return render(request, "error.html", {"errMsg": "Workflow does not exist"})
    auditor = get_auditor(workflow=sql_query_apply)
    # Keep data consistent using a transaction.
    with transaction.atomic():
        try:
            workflow_audit_detail = auditor.operate(
                audit_status, request.user, audit_remark
            )
        except AuditException as e:
            return render(request, "error.html", {"errMsg": f"Audit failed: {str(e)}"})
        # Unified callback that handles grants and data updates.
        _query_apply_audit_call_back(
            auditor.audit.workflow_id, auditor.audit.current_status
        )

    # Send notification.
    async_task(
        notify_for_audit,
        workflow_audit=auditor.audit,
        workflow_audit_detail=workflow_audit_detail,
        timeout=60,
        task_name=f"query-priv-audit-{apply_id}",
    )

    return HttpResponseRedirect(reverse("sql:queryapplydetail", args=(apply_id,)))


def _table_ref(sql_content, instance, db_name):
    """
    Parse syntax tree and get referenced tables for permission checks.
    :param sql_content:
    :param instance:
    :param db_name:
    :return:
    """
    engine = GoInceptionEngine()
    query_tree = engine.query_print(
        instance=instance, db_name=db_name, sql=sql_content
    ).get("query_tree")
    return engine.get_table_ref(json.loads(query_tree), db_name=db_name)


def _db_priv(user, instance, db_name):
    """
    Check whether user has permission on specified database.
    :param user: user object
    :param instance: instance object
    :param db_name: database name
    :return: limit_num if permission exists, otherwise False
    TODO: return int consistently, and 0 for not found.
    """
    if user.is_superuser:
        return int(SysConfig().get("admin_query_limit", 5000))
    # Get user's database privileges.
    user_privileges = QueryPrivileges.objects.filter(
        user_name=user.username,
        instance=instance,
        db_name__in=[str(db_name), str("*")],
        valid_date__gte=datetime.datetime.now(),
        is_deleted=0,
        priv_type=1,
    )
    if user_privileges.exists():
        return user_privileges.first().limit_num
    return False


def _tb_priv(user, instance, db_name, tb_name):
    """
    Check whether user has permission on specified table.
    :param user: user object
    :param instance: instance object
    :param db_name: database name
    :param tb_name: table name
    :return: limit_num if permission exists, otherwise False
    """
    # Get user's table privileges.
    user_privileges = QueryPrivileges.objects.filter(
        user_name=user.username,
        instance=instance,
        db_name=str(db_name),
        table_name=str(tb_name),
        valid_date__gte=datetime.datetime.now(),
        is_deleted=0,
        priv_type=2,
    )
    if user.is_superuser:
        return int(SysConfig().get("admin_query_limit", 5000))
    else:
        if user_privileges.exists():
            return user_privileges.first().limit_num
    return False


def _priv_limit(user, instance, db_name, tb_name=None):
    """
    Get the minimum limit of user's query privileges for result restriction.
    :param db_name:
    :param tb_name: optional, if empty returns db-level privilege
    :return:
    """
    # Get db/table limit values.
    db_limit_num = _db_priv(user, instance, db_name)
    if tb_name:
        tb_limit_num = _tb_priv(user, instance, db_name, tb_name)
    else:
        tb_limit_num = None
    # Return minimum value.
    if db_limit_num and tb_limit_num:
        return min(db_limit_num, tb_limit_num)
    elif db_limit_num:
        return db_limit_num
    elif tb_limit_num:
        return tb_limit_num
    else:
        raise RuntimeError("User has no valid privileges!")


def _query_apply_audit_call_back(apply_id, workflow_status):
    """
    Workflow audit callback for query permission applications.
    :param apply_id: application id
    :param workflow_status: audit result
    :return:
    """
    # Update business table status.
    apply_info = QueryPrivilegesApply.objects.get(apply_id=apply_id)
    apply_info.status = workflow_status
    apply_info.save()
    # Insert privileges on approval using bulk insert for better performance.
    if workflow_status == WorkflowStatus.PASSED:
        apply_queryset = QueryPrivilegesApply.objects.get(apply_id=apply_id)
        # Database-level privileges.

        if apply_queryset.priv_type == 1:
            insert_list = [
                QueryPrivileges(
                    user_name=apply_queryset.user_name,
                    user_display=apply_queryset.user_display,
                    instance=apply_queryset.instance,
                    db_name=db_name,
                    table_name=apply_queryset.table_list,
                    valid_date=apply_queryset.valid_date,
                    limit_num=apply_queryset.limit_num,
                    priv_type=apply_queryset.priv_type,
                )
                for db_name in apply_queryset.db_list.split(",")
            ]
        # Table-level privileges.
        elif apply_queryset.priv_type == 2:
            insert_list = [
                QueryPrivileges(
                    user_name=apply_queryset.user_name,
                    user_display=apply_queryset.user_display,
                    instance=apply_queryset.instance,
                    db_name=apply_queryset.db_list,
                    table_name=table_name,
                    valid_date=apply_queryset.valid_date,
                    limit_num=apply_queryset.limit_num,
                    priv_type=apply_queryset.priv_type,
                )
                for table_name in apply_queryset.table_list.split(",")
            ]
        QueryPrivileges.objects.bulk_create(insert_list)
