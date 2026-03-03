# -*- coding: UTF-8 -*-
import os
import traceback

from django.contrib.auth.decorators import permission_required
from django.contrib.auth.models import Group
from django.core.exceptions import PermissionDenied
from django.shortcuts import render, get_object_or_404
from django.http import HttpResponseRedirect, FileResponse, Http404
from django.urls import reverse

from django.conf import settings
from common.config import SysConfig
from sql.engines import get_engine, engine_map
from common.utils.permission import superuser_required
from common.utils.convert import Convert
from sql.utils.tasks import task_info
from sql.utils.resource_group import user_groups

from .models import (
    Users,
    SqlWorkflow,
    QueryPrivileges,
    ResourceGroup,
    QueryPrivilegesApply,
    Config,
    SQL_WORKFLOW_CHOICES,
    InstanceTag,
    Instance,
    QueryLog,
    ArchiveConfig,
    AuditEntry,
    TwoFactorAuthConfig,
)
from sql.utils.workflow_audit import Audit, AuditV2, AuditException
from sql.utils.sql_review import (
    can_execute,
    can_timingtask,
    can_cancel,
    can_view,
    can_rollback,
)
from common.utils.const import Const, WorkflowType, WorkflowAction
from sql.utils.resource_group import user_groups, user_instances

import logging

logger = logging.getLogger("default")


def index(request):
    index_path_url = SysConfig().get("index_path_url", "sqlworkflow")
    return HttpResponseRedirect(f"/{index_path_url.strip('/')}/")


def login(request):
    """Login page."""
    if request.user and request.user.is_authenticated:
        return HttpResponseRedirect("/")

    return render(
        request,
        "login.html",
        context={
            "sign_up_enabled": SysConfig().get("sign_up_enabled"),
            "oidc_enabled": settings.ENABLE_OIDC,
            "dingding_enabled": settings.ENABLE_DINGDING,
            "cas_enabled": settings.ENABLE_CAS,
            "oidc_btn_name": SysConfig().get("oidc_btn_name", "Log in with OIDC"),
        },
    )


def twofa(request):
    """2FA page."""
    if request.user.is_authenticated:
        return HttpResponseRedirect("/")

    username = request.session.get("user")
    if username:
        verify_mode = request.session.get("verify_mode")
        twofa_enabled = TwoFactorAuthConfig.objects.filter(username=username)
        user_auth_types = [twofa.auth_type for twofa in twofa_enabled]

        auth_types = []
        for user_auth_type in user_auth_types:
            auth_type = {}
            auth_type["code"] = user_auth_type
            if user_auth_type == "totp":
                auth_type["display"] = "Google Authenticator"
            elif user_auth_type == "sms":
                auth_type["display"] = "SMS verification code"
            auth_types.append(auth_type)
        if "sms" in user_auth_types:
            phone = TwoFactorAuthConfig.objects.get(
                username=username, auth_type="sms"
            ).phone
        else:
            phone = 0
    else:
        return HttpResponseRedirect("/login/")

    return render(
        request,
        "2fa.html",
        context={
            "verify_mode": verify_mode,
            "auth_types": auth_types,
            "username": username,
            "phone": phone,
        },
    )


@permission_required("sql.menu_dashboard", raise_exception=True)
def dashboard(request):
    """Dashboard page."""
    return render(request, "dashboard.html")


def sqlworkflow(request):
    """SQL review workflow list page."""
    user = request.user
    # Data for filter options
    filter_dict = dict()
    # Admin users can view all workflows
    if user.is_superuser or user.has_perm("sql.audit_user"):
        pass
    # Non-admin users with review or resource-group execution permission
    # can view all workflows in their groups.
    elif user.has_perm("sql.sql_review") or user.has_perm(
        "sql.sql_execute_for_resource_group"
    ):
        # Get the user's resource groups first
        group_list = user_groups(user)
        group_ids = [group.group_id for group in group_list]
        filter_dict["group_id__in"] = group_ids
    # Everyone else can only view their own workflows
    else:
        filter_dict["engineer"] = user.username
    instance_id = (
        SqlWorkflow.objects.filter(**filter_dict).values("instance_id").distinct()
    )
    instance = Instance.objects.filter(pk__in=instance_id).order_by(
        Convert("instance_name", "gbk").asc()
    )
    resource_group_id = (
        SqlWorkflow.objects.filter(**filter_dict).values("group_id").distinct()
    )
    resource_group = ResourceGroup.objects.filter(group_id__in=resource_group_id)

    return render(
        request,
        "sqlworkflow.html",
        {
            "status_list": SQL_WORKFLOW_CHOICES,
            "instance": instance,
            "resource_group": resource_group,
        },
    )


def sqlexportworkflow(request):
    """SQL data export workflow list page."""
    user = request.user
    # Get all config values
    storage_type = SysConfig().get("storage_type")
    # Check offline download permission
    can_offline_download = user.is_superuser or user.has_perm("sql.offline_download")
    # Data for filter options
    filter_dict = dict()
    # Admin users can view all workflows
    if user.is_superuser or user.has_perm("sql.audit_user"):
        pass
    # Non-admin users with review or resource-group execution permission
    # can view all workflows in their groups.
    elif user.has_perm("sql.sql_review") or user.has_perm(
        "sql.sql_execute_for_resource_group"
    ):
        # Get the user's resource groups first
        group_list = user_groups(user)
        group_ids = [group.group_id for group in group_list]
        filter_dict["group_id__in"] = group_ids
    # Everyone else can only view their own workflows
    else:
        filter_dict["engineer"] = user.username
    instance_id = (
        SqlWorkflow.objects.filter(**filter_dict).values("instance_id").distinct()
    )
    instance = Instance.objects.filter(pk__in=instance_id).order_by(
        Convert("instance_name", "gbk").asc()
    )
    resource_group_id = (
        SqlWorkflow.objects.filter(**filter_dict).values("group_id").distinct()
    )
    resource_group = ResourceGroup.objects.filter(group_id__in=resource_group_id)

    return render(
        request,
        "sqlexportworkflow.html",
        {
            "status_list": SQL_WORKFLOW_CHOICES,
            "instance": instance,
            "resource_group": resource_group,
            "storage_type": storage_type,
            "can_offline_download": can_offline_download,
        },
    )


@permission_required("sql.sql_submit", raise_exception=True)
def submit_sql(request):
    """SQL submission page."""
    user = request.user
    # Get group information
    group_list = user_groups(user)

    # Get system config
    archer_config = SysConfig()

    # Ensure tag exists
    InstanceTag.objects.get_or_create(
        tag_code="can_write", defaults={"tag_name": "Supports SQL Review", "active": True}
    )

    context = {
        "group_list": group_list,
        "enable_backup_switch": archer_config.get("enable_backup_switch"),
        "engines": engine_map,
    }
    return render(request, "sqlsubmit.html", context)


def detail(request, workflow_id):
    """SQL workflow detail page."""
    workflow_detail = get_object_or_404(SqlWorkflow, pk=workflow_id)
    audit_handler = AuditV2(workflow=workflow_detail)
    if not can_view(request.user, workflow_id):
        raise PermissionDenied
    review_info = audit_handler.get_review_info()
    # For auto-review failures, no need to fetch the fields below.
    if workflow_detail.status != "workflow_autoreviewwrong":
        # Can review
        is_can_review = Audit.can_review(request.user, workflow_id, 2)
        # Can execute
        # TODO: pass workflow object into these checks to reduce repeated DB queries.
        is_can_execute = can_execute(request.user, workflow_id)
        # Can schedule
        is_can_timingtask = can_timingtask(request.user, workflow_id)
        # Can cancel
        is_can_cancel = can_cancel(request.user, workflow_id)
        # Can view rollback information
        is_can_rollback = can_rollback(request.user, workflow_id)

        # Get audit logs
        try:
            audit_detail = Audit.detail_by_workflow_id(
                workflow_id=workflow_id,
                workflow_type=WorkflowType.SQL_REVIEW,
            )
            audit_id = audit_detail.audit_id
            last_operation_info = (
                Audit.logs(audit_id=audit_id).latest("id").operation_info
            )
        except Exception as e:
            logger.debug(f"No audit log records found, error: {e}")
            last_operation_info = ""
    else:
        is_can_review = False
        is_can_execute = False
        is_can_timingtask = False
        is_can_cancel = False
        is_can_rollback = False
        last_operation_info = None

    # Get scheduled task information
    if workflow_detail.status == "workflow_timingtask":
        job_id = Const.workflowJobprefix["sqlreview"] + "-" + str(workflow_id)
        job = task_info(job_id)
        if job:
            run_date = job.next_run
        else:
            run_date = ""
    else:
        run_date = ""

    # Add current reviewer information
    current_reviewers = []
    for node in review_info.nodes:
        if node.is_current_node == False:
            continue
        for user in node.group.user_set.filter(is_active=1):
            # Ensure group_name and group.name use the same type.
            group_names = [group.group_name for group in user_groups(user)]
            if workflow_detail.group_name in group_names:
                current_reviewers.append(user)

    # Check whether manual execution confirmation is enabled.
    manual = SysConfig().get("manual")

    context = {
        "workflow_detail": workflow_detail,
        "current_reviewers": current_reviewers,
        "last_operation_info": last_operation_info,
        "is_can_review": is_can_review,
        "is_can_execute": is_can_execute,
        "is_can_timingtask": is_can_timingtask,
        "is_can_cancel": is_can_cancel,
        "is_can_rollback": is_can_rollback,
        "review_info": review_info,
        "manual": manual,
        "run_date": run_date,
    }
    return render(request, "detail.html", context)


def rollback(request):
    """Rollback SQL page."""
    workflow_id = request.GET.get("workflow_id")
    if not can_rollback(request.user, workflow_id):
        raise PermissionDenied
    download = request.GET.get("download")
    if workflow_id == "" or workflow_id is None:
        context = {"errMsg": "workflow_id parameter is empty."}
        return render(request, "error.html", context)
    workflow = SqlWorkflow.objects.get(id=int(workflow_id))

    # Directly download rollback SQL
    if download:
        try:
            query_engine = get_engine(instance=workflow.instance)
            list_backup_sql = query_engine.get_rollback(workflow=workflow)
        except Exception as msg:
            logger.error(traceback.format_exc())
            context = {"errMsg": msg}
            return render(request, "error.html", context)

        # Get data and save into directory
        path = os.path.join(settings.BASE_DIR, "downloads/rollback")
        os.makedirs(path, exist_ok=True)
        file_name = f"{path}/rollback_{workflow_id}.sql"
        with open(file_name, "w") as f:
            for sql in list_backup_sql:
                f.write(f"/*{sql[0]}*/\n{sql[1]}\n")
        # Return response
        response = FileResponse(open(file_name, "rb"))
        response["Content-Type"] = "application/octet-stream"
        response["Content-Disposition"] = (
            f'attachment;filename="rollback_{workflow_id}.sql"'
        )
        return response
    # Fetch asynchronously and render in page; large datasets may load slowly.
    else:
        rollback_workflow_name = (
            f"[Rollback Workflow] Source workflow ID: {workflow_id}, {workflow.workflow_name}"
        )
        context = {
            "workflow_detail": workflow,
            "rollback_workflow_name": rollback_workflow_name,
        }
        return render(request, "rollback.html", context)


@permission_required("sql.menu_sqlanalyze", raise_exception=True)
def sqlanalyze(request):
    """SQL analysis page."""
    return render(request, "sqlanalyze.html")


@permission_required("sql.menu_query", raise_exception=True)
def sqlquery(request):
    """Online SQL query page."""
    # Ensure tag exists
    InstanceTag.objects.get_or_create(
        tag_code="can_read", defaults={"tag_name": "Supports Query", "active": True}
    )
    # Favorite statements
    user = request.user
    group_list = user_groups(user)
    storage_type = SysConfig().get("storage_type")

    favorites = QueryLog.objects.filter(username=user.username, favorite=True).values(
        "id", "alias"
    )
    can_download = 1 if user.has_perm("sql.query_download") or user.is_superuser else 0
    can_offline_download = user.has_perm("sql.offline_download") or user.is_superuser
    context = {
        "favorites": favorites,
        "can_download": can_download,
        "engines": engine_map,
        "group_list": group_list,
        "storage_type": storage_type,
        "can_offline_download": can_offline_download,
    }
    return render(request, "sqlquery.html", context)


@permission_required("sql.menu_queryapplylist", raise_exception=True)
def queryapplylist(request):
    """Query permission request list page."""
    user = request.user
    # Get resource groups
    group_list = user_groups(user)

    context = {"group_list": group_list, "engines": engine_map}
    return render(request, "queryapplylist.html", context)


def queryapplydetail(request, apply_id):
    """Query permission request detail page."""
    workflow_detail = QueryPrivilegesApply.objects.get(apply_id=apply_id)
    # Get current approver and approval flow
    audit_handler = AuditV2(workflow=workflow_detail)
    review_info = audit_handler.get_review_info()

    # Can review
    is_can_review = Audit.can_review(request.user, apply_id, 1)
    # Get audit logs
    if workflow_detail.status == 2:
        try:
            audit_id = Audit.detail_by_workflow_id(
                workflow_id=apply_id, workflow_type=1
            ).audit_id
            last_operation_info = (
                Audit.logs(audit_id=audit_id).latest("id").operation_info
            )
        except Exception as e:
            logger.debug(f"No audit log records found, error: {e}")
            last_operation_info = ""
    else:
        last_operation_info = ""

    # Add current reviewer information
    current_reviewers = []
    for node in review_info.nodes:
        if node.is_current_node == False:
            continue
        for user in node.group.user_set.filter(is_active=1):
            # Ensure group_name and group.name use the same type.
            group_names = [group.group_name for group in user_groups(user)]
            if workflow_detail.group_name in group_names:
                current_reviewers.append(user)

    context = {
        "workflow_detail": workflow_detail,
        "current_reviewers": current_reviewers,
        "review_info": review_info,
        "last_operation_info": last_operation_info,
        "is_can_review": is_can_review,
    }
    return render(request, "queryapplydetail.html", context)


def queryuserprivileges(request):
    """Query permission management page."""
    # Get all users
    user_list = (
        QueryPrivileges.objects.filter(is_deleted=0).values("user_display").distinct()
    )
    context = {"user_list": user_list}
    return render(request, "queryuserprivileges.html", context)


@permission_required("sql.menu_sqladvisor", raise_exception=True)
def sqladvisor(request):
    """SQL optimization tool page."""
    return render(request, "sqladvisor.html")


@permission_required("sql.menu_slowquery", raise_exception=True)
def slowquery(request):
    """SQL slow log page."""
    return render(request, "slowquery.html")


@permission_required("sql.menu_instance", raise_exception=True)
def instance(request):
    """Instance management page."""
    # Get instance tags
    tags = InstanceTag.objects.filter(active=True)
    return render(request, "instance.html", {"tags": tags, "engines": engine_map})


@permission_required("sql.menu_instance_account", raise_exception=True)
def instanceaccount(request):
    """Instance account management page."""
    return render(request, "instanceaccount.html")


@permission_required("sql.menu_database", raise_exception=True)
def database(request):
    """Instance database management page."""
    # Get all active users as notification targets
    active_user = Users.objects.filter(is_active=1)

    return render(request, "database.html", {"active_user": active_user})


@permission_required("sql.menu_dbdiagnostic", raise_exception=True)
def dbdiagnostic(request):
    """Session management page."""
    return render(request, "dbdiagnostic.html")


@permission_required("sql.menu_data_dictionary", raise_exception=True)
def data_dictionary(request):
    """Data dictionary page."""
    return render(request, "data_dictionary.html", locals())


@permission_required("sql.menu_param", raise_exception=True)
def instance_param(request):
    """Instance parameter management page."""
    return render(request, "param.html")


@permission_required("sql.menu_my2sql", raise_exception=True)
def my2sql(request):
    """My2SQL page."""
    return render(request, "my2sql.html")


@permission_required("sql.menu_schemasync", raise_exception=True)
def schemasync(request):
    """Schema diff page."""
    return render(request, "schemasync.html")


@permission_required("sql.menu_archive", raise_exception=True)
def archive(request):
    """Archive list page."""
    # Get resource groups
    group_list = user_groups(request.user)
    ins_list = user_instances(request.user, db_type=["mysql"]).order_by(
        Convert("instance_name", "gbk").asc()
    )
    return render(
        request, "archive.html", {"group_list": group_list, "ins_list": ins_list}
    )


def archive_detail(request, id):
    """Archive detail page."""
    archive_config = ArchiveConfig.objects.get(pk=id)
    # Get current approver, approval flow, and whether review is allowed.
    audit_handler = AuditV2(
        workflow=archive_config, resource_group=archive_config.resource_group
    )
    review_info = audit_handler.get_review_info()
    try:
        audit_handler.can_operate(WorkflowAction.PASS, request.user)
        can_review = True
    except AuditException:
        can_review = False
    # Get audit logs
    if archive_config.status == 2:
        try:
            audit_id = Audit.detail_by_workflow_id(
                workflow_id=id, workflow_type=3
            ).audit_id
            last_operation_info = (
                Audit.logs(audit_id=audit_id).latest("id").operation_info
            )
        except Exception as e:
            logger.debug(f"No audit log records for archive config {id}, error: {e}")
            last_operation_info = ""
    else:
        last_operation_info = ""

    # Add current reviewer information
    current_reviewers = []
    for node in review_info.nodes:
        if node.is_current_node == False:
            continue
        for user in node.group.user_set.filter(is_active=1):
            # Ensure group_name and group.name use the same type.
            group_names = [group.group_name for group in user_groups(user)]
            if archive_config.resource_group.group_name in group_names:
                current_reviewers.append(user)

    context = {
        "archive_config": archive_config,
        "current_reviewers": current_reviewers,
        "review_info": review_info,
        "last_operation_info": last_operation_info,
        "can_review": can_review,
    }
    return render(request, "archivedetail.html", context)


@superuser_required
def config(request):
    """Configuration management page."""
    # Get all resource group names
    group_list = ResourceGroup.objects.all()
    # Get all permission groups
    auth_group_list = Group.objects.all()
    # Get all instance tags
    instance_tags = InstanceTag.objects.all()
    # Database types that support auto review
    db_type = ["mysql", "oracle", "mongo", "clickhouse", "redis", "doris"]
    # Get all config values
    all_config = Config.objects.all().values("item", "value")
    sys_config = {}
    for items in all_config:
        sys_config[items["item"]] = items["value"]

    # Set OpenAI defaults when config values are missing.
    if not sys_config.get("default_chat_model", ""):
        sys_config["default_chat_model"] = "gpt-3.5-turbo"
    if not sys_config.get("default_query_template", ""):
        sys_config["default_query_template"] = (
            "You are an engineer familiar with {{db_type}}. I will give you basic information and requirements. Generate one query for me. Do not return comments or numbering. Return only the query: {{table_schema}} \n {{user_input}}"
        )

    context = {
        "group_list": group_list,
        "auth_group_list": auth_group_list,
        "instance_tags": instance_tags,
        "db_type": db_type,
        "config": sys_config,
        "workflow_choices": WorkflowType,
    }
    return render(request, "config.html", context)


@superuser_required
def group(request):
    """Resource group management page."""
    return render(request, "group.html")


@superuser_required
def groupmgmt(request, group_id):
    """Resource group relation management page."""
    group = ResourceGroup.objects.get(group_id=group_id)
    return render(request, "groupmgmt.html", {"group": group})


def workflows(request):
    """Todo list page."""
    return render(request, "workflow.html")


def workflowsdetail(request, audit_id):
    """Todo detail."""
    # Return different detail pages by workflow_type.
    audit_detail = Audit.detail(audit_id)
    if not audit_detail:
        raise Http404("No corresponding workflow record exists")
    if audit_detail.workflow_type == WorkflowType.QUERY:
        return HttpResponseRedirect(
            reverse("sql:queryapplydetail", args=(audit_detail.workflow_id,))
        )
    elif audit_detail.workflow_type == WorkflowType.SQL_REVIEW:
        return HttpResponseRedirect(
            reverse("sql:detail", args=(audit_detail.workflow_id,))
        )
    elif audit_detail.workflow_type == WorkflowType.ARCHIVE:
        return HttpResponseRedirect(
            reverse("sql:archive_detail", args=(audit_detail.workflow_id,))
        )


@permission_required("sql.menu_document", raise_exception=True)
def dbaprinciples(request):
    """SQL documentation page."""
    # Read markdown file
    file = os.path.join(settings.BASE_DIR, "docs/docs.md")
    with open(file, "r", encoding="utf-8") as f:
        md = f.read().replace("\n", "\\n")
    return render(request, "dbaprinciples.html", {"md": md})


@permission_required("sql.audit_user", raise_exception=True)
def audit(request):
    """General audit log page."""
    _action_types = AuditEntry.objects.values_list("action").distinct()
    action_types = [i[0] for i in _action_types]
    return render(request, "audit.html", {"action_types": action_types})


@permission_required("sql.audit_user", raise_exception=True)
def audit_sqlquery(request):
    """Online SQL query audit page."""
    user = request.user
    favorites = QueryLog.objects.filter(username=user.username, favorite=True).values(
        "id", "alias"
    )
    return render(request, "audit_sqlquery.html", {"favorites": favorites})


def audit_sqlworkflow(request):
    """SQL review workflow list page."""
    user = request.user
    # Data for filter options
    filter_dict = dict()
    # Admin users can view all workflows
    if user.is_superuser or user.has_perm("sql.audit_user"):
        pass
    # Non-admin users with review or resource-group execution permission
    # can view all workflows in their groups.
    elif user.has_perm("sql.sql_review") or user.has_perm(
        "sql.sql_execute_for_resource_group"
    ):
        # Get the user's resource groups first
        group_list = user_groups(user)
        group_ids = [group.group_id for group in group_list]
        filter_dict["group_id__in"] = group_ids
    # Everyone else can only view their own workflows
    else:
        filter_dict["engineer"] = user.username
    instance_id = (
        SqlWorkflow.objects.filter(**filter_dict).values("instance_id").distinct()
    )
    instance = Instance.objects.filter(pk__in=instance_id)
    resource_group_id = (
        SqlWorkflow.objects.filter(**filter_dict).values("group_id").distinct()
    )
    resource_group = ResourceGroup.objects.filter(group_id__in=resource_group_id)

    return render(
        request,
        "audit_sqlworkflow.html",
        {
            "status_list": SQL_WORKFLOW_CHOICES,
            "instance": instance,
            "resource_group": resource_group,
        },
    )


@permission_required("sql.sqlexport_submit", raise_exception=True)
def sqlexportsubmit(request):
    """SQL export workflow submission page."""
    # Ensure tag exists
    InstanceTag.objects.get_or_create(
        tag_code="can_read", defaults={"tag_name": "Supports Query", "active": True}
    )
    # Favorite statements
    user = request.user
    group_list = user_groups(user)
    # Get all config values
    max_export_rows = SysConfig().get("max_export_rows")
    max_export_rows = int(max_export_rows) if max_export_rows else 10000

    favorites = QueryLog.objects.filter(username=user.username, favorite=True).values(
        "id", "alias"
    )
    can_download = user.has_perm("sql.query_download") or user.is_superuser
    can_offline_download = user.has_perm("sql.offline_download") or user.is_superuser
    context = {
        "favorites": favorites,
        "can_download": can_download,
        "engines": engine_map,
        "group_list": group_list,
        "max_export_rows": max_export_rows,
        "can_offline_download": can_offline_download,
    }
    return render(request, "sqlexportsubmit.html", context)
