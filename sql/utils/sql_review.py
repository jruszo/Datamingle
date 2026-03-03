import datetime
import json
import re
from django.db import transaction

from sql.engines.models import ReviewResult
from sql.models import SqlWorkflow
from common.config import SysConfig
from sql.utils.resource_group import user_groups
from sql.utils.sql_utils import remove_comments


def can_execute(user, workflow_id):
    """
    Determine whether the user can execute now.
    User has execution permission in two cases:
    1. User has resource-group-level execution permission and is in the group.
    2. User is the submitter and has execution permission.
    :param user:
    :param workflow_id:
    :return:
    """
    result = False
    # Ensure current workflow status is executable.
    with transaction.atomic():
        workflow_detail = SqlWorkflow.objects.select_for_update().get(id=workflow_id)
        # Only approved and scheduled workflows can be executed immediately.
        if workflow_detail.status not in [
            "workflow_review_pass",
            "workflow_timingtask",
        ]:
            return False
    # User has resource-group-level execution permission and is in the group.
    group_ids = [group.group_id for group in user_groups(user)]
    if workflow_detail.group_id in group_ids and user.has_perm(
        "sql.sql_execute_for_resource_group"
    ):
        result = True
    # User is submitter and has execution permission.
    if workflow_detail.engineer == user.username and user.has_perm("sql.sql_execute"):
        result = True
    return result


def on_correct_time_period(workflow_id, run_date=None):
    """
    Check whether current time is within executable time period.
    Includes manual execution and scheduled execution.
    :param workflow_id:
    :param run_date:
    :return:
    """
    workflow_detail = SqlWorkflow.objects.get(id=workflow_id)
    result = True
    ctime = run_date or datetime.datetime.now()
    stime = workflow_detail.run_date_start
    etime = workflow_detail.run_date_end
    if (stime and stime > ctime) or (etime and etime < ctime):
        result = False
    return result


def can_timingtask(user, workflow_id):
    """
    Determine whether the user can schedule execution now.
    User has scheduling permission in two cases:
    1. User has resource-group-level execution permission and is in the group.
    2. User is the submitter and has execution permission.
    :param user:
    :param workflow_id:
    :return:
    """
    workflow_detail = SqlWorkflow.objects.get(id=workflow_id)
    result = False
    # Only approved and scheduled workflows can be executed.
    if workflow_detail.status in ["workflow_review_pass", "workflow_timingtask"]:
        # User has resource-group-level execution permission and is in the group.
        group_ids = [group.group_id for group in user_groups(user)]
        if workflow_detail.group_id in group_ids and user.has_perm(
            "sql.sql_execute_for_resource_group"
        ):
            result = True
        # User is submitter and has execution permission.
        if workflow_detail.engineer == user.username and user.has_perm(
            "sql.sql_execute"
        ):
            result = True
    return result


def can_cancel(user, workflow_id):
    """
    Determine whether current user can cancel workflow.
    For in-review and approved workflows, reviewer and submitter can cancel.
    :param user:
    :param workflow_id:
    :return:
    """
    workflow_detail = SqlWorkflow.objects.get(id=workflow_id)
    result = False
    # In-review workflows: reviewer and submitter can cancel.
    if workflow_detail.status == "workflow_manreviewing":
        from sql.utils.workflow_audit import Audit

        return any(
            [
                Audit.can_review(user, workflow_id, 2),
                user.username == workflow_detail.engineer,
            ]
        )
    elif workflow_detail.status in ["workflow_review_pass", "workflow_timingtask"]:
        return any(
            [can_execute(user, workflow_id), user.username == workflow_detail.engineer]
        )
    return result


def can_view(user, workflow_id):
    """
    Determine whether current user can view workflow details.
    Keep logic consistent with workflow list filtering.
    :param user:
    :param workflow_id:
    :return:
    """
    workflow_detail = SqlWorkflow.objects.get(id=workflow_id)
    result = False
    # Superuser can view all workflows.
    if user.is_superuser:
        result = True
    # Non-admin users with review permission or resource-group-level execution
    # permission can view all workflows in their groups.
    elif user.has_perm("sql.sql_review") or user.has_perm(
        "sql.sql_execute_for_resource_group"
    ):
        # Get user's resource groups first.
        group_list = user_groups(user)
        group_ids = [group.group_id for group in group_list]
        if workflow_detail.group_id in group_ids:
            result = True
    # Others can only view workflows submitted by themselves.
    else:
        if workflow_detail.engineer == user.username:
            result = True
    return result


def can_rollback(user, workflow_id):
    """
    Determine whether current user can view rollback details.
    Keep behavior consistent with workflow detail page.
    Rollback can be viewed only for finished/exception workflows with backup enabled.
    :param user:
    :param workflow_id:
    :return:
    """
    workflow_detail = SqlWorkflow.objects.get(id=workflow_id)
    result = False
    # Rollback is available only after execution ends and backup is enabled.
    if workflow_detail.is_backup and workflow_detail.status in (
        "workflow_finish",
        "workflow_exception",
    ):
        return can_view(user, workflow_id)
    return result
