# -*- coding: UTF-8 -*-
import logging
import traceback

from django.db import close_old_connections, connection, transaction
from django.utils import timezone
from django_redis import get_redis_connection
from common.utils.const import WorkflowType
from common.config import SysConfig
from api_agents.services import dispatch_sql_workflow_to_agent
from sql.engines.models import ReviewResult, ReviewSet
from sql.mailbox import emit_execution_finished_notifications, resolve_mailbox_items
from sql.models import SqlWorkflow
from sql.notify import notify_for_execute
from sql.utils.workflow_audit import Audit

logger = logging.getLogger("default")


def dispatch_scheduled_agent_execution(workflow_id, execution_options=None):
    """Dispatch a scheduled SQL workflow to its assigned agent."""
    workflow = SqlWorkflow.objects.select_related("instance").get(id=workflow_id)
    if workflow.status != "workflow_timingtask":
        raise Exception("Invalid workflow status, scheduled execution is not allowed!")

    audit = Audit.detail_by_workflow_id(
        workflow_id=workflow_id, workflow_type=WorkflowType.SQL_REVIEW
    )
    executor_id = (execution_options or {}).get("executor")
    operation_info = "Scheduled workflow dispatched to agent"
    if executor_id:
        operation_info = f"{operation_info} (executor: {executor_id})"

    if audit is not None:
        Audit.add_log(
            audit_id=audit.audit_id,
            operation_type=5,
            operation_type_desc="Execute workflow",
            operation_info=operation_info,
            operator="",
            operator_display="System",
        )

    try:
        command = dispatch_sql_workflow_to_agent(
            workflow, user=None, executor=executor_id
        )
    except Exception as exc:
        logger.exception(
            "Failed to dispatch scheduled workflow %s to agent.", workflow_id
        )
        _mark_agent_dispatch_failed(workflow_id, str(exc))
        raise
    return {"agent_dispatched": True, "command_id": command.id}


def execute(workflow_id, user=None, execution_options=None):
    """Compatibility task entrypoint that dispatches execution to the agent."""
    workflow_detail = SqlWorkflow.objects.select_related("instance").get(id=workflow_id)
    if workflow_detail.status not in ["workflow_queuing", "workflow_timingtask"]:
        raise Exception("Invalid workflow status, execution is not allowed!")

    try:
        workflow_for_mailbox = SqlWorkflow.objects.select_related("instance").get(
            id=workflow_id
        )
        resolve_mailbox_items(workflow_for_mailbox, category="execution_needed")
    except Exception:
        logger.exception(
            "Failed to resolve mailbox items for workflow_id=%s category=%s "
            "before execution started.",
            workflow_id,
            "execution_needed",
        )
    # Add execution log.
    audit_id = Audit.detail_by_workflow_id(
        workflow_id=workflow_id, workflow_type=WorkflowType.SQL_REVIEW
    ).audit_id
    executor_id = (execution_options or {}).get("executor")
    operation_info = (
        "Workflow dispatched to agent"
        if user
        else "System scheduled workflow dispatched to agent"
    )
    if executor_id:
        operation_info = f"{operation_info} (executor: {executor_id})"
    Audit.add_log(
        audit_id=audit_id,
        operation_type=5,
        operation_type_desc="Execute workflow",
        operation_info=operation_info,
        operator=user.username if user else "",
        operator_display=user.display if user else "System",
    )
    try:
        command = dispatch_sql_workflow_to_agent(
            workflow_detail, user=user, executor=executor_id
        )
    except Exception as exc:
        logger.exception("Failed to dispatch workflow %s to agent.", workflow_id)
        _mark_agent_dispatch_failed(workflow_id, str(exc))
        raise
    return {"agent_dispatched": True, "command_id": command.id}


def execute_callback(task):
    """Callback for async tasks to persist execution result.
    Uses the task queue hook with the full task object.
    task.result is the actual result.
    """
    if isinstance(task.result, dict) and task.result.get("agent_dispatched"):
        return

    # https://stackoverflow.com/questions/7835272/django-operationalerror-2006-mysql-server-has-gone-away
    if connection.connection and not connection.is_usable():
        close_old_connections()
    workflow_id = task.args[0]
    # Only executing workflows are allowed to update execution result.
    with transaction.atomic():
        workflow = SqlWorkflow.objects.get(id=workflow_id)
        if workflow.status != "workflow_executing":
            raise Exception(
                f"Workflow {workflow.id} has invalid status, duplicate result update is not allowed!"
            )

    workflow.finish_time = task.stopped

    if not task.success:
        # Failed task returns error stack info; build an error result.
        workflow.status = "workflow_exception"
        execute_result = ReviewSet(full_sql=workflow.sqlworkflowcontent.sql_content)
        execute_result.rows = [
            ReviewResult(
                stage="Execute failed",
                errlevel=2,
                stagestatus="Aborted unexpectedly",
                errormessage=task.result,
                sql=workflow.sqlworkflowcontent.sql_content,
            )
        ]
    elif task.result.warning or task.result.error:
        execute_result = task.result
        workflow.status = "workflow_exception"
    else:
        execute_result = task.result
        workflow.status = "workflow_finish"
    try:
        # Save execution result.
        workflow.sqlworkflowcontent.execute_result = execute_result.json()
        workflow.sqlworkflowcontent.save()
        workflow.save()
    except Exception as e:
        logger.error(
            f"SQL workflow callback exception: {workflow_id} {traceback.format_exc()}"
        )
        SqlWorkflow.objects.filter(id=workflow_id).update(
            finish_time=task.stopped,
            status="workflow_exception",
        )
        workflow.sqlworkflowcontent.execute_result = {f"{e}"}
        workflow.sqlworkflowcontent.save()
    # Add workflow log.
    audit_id = Audit.detail_by_workflow_id(
        workflow_id=workflow_id, workflow_type=WorkflowType.SQL_REVIEW
    ).audit_id
    # Keep this wording stable for tests and notifications, independent of locale.
    status_desc = (
        "finished successfully"
        if workflow.status == "workflow_finish"
        else workflow.get_status_display()
    )
    Audit.add_log(
        audit_id=audit_id,
        operation_type=6,
        operation_type_desc="Execution finished",
        operation_info="Execution result: {}".format(status_desc),
        operator="",
        operator_display="System",
    )

    # Clear instance resource cache after DDL workflow completion.
    if workflow.syntax_type == 1:
        r = get_redis_connection("default")
        for key in r.scan_iter(match="*insRes*", count=2000):
            r.delete(key)

    # Send notification only when Execute phase notification is enabled.
    sys_config = SysConfig()
    is_notified = (
        "Execute" in sys_config.get("notify_phase_control").split(",")
        if sys_config.get("notify_phase_control")
        else True
    )
    if is_notified:
        notify_for_execute(workflow)
    try:
        resolve_mailbox_items(workflow, category="execution_needed")
    except Exception:
        logger.exception(
            "Failed to resolve mailbox items for workflow_id=%s category=%s "
            "during execute callback.",
            workflow.id,
            "execution_needed",
        )
    emit_execution_finished_notifications(
        workflow,
        outcome="success" if workflow.status == "workflow_finish" else "failure",
        actor=(
            task.args[1]
            if len(task.args) > 1 and hasattr(task.args[1], "username")
            else None
        ),
        dedupe_suffix=(
            workflow.finish_time.strftime("%Y%m%d%H%M%S%f")
            if workflow.finish_time
            else f"workflow-{workflow.id}"
        ),
    )


def _mark_agent_dispatch_failed(workflow_id, message):
    workflow = SqlWorkflow.objects.select_related("sqlworkflowcontent").get(
        id=workflow_id
    )
    workflow.status = "workflow_exception"
    workflow.finish_time = timezone.now()
    execute_result = ReviewSet(full_sql=workflow.sqlworkflowcontent.sql_content)
    execute_result.rows = [
        ReviewResult(
            stage="Execute failed",
            errlevel=2,
            stagestatus="Agent dispatch failed",
            errormessage=message,
            sql=workflow.sqlworkflowcontent.sql_content,
        )
    ]
    workflow.sqlworkflowcontent.execute_result = execute_result.json()
    workflow.sqlworkflowcontent.save(update_fields=["execute_result"])
    workflow.save(update_fields=["status", "finish_time"])

    audit = Audit.detail_by_workflow_id(
        workflow_id=workflow_id, workflow_type=WorkflowType.SQL_REVIEW
    )
    if audit is not None:
        Audit.add_log(
            audit_id=audit.audit_id,
            operation_type=6,
            operation_type_desc="Execution finished",
            operation_info="Execution result: Agent dispatch failed",
            operator="",
            operator_display="System",
        )
    sys_config = SysConfig()
    is_notified = (
        "Execute" in sys_config.get("notify_phase_control").split(",")
        if sys_config.get("notify_phase_control")
        else True
    )
    if is_notified:
        notify_for_execute(workflow)
    try:
        resolve_mailbox_items(workflow, category="execution_needed")
    except Exception:
        logger.exception(
            "Failed to resolve mailbox items for workflow_id=%s after dispatch failure.",
            workflow.id,
        )
    emit_execution_finished_notifications(
        workflow,
        outcome="failure",
        actor=None,
        dedupe_suffix=workflow.finish_time.strftime("%Y%m%d%H%M%S%f"),
    )
