# -*- coding: UTF-8 -*-
import logging
import traceback

from django.db import close_old_connections, connection, transaction
from django_redis import get_redis_connection
from common.utils.const import WorkflowStatus, WorkflowType
from common.config import SysConfig
from sql.engines.models import ReviewResult, ReviewSet
from sql.models import SqlWorkflow
from sql.notify import notify_for_execute, EventType
from sql.utils.workflow_audit import Audit
from sql.engines import get_engine
from sql.offlinedownload import OffLineDownLoad

logger = logging.getLogger("default")


def execute(workflow_id, user=None):
    """Execute for delayed/async tasks with workflow ID and executor info."""
    # Use current read to prevent duplicate execution.
    with transaction.atomic():
        workflow_detail = SqlWorkflow.objects.select_for_update().get(id=workflow_id)
        # Only queued and scheduled workflows can continue execution.
        if workflow_detail.status not in ["workflow_queuing", "workflow_timingtask"]:
            raise Exception("Invalid workflow status, execution is not allowed!")
        # Update workflow status to executing.
        else:
            SqlWorkflow(id=workflow_id, status="workflow_executing").save(
                update_fields=["status"]
            )
    # Add execution log.
    audit_id = Audit.detail_by_workflow_id(
        workflow_id=workflow_id, workflow_type=WorkflowType.SQL_REVIEW
    ).audit_id
    Audit.add_log(
        audit_id=audit_id,
        operation_type=5,
        operation_type_desc="Execute workflow",
        operation_info=(
            "Workflow execution started"
            if user
            else "System scheduled workflow execution"
        ),
        operator=user.username if user else "",
        operator_display=user.display if user else "System",
    )
    execute_engine = get_engine(instance=workflow_detail.instance)
    if workflow_detail.is_offline_export:
        return OffLineDownLoad().execute_offline_download(workflow=workflow_detail)
    else:
        return execute_engine.execute_workflow(workflow=workflow_detail)


def execute_callback(task):
    """Callback for async tasks to persist execution result.
    Uses django-q hook with the full task object.
    task.result is the actual result.
    """
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
    Audit.add_log(
        audit_id=audit_id,
        operation_type=6,
        operation_type_desc="Execution finished",
        operation_info="Execution result: {}".format(workflow.get_status_display()),
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
