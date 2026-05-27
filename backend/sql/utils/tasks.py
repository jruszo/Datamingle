import logging

from common.task_queue import (
    delete_schedule,
    schedule,
    task_info as get_task_info,
)

logger = logging.getLogger("default")


def add_sql_schedule(name, run_date, workflow_id, execution_options=None):
    """Add or update a scheduled SQL task."""
    del_schedule(name)
    schedule(
        "sql.utils.execute_sql.dispatch_scheduled_agent_execution",
        workflow_id,
        execution_options=execution_options or None,
        name=name,
        schedule_type="O",
        next_run=run_date,
        repeats=1,
        timeout=-1,
    )
    logger.debug(f"Added scheduled SQL execution task: {name}, run time: {run_date}")


def add_kill_conn_schedule(name, run_date, instance_id, thread_id):
    """Add or update a scheduled task to terminate database connections."""
    del_schedule(name)
    schedule(
        "sql.query.kill_query_conn",
        instance_id,
        thread_id,
        name=name,
        schedule_type="O",
        next_run=run_date,
        repeats=1,
        timeout=-1,
    )


def del_schedule(name):
    """Delete a schedule."""
    delete_schedule(name)
    logger.debug(f"Deleted schedule: {name}")


def task_info(name):
    """Get schedule details."""
    return get_task_info(name)
