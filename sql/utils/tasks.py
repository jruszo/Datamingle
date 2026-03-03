# -*- coding:utf-8 -*-
from django_q.tasks import schedule
from django_q.models import Schedule
from django.conf import settings

import logging

logger = logging.getLogger("default")


def add_sql_schedule(name, run_date, workflow_id):
    """Add or update a scheduled SQL task."""
    del_schedule(name)
    schedule(
        "sql.utils.execute_sql.execute",
        workflow_id,
        hook="sql.utils.execute_sql.execute_callback",
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
    cluster_name = settings.Q_CLUSTER.get("name", "archery")
    schedule(
        "sql.query.kill_query_conn",
        instance_id,
        thread_id,
        name=name,
        schedule_type="O",
        next_run=run_date,
        repeats=1,
        timeout=-1,
        cluster=cluster_name,
    )


def add_sync_ding_user_schedule():
    """Add a scheduled task to sync DingTalk user IDs."""
    del_schedule(name="Sync DingTalk User IDs")
    schedule(
        "common.utils.ding_api.sync_ding_user_id",
        name="Sync DingTalk User IDs",
        schedule_type="D",
        repeats=-1,
        timeout=-1,
    )


def del_schedule(name):
    """Delete a schedule."""
    try:
        sql_schedule = Schedule.objects.get(name=name)
        Schedule.delete(sql_schedule)
        logger.debug(f"Deleted schedule: {name}")
    except Schedule.DoesNotExist:
        pass


def task_info(name):
    """Get schedule details."""
    try:
        sql_schedule = Schedule.objects.get(name=name)
        return sql_schedule
    except Schedule.DoesNotExist:
        pass
