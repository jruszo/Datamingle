# -*- coding: UTF-8 -*-
import datetime
import logging
import re
import time
import traceback

import simplejson as json
from django.contrib.auth.decorators import permission_required
from django.db import connection, close_old_connections
from django.db.models import Q
from django.http import HttpResponse
from common.config import SysConfig
from common.utils.extend_json_encoder import ExtendJSONEncoder, ExtendJSONEncoderFTime
from common.utils.openai import OpenaiClient, check_openai_config
from common.utils.timer import FuncTimer
from sql.query_privileges import query_priv_check
from sql.utils.resource_group import user_instances
from sql.utils.tasks import add_kill_conn_schedule, del_schedule
from .models import QueryLog, Instance
from sql.engines import get_engine

logger = logging.getLogger("default")


@permission_required("sql.query_submit", raise_exception=True)
def query(request):
    """
    Get SQL query result.
    :param request:
    :return:
    """
    instance_name = request.POST.get("instance_name")
    sql_content = request.POST.get("sql_content")
    db_name = request.POST.get("db_name")
    tb_name = request.POST.get("tb_name")
    limit_num = int(request.POST.get("limit_num", 0))
    schema_name = request.POST.get("schema_name", None)
    user = request.user

    result = {"status": 0, "msg": "ok", "data": {}}
    try:
        instance = user_instances(request.user).get(instance_name=instance_name)
    except Instance.DoesNotExist:
        result["status"] = 1
        result["msg"] = "Your group is not associated with this instance"
        return HttpResponse(json.dumps(result), content_type="application/json")

    # Server-side parameter validation.
    if None in [sql_content, db_name, instance_name, limit_num]:
        result["status"] = 1
        result["msg"] = "Submitted parameters may be empty"
        return HttpResponse(json.dumps(result), content_type="application/json")

    try:
        config = SysConfig()
        # Pre-query checks: forbidden SQL check and SQL splitting.
        query_engine = get_engine(instance=instance)
        query_check_info = query_engine.query_check(db_name=db_name, sql=sql_content)
        if query_check_info.get("bad_query"):
            # Marked as bad_query by engine.
            result["status"] = 1
            result["msg"] = query_check_info.get("msg")
            return HttpResponse(json.dumps(result), content_type="application/json")
        if query_check_info.get("has_star") and config.get("disable_star") is True:
            # Engine detected SELECT * while disable_star is enabled.
            result["status"] = 1
            result["msg"] = query_check_info.get("msg")
            return HttpResponse(json.dumps(result), content_type="application/json")
        sql_content = query_check_info["filtered_sql"]

        # Permission check and effective limit_num.
        priv_check_info = query_priv_check(
            user, instance, db_name, sql_content, limit_num
        )
        if priv_check_info["status"] == 0:
            limit_num = priv_check_info["data"]["limit_num"]
            priv_check = priv_check_info["data"]["priv_check"]
        else:
            result["status"] = priv_check_info["status"]
            result["msg"] = priv_check_info["msg"]
            return HttpResponse(json.dumps(result), content_type="application/json")
        # EXPLAIN should run without limit.
        limit_num = 0 if re.match(r"^explain", sql_content.lower()) else limit_num

        # Add limit or rewrite query SQL.
        sql_content = query_engine.filter_sql(sql=sql_content, limit_num=limit_num)

        # Get connection in advance for reuse and session kill scheduling.
        query_engine.get_connection(db_name=db_name)
        thread_id = query_engine.thread_id
        max_execution_time = int(config.get("max_execution_time", 60))
        # Execute query and add kill schedule for timeout protection.
        if thread_id:
            schedule_name = f"query-{time.time()}"
            run_date = datetime.datetime.now() + datetime.timedelta(
                seconds=max_execution_time
            )
            add_kill_conn_schedule(schedule_name, run_date, instance.id, thread_id)
        with FuncTimer() as t:
            # Get replication lag info.
            seconds_behind_master = query_engine.seconds_behind_master
            query_result = query_engine.query(
                db_name,
                sql_content,
                limit_num,
                schema_name=schema_name,
                tb_name=tb_name,
                max_execution_time=max_execution_time * 1000,
            )
        query_result.query_time = t.cost
        # Remove schedule after query returns.
        if thread_id:
            del_schedule(schedule_name)

        # Query error.
        if query_result.error:
            result["status"] = 1
            result["msg"] = query_result.error
        # Data masking for successful query result sets.
        elif config.get("data_masking"):
            try:
                with FuncTimer() as t:
                    masking_result = query_engine.query_masking(
                        db_name, sql_content, query_result
                    )
                masking_result.mask_time = t.cost
                # Masking error.
                if masking_result.error:
                    # If query_check enabled, return error directly.
                    if config.get("query_check"):
                        result["status"] = 1
                        result["msg"] = f"Data masking error: {masking_result.error}"
                    # If query_check disabled, allow query and return unmasked data.
                    else:
                        logger.warning(
                            "Data masking error, allowed by config. "
                            f"SQL: {sql_content}, error: {masking_result.error}"
                        )
                        query_result.error = None
                        result["data"] = query_result.__dict__
                # Masking succeeded.
                else:
                    result["data"] = masking_result.__dict__
            except Exception as msg:
                logger.error(traceback.format_exc())
                # Unexpected masking exception.
                if config.get("query_check"):
                    result["status"] = 1
                    result["msg"] = (
                        f"Data masking error, contact admin. Error details: {msg}"
                    )
                # If query_check disabled, allow query and return unmasked data.
                else:
                    logger.warning(
                        "Data masking error, allowed by config. "
                        f"SQL: {sql_content}, error: {msg}"
                    )
                    query_result.error = None
                    result["data"] = query_result.__dict__
        # Statements not requiring masking.
        else:
            result["data"] = query_result.__dict__

        # Persist only successful query logs.
        if not query_result.error:
            result["data"]["seconds_behind_master"] = seconds_behind_master
            if int(limit_num) == 0:
                limit_num = int(query_result.affected_rows)
            else:
                limit_num = min(int(limit_num), int(query_result.affected_rows))
            # Avoid stale DB connections after timeout.
            if connection.connection and not connection.is_usable():
                close_old_connections()
        else:
            limit_num = 0
        query_log = QueryLog(
            username=user.username,
            user_display=user.display,
            db_name=db_name,
            instance_name=instance.instance_name,
            sqllog=sql_content,
            effect_row=limit_num,
            cost_time=query_result.query_time,
            priv_check=priv_check,
            hit_rule=query_result.mask_rule_hit,
            masking=query_result.is_masked,
        )
        query_log.save()
    except Exception as e:
        logger.error(
            "Query error.\n"
            f"SQL: {sql_content}\n"
            f"Error details: {traceback.format_exc()}"
        )
        result["status"] = 1
        result["msg"] = f"Query error, details: {e}"
        return HttpResponse(json.dumps(result), content_type="application/json")
    # Return query result.
    try:
        return HttpResponse(
            json.dumps(
                result,
                use_decimal=False,
                cls=ExtendJSONEncoderFTime,
                bigint_as_string=True,
            ),
            content_type="application/json",
        )
    # Response is valid, but may still produce garbled text in edge cases.
    except UnicodeDecodeError:
        return HttpResponse(
            json.dumps(result, default=str, bigint_as_string=True, encoding="latin1"),
            content_type="application/json",
        )


@permission_required("sql.menu_sqlquery", raise_exception=True)
def querylog(request):
    return _querylog(request)


@permission_required("sql.audit_user", raise_exception=True)
def querylog_audit(request):
    return _querylog(request)


def _querylog(request):
    """
    Get SQL query logs.
    :param request:
    :return:
    """
    # Get user.
    user = request.user

    limit = int(request.GET.get("limit", 0))
    offset = int(request.GET.get("offset", 0))
    limit = offset + limit
    limit = limit if limit else None
    star = True if request.GET.get("star") == "true" else False
    query_log_id = request.GET.get("query_log_id")
    search = request.GET.get("search", "")
    start_date = request.GET.get("start_date", "")
    end_date = request.GET.get("end_date", "")

    # Build filter options.
    filter_dict = dict()
    # Favorite only.
    if star:
        filter_dict["favorite"] = star
    # Query log alias.
    if query_log_id:
        filter_dict["id"] = query_log_id

    # The normal query-history screen is always scoped to the current user.
    filter_dict["username"] = user.username

    if start_date and end_date:
        end_date = datetime.datetime.strptime(
            end_date, "%Y-%m-%d"
        ) + datetime.timedelta(days=1)
        filter_dict["create_time__range"] = (start_date, end_date)

    # Apply combined filters.
    sql_log = QueryLog.objects.filter(**filter_dict)

    # Apply search filter.
    sql_log = sql_log.filter(
        Q(sqllog__icontains=search)
        | Q(user_display__icontains=search)
        | Q(alias__icontains=search)
    )

    sql_log_count = sql_log.count()
    sql_log_list = sql_log.order_by("-id")[offset:limit].values(
        "id",
        "instance_name",
        "db_name",
        "sqllog",
        "effect_row",
        "cost_time",
        "user_display",
        "favorite",
        "alias",
        "create_time",
    )
    # Serialize QuerySet.
    rows = [row for row in sql_log_list]
    result = {"total": sql_log_count, "rows": rows}
    # Return query result.
    return HttpResponse(
        json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
        content_type="application/json",
    )


@permission_required("sql.menu_sqlquery", raise_exception=True)
def favorite(request):
    """
    Favorite query log and set alias.
    :param request:
    :return:
    """
    query_log_id = request.POST.get("query_log_id")
    star = True if request.POST.get("star") == "true" else False
    alias = request.POST.get("alias")
    query_log = QueryLog.objects.filter(
        id=query_log_id, username=request.user.username
    ).first()
    if not query_log:
        return HttpResponse(
            json.dumps({"status": 1, "msg": "Query log does not exist."}),
            content_type="application/json",
            status=400,
        )
    query_log.favorite = star
    query_log.alias = alias
    query_log.save(update_fields=["favorite", "alias"])
    # Return query result.
    return HttpResponse(
        json.dumps({"status": 0, "msg": "ok"}), content_type="application/json"
    )


def kill_query_conn(instance_id, thread_id):
    """Terminate query session, used by schedule task."""
    instance = Instance.objects.get(pk=instance_id)
    query_engine = get_engine(instance)
    query_engine.kill_connection(thread_id)


@permission_required("sql.menu_sqlquery", raise_exception=True)
def generate_sql(request):
    """
    Generate query SQL using AI from schema info and query description.
    :param request:
    :return:
    """
    query_desc = request.POST.get("query_desc")
    db_type = request.POST.get("db_type")
    if not query_desc or not db_type:
        return HttpResponse(
            json.dumps(
                {"status": 1, "msg": "query_desc or db_type does not exist", "data": []}
            ),
            content_type="application/json",
        )

    instance_name = request.POST.get("instance_name")
    try:
        instance = Instance.objects.get(instance_name=instance_name)
    except Instance.DoesNotExist:
        return HttpResponse(
            json.dumps({"status": 1, "msg": "Instance does not exist", "data": []}),
            content_type="application/json",
        )
    db_name = request.POST.get("db_name")
    schema_name = request.POST.get("schema_name")
    tb_name = request.POST.get("tb_name")

    result = {"status": 0, "msg": "ok", "data": ""}
    try:
        query_engine = get_engine(instance=instance)
        query_result = query_engine.describe_table(
            db_name, tb_name, schema_name=schema_name
        )
        openai_client = OpenaiClient()
        # Some engines may not have table schema, such as Redis.
        if len(query_result.rows) != 0:
            result["data"] = openai_client.generate_sql_by_openai(
                db_type, query_result.rows[0][-1], query_desc
            )
        else:
            result["data"] = openai_client.generate_sql_by_openai(
                db_type, "", query_desc
            )
    except Exception as msg:
        result["status"] = 1
        result["msg"] = str(msg)
    return HttpResponse(json.dumps(result), content_type="application/json")


def check_openai(request):
    """
    Validate whether OpenAI configuration exists.
    :param request:
    :return:
    """
    config_validate = check_openai_config()
    if not config_validate:
        return HttpResponse(
            json.dumps(
                {
                    "status": 1,
                    "msg": (
                        "OpenAI config is missing. Required keys: "
                        "[openai_base_url, openai_api_key, default_chat_model]"
                    ),
                    "data": False,
                }
            ),
            content_type="application/json",
        )

    return HttpResponse(
        json.dumps({"status": 0, "msg": "ok", "data": True}),
        content_type="application/json",
    )
