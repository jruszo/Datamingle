# -*- coding: UTF-8 -*-
"""
@author: hhyo
@license: Apache Licence
@file: sql_optimize.py
@time: 2019/03/04
"""

import MySQLdb
import re

import simplejson as json
import sqlparse
from django.contrib.auth.decorators import permission_required
from django.http import HttpResponse
from common.config import SysConfig
from common.utils.extend_json_encoder import ExtendJSONEncoder
from sql.engines import get_engine
from sql.models import Instance
from sql.plugins.soar import Soar
from sql.plugins.sqladvisor import SQLAdvisor
from sql.sql_tuning import SqlTuning
from sql.utils.resource_group import user_instances

__author__ = "hhyo"


@permission_required("sql.optimize_sqladvisor", raise_exception=True)
def optimize_sqladvisor(request):
    sql_content = request.POST.get("sql_content")
    instance_name = request.POST.get("instance_name")
    db_name = request.POST.get("db_name")
    verbose = request.POST.get("verbose", 1)
    result = {"status": 0, "msg": "ok", "data": []}

    # Server-side parameter validation.
    if sql_content is None or instance_name is None:
        result["status"] = 1
        result["msg"] = "Submitted page parameters may be empty"
        return HttpResponse(json.dumps(result), content_type="application/json")

    try:
        instance_info = user_instances(request.user, db_type=["mysql"]).get(
            instance_name=instance_name
        )
    except Instance.DoesNotExist:
        result["status"] = 1
        result["msg"] = "Your group is not associated with this instance!"
        return HttpResponse(json.dumps(result), content_type="application/json")

    # Check SQLAdvisor program path.
    sqladvisor_path = SysConfig().get("sqladvisor")
    if sqladvisor_path is None:
        result["status"] = 1
        result["msg"] = "Please configure the SQLAdvisor path!"
        return HttpResponse(json.dumps(result), content_type="application/json")

    # Submit to SQLAdvisor for analysis report.
    sqladvisor = SQLAdvisor()
    # Prepare parameters.
    args = {
        "h": instance_info.host,
        "P": instance_info.port,
        "u": instance_info.user,
        "p": instance_info.password,
        "d": db_name,
        "v": verbose,
        "q": sql_content.strip(),
    }

    # Validate parameters.
    args_check_result = sqladvisor.check_args(args)
    if args_check_result["status"] == 1:
        return HttpResponse(
            json.dumps(args_check_result), content_type="application/json"
        )
    # Convert parameters.
    cmd_args = sqladvisor.generate_args2cmd(args)
    # Execute command.
    try:
        stdout, stderr = sqladvisor.execute_cmd(cmd_args).communicate()
        result["data"] = f"{stdout}{stderr}"
    except RuntimeError as e:
        result["status"] = 1
        result["msg"] = str(e)
    return HttpResponse(json.dumps(result), content_type="application/json")


@permission_required("sql.optimize_soar", raise_exception=True)
def optimize_soar(request):
    instance_name = request.POST.get("instance_name")
    db_name = request.POST.get("db_name")
    sql = request.POST.get("sql")
    result = {"status": 0, "msg": "ok", "data": []}

    # Server-side parameter validation.
    if not (instance_name and db_name and sql):
        result["status"] = 1
        result["msg"] = "Submitted page parameters may be empty"
        return HttpResponse(json.dumps(result), content_type="application/json")
    try:
        instance = user_instances(request.user, db_type=["mysql"]).get(
            instance_name=instance_name
        )
    except Exception:
        result["status"] = 1
        result["msg"] = "Your group is not associated with this instance"
        return HttpResponse(json.dumps(result), content_type="application/json")

    # Check test DSN and Soar program path.
    soar_test_dsn = SysConfig().get("soar_test_dsn")
    soar_path = SysConfig().get("soar")
    if not (soar_path and soar_test_dsn):
        result["status"] = 1
        result["msg"] = "Please configure soar_path and test_dsn!"
        return HttpResponse(json.dumps(result), content_type="application/json")

    # Connection info for target instance.
    online_dsn = (
        f"{instance.user}:{instance.password}@{instance.host}:{instance.port}/{db_name}"
    )

    # Submit to Soar for analysis report.
    soar = Soar()
    # Prepare parameters.
    args = {
        "online-dsn": online_dsn,
        "test-dsn": soar_test_dsn,
        "allow-online-as-test": False,
        "report-type": "markdown",
        "query": sql.strip(),
    }
    # Validate parameters.
    args_check_result = soar.check_args(args)
    if args_check_result["status"] == 1:
        return HttpResponse(
            json.dumps(args_check_result), content_type="application/json"
        )
    # Convert parameters.
    cmd_args = soar.generate_args2cmd(args)
    # Execute command.
    try:
        stdout, stderr = soar.execute_cmd(cmd_args).communicate()
        result["data"] = stdout if stdout else stderr
    except RuntimeError as e:
        result["status"] = 1
        result["msg"] = str(e)
    return HttpResponse(json.dumps(result), content_type="application/json")


@permission_required("sql.optimize_sqltuning", raise_exception=True)
def optimize_sqltuning(request):
    instance_name = request.POST.get("instance_name")
    db_name = request.POST.get("db_name")
    sqltext = request.POST.get("sql_content")
    option = request.POST.getlist("option[]")
    sqltext = sqlparse.format(sqltext, strip_comments=True)
    sqltext = sqlparse.split(sqltext)[0]
    if re.match(r"^select|^show|^explain", sqltext, re.I) is None:
        result = {"status": 1, "msg": "Only query SQL is supported!", "data": []}
        return HttpResponse(json.dumps(result), content_type="application/json")
    try:
        user_instances(request.user).get(instance_name=instance_name)
    except Instance.DoesNotExist:
        result = {
            "status": 1,
            "msg": "Your group is not associated with this instance!",
            "data": [],
        }
        return HttpResponse(json.dumps(result), content_type="application/json")

    sql_tunning = SqlTuning(
        instance_name=instance_name, db_name=db_name, sqltext=sqltext
    )
    result = {"status": 0, "msg": "ok", "data": {}}
    if "sys_parm" in option:
        basic_information = sql_tunning.basic_information()
        sys_parameter = sql_tunning.sys_parameter()
        optimizer_switch = sql_tunning.optimizer_switch()
        result["data"]["basic_information"] = basic_information
        result["data"]["sys_parameter"] = sys_parameter
        result["data"]["optimizer_switch"] = optimizer_switch
    if "sql_plan" in option:
        plan, optimizer_rewrite_sql = sql_tunning.sqlplan()
        result["data"]["optimizer_rewrite_sql"] = optimizer_rewrite_sql
        result["data"]["plan"] = plan
    if "obj_stat" in option:
        result["data"]["object_statistics"] = sql_tunning.object_statistics()
    if "sql_profile" in option:
        session_status = sql_tunning.exec_sql()
        result["data"]["session_status"] = session_status
    # Close connection.
    sql_tunning.engine.close()
    result["data"]["sqltext"] = sqltext
    return HttpResponse(
        json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
        content_type="application/json",
    )


def explain(request):
    """
    Get SQL execution plan from SQL optimization page.
    :param request:
    :return:
    """
    sql_content = request.POST.get("sql_content")
    instance_name = request.POST.get("instance_name")
    db_name = request.POST.get("db_name")
    result = {"status": 0, "msg": "ok", "data": []}

    # Server-side parameter validation.
    if sql_content is None or instance_name is None:
        result["status"] = 1
        result["msg"] = "Submitted page parameters may be empty"
        return HttpResponse(json.dumps(result), content_type="application/json")

    try:
        instance = user_instances(request.user).get(instance_name=instance_name)
    except Instance.DoesNotExist:
        result = {"status": 1, "msg": "Instance does not exist", "data": []}
        return HttpResponse(json.dumps(result), content_type="application/json")

    # Remove comments, validate syntax, and execute first valid SQL.
    sql_content = sqlparse.format(sql_content.strip(), strip_comments=True)
    try:
        sql_content = sqlparse.split(sql_content)[0]
    except IndexError:
        result["status"] = 1
        result["msg"] = "No valid SQL statement found"
        return HttpResponse(json.dumps(result), content_type="application/json")
    else:
        # Filter out statements that do not start with EXPLAIN.
        if not re.match(r"^explain", sql_content, re.I):
            result["status"] = 1
            result["msg"] = (
                "Only statements starting with EXPLAIN are supported. Please check."
            )
            return HttpResponse(json.dumps(result), content_type="application/json")

    # Execute and get execution plan.
    query_engine = get_engine(instance=instance)
    db_name = query_engine.escape_string(db_name)
    sql_result = query_engine.query(str(db_name), sql_content).to_sep_dict()
    result["data"] = sql_result

    # Return query result.
    return HttpResponse(
        json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
        content_type="application/json",
    )


def optimize_sqltuningadvisor(request):
    """
    Get optimization report using SQLTuningAdvisor.
    :param request:
    :return:
    """
    sql_content = request.POST.get("sql_content")
    instance_name = request.POST.get("instance_name")
    db_name = request.POST.get("schema_name")
    result = {"status": 0, "msg": "ok", "data": []}

    # Server-side parameter validation.
    if sql_content is None or instance_name is None:
        result["status"] = 1
        result["msg"] = "Submitted page parameters may be empty"
        return HttpResponse(json.dumps(result), content_type="application/json")

    try:
        instance = user_instances(request.user).get(instance_name=instance_name)
    except Instance.DoesNotExist:
        result = {"status": 1, "msg": "Instance does not exist", "data": []}
        return HttpResponse(json.dumps(result), content_type="application/json")

    # Keep comments to preserve hints, then validate and execute first SQL.
    sql_content = sqlparse.format(sql_content.strip(), strip_comments=False)
    # Escape single quotes for PL/SQL syntax support.
    sql_content = sql_content.replace("'", "''")
    try:
        sql_content = sqlparse.split(sql_content)[0]
    except IndexError:
        result["status"] = 1
        result["msg"] = "No valid SQL statement found"
        return HttpResponse(json.dumps(result), content_type="application/json")
    else:
        # Filter non-Oracle statements.
        if not instance.db_type == "oracle":
            result["status"] = 1
            result["msg"] = "SQLTuningAdvisor only supports Oracle database checks"
            return HttpResponse(json.dumps(result), content_type="application/json")

    # Execute and get optimization report.
    query_engine = get_engine(instance=instance)
    db_name = query_engine.escape_string(db_name)
    sql_result = query_engine.sqltuningadvisor(str(db_name), sql_content).to_sep_dict()
    result["data"] = sql_result

    # Return query result.
    return HttpResponse(
        json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
        content_type="application/json",
    )
