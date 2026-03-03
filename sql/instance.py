# -*- coding: UTF-8 -*-

import MySQLdb
import os
import time

import simplejson as json
from django.conf import settings
from django.contrib.auth.decorators import permission_required
from django.http import HttpResponse
from django.views.decorators.cache import cache_page

from common.utils.extend_json_encoder import ExtendJSONEncoder
from common.utils.convert import Convert
from sql.engines import get_engine
from sql.plugins.schemasync import SchemaSync
from sql.utils.sql_utils import filter_db_list
from .models import Instance, ParamTemplate, ParamHistory


@permission_required("sql.menu_instance_list", raise_exception=True)
def lists(request):
    """Get instance list."""
    limit = int(request.POST.get("limit"))
    offset = int(request.POST.get("offset"))
    type = request.POST.get("type")
    db_type = request.POST.get("db_type")
    tags = request.POST.getlist("tags[]")
    limit = offset + limit
    search = request.POST.get("search", "")
    sortName = str(request.POST.get("sortName"))
    sortOrder = str(request.POST.get("sortOrder")).lower()

    # Build filter options.
    filter_dict = dict()
    # Filter by search keyword.
    if search:
        filter_dict["instance_name__icontains"] = search
    # Filter by instance type.
    if type:
        filter_dict["type"] = type
    # Filter by database type.
    if db_type:
        filter_dict["db_type"] = db_type

    instances = Instance.objects.filter(**filter_dict)
    # Filter by tags and return instances containing all selected tags.
    # TODO: loop may generate multiple joins and impact large datasets.
    if tags:
        for tag in tags:
            instances = instances.filter(instance_tag=tag, instance_tag__active=True)

    count = instances.count()
    if sortName == "instance_name":
        instances = instances.order_by(getattr(Convert(sortName, "gbk"), sortOrder)())[
            offset:limit
        ]
    else:
        instances = instances.order_by(
            "-" + sortName if sortOrder == "desc" else sortName
        )[offset:limit]
    instances = instances.values(
        "id", "instance_name", "db_type", "type", "host", "port", "user"
    )

    # Serialize QuerySet.
    rows = [row for row in instances]

    result = {"total": count, "rows": rows}
    return HttpResponse(
        json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
        content_type="application/json",
    )


@permission_required("sql.param_view", raise_exception=True)
def param_list(request):
    """
    Get instance parameter list.
    :param request:
    :return:
    """
    instance_id = request.POST.get("instance_id")
    editable = True if request.POST.get("editable") else False
    search = request.POST.get("search", "")
    try:
        ins = Instance.objects.get(id=instance_id)
    except Instance.DoesNotExist:
        result = {"status": 1, "msg": "Instance does not exist", "data": []}
        return HttpResponse(json.dumps(result), content_type="application/json")
    # Get configured parameter templates.
    cnf_params = dict()
    for param in ParamTemplate.objects.filter(
        db_type=ins.db_type, variable_name__contains=search
    ).values(
        "id",
        "variable_name",
        "default_value",
        "valid_values",
        "description",
        "editable",
    ):
        param["variable_name"] = param["variable_name"].lower()
        cnf_params[param["variable_name"]] = param
    # Get runtime instance parameters.
    engine = get_engine(instance=ins)
    ins_variables = engine.get_variables()
    # Build output rows.
    rows = list()
    for variable in ins_variables.rows:
        variable_name = variable[0].lower()
        row = {
            "variable_name": variable_name,
            "runtime_value": variable[1],
            "editable": False,
        }
        if variable_name in cnf_params.keys():
            row = dict(row, **cnf_params[variable_name])
        rows.append(row)
    # Apply editable filter.
    if editable:
        rows = [row for row in rows if row["editable"]]
    else:
        rows = [row for row in rows if not row["editable"]]
    return HttpResponse(
        json.dumps(rows, cls=ExtendJSONEncoder, bigint_as_string=True),
        content_type="application/json",
    )


@permission_required("sql.param_view", raise_exception=True)
def param_history(request):
    """Instance parameter change history."""
    limit = int(request.POST.get("limit"))
    offset = int(request.POST.get("offset"))
    limit = offset + limit
    instance_id = request.POST.get("instance_id")
    search = request.POST.get("search", "")
    phs = ParamHistory.objects.filter(instance__id=instance_id)
    # Apply search filter.
    if search:
        phs = ParamHistory.objects.filter(variable_name__contains=search)
    count = phs.count()
    phs = phs[offset:limit].values(
        "instance__instance_name",
        "variable_name",
        "old_var",
        "new_var",
        "user_display",
        "create_time",
    )
    # Serialize QuerySet.
    rows = [row for row in phs]

    result = {"total": count, "rows": rows}
    return HttpResponse(
        json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
        content_type="application/json",
    )


@permission_required("sql.param_edit", raise_exception=True)
def param_edit(request):
    user = request.user
    instance_id = request.POST.get("instance_id")
    variable_name = request.POST.get("variable_name")
    variable_value = request.POST.get("runtime_value")
    try:
        ins = Instance.objects.get(id=instance_id)
    except Instance.DoesNotExist:
        result = {"status": 1, "msg": "Instance does not exist", "data": []}
        return HttpResponse(json.dumps(result), content_type="application/json")

    # Update parameter.
    engine = get_engine(instance=ins)
    variable_name = engine.escape_string(variable_name)
    variable_value = engine.escape_string(variable_value)
    # Validate parameter template exists.
    if not ParamTemplate.objects.filter(variable_name=variable_name).exists():
        result = {
            "status": 1,
            "msg": "Please configure this parameter in parameter template first!",
            "data": [],
        }
        return HttpResponse(json.dumps(result), content_type="application/json")
    # Get current runtime value.
    runtime_value = engine.get_variables(variables=[variable_name]).rows[0][1]
    if variable_value == runtime_value:
        result = {
            "status": 1,
            "msg": "Parameter value matches runtime value; no update was made!",
            "data": [],
        }
        return HttpResponse(json.dumps(result), content_type="application/json")
    set_result = engine.set_variable(
        variable_name=variable_name, variable_value=variable_value
    )
    if set_result.error:
        result = {
            "status": 1,
            "msg": f"Set variable failed, error: {set_result.error}",
            "data": [],
        }
        return HttpResponse(json.dumps(result), content_type="application/json")
    # Save change history after successful update.
    else:
        ParamHistory.objects.create(
            instance=ins,
            variable_name=variable_name,
            old_var=runtime_value,
            new_var=variable_value,
            set_sql=set_result.full_sql,
            user_name=user.username,
            user_display=user.display,
        )
        result = {
            "status": 0,
            "msg": "Update succeeded. Please persist manually to config file!",
            "data": [],
        }
    return HttpResponse(json.dumps(result), content_type="application/json")


@permission_required("sql.menu_schemasync", raise_exception=True)
def schemasync(request):
    """Compare schema information between instances."""
    instance_name = request.POST.get("instance_name")
    db_name = request.POST.get("db_name")
    target_instance_name = request.POST.get("target_instance_name")
    target_db_name = request.POST.get("target_db_name")
    sync_auto_inc = True if request.POST.get("sync_auto_inc") == "true" else False
    sync_comments = True if request.POST.get("sync_comments") == "true" else False
    result = {
        "status": 0,
        "msg": "ok",
        "data": {"diff_stdout": "", "patch_stdout": "", "revert_stdout": ""},
    }

    # Compare all databases in loop mode.
    if db_name == "all" or target_db_name == "all":
        db_name = "*"
        target_db_name = "*"

    # Load instance connection information.
    instance = Instance.objects.get(instance_name=instance_name)
    target_instance = Instance.objects.get(instance_name=target_instance_name)

    # Run SchemaSync to get diff results.
    schema_sync = SchemaSync()
    # Prepare parameters.
    tag = int(time.time())
    output_directory = os.path.join(settings.BASE_DIR, "downloads/schemasync/")
    os.makedirs(output_directory, exist_ok=True)

    username, password = instance.get_username_password()
    target_username, target_password = target_instance.get_username_password()

    args = {
        "sync-auto-inc": sync_auto_inc,
        "sync-comments": sync_comments,
        "charset": "utf8mb4",
        "tag": tag,
        "output-directory": output_directory,
        "source": f"mysql://{username}:{password}@{instance.host}:{instance.port}/{db_name}",
        "target": f"mysql://{target_username}:{target_password}@{target_instance.host}:{target_instance.port}/{target_db_name}",
    }
    # Validate parameters.
    args_check_result = schema_sync.check_args(args)
    if args_check_result["status"] == 1:
        return HttpResponse(
            json.dumps(args_check_result), content_type="application/json"
        )
    # Convert parameters.
    cmd_args = schema_sync.generate_args2cmd(args)
    # Execute command.
    try:
        stdout, stderr = schema_sync.execute_cmd(cmd_args).communicate()
        diff_stdout = f"{stdout}{stderr}"
    except RuntimeError as e:
        diff_stdout = str(e)

    # For single-db comparison, return patch/revert scripts for frontend display.
    if db_name != "*":
        date = time.strftime("%Y%m%d", time.localtime())
        patch_sql_file = "%s%s_%s.%s.patch.sql" % (
            output_directory,
            target_db_name,
            tag,
            date,
        )
        revert_sql_file = "%s%s_%s.%s.revert.sql" % (
            output_directory,
            target_db_name,
            tag,
            date,
        )
        try:
            with open(patch_sql_file, "r") as f:
                patch_sql = f.read()
        except FileNotFoundError as e:
            patch_sql = str(e)
        try:
            with open(revert_sql_file, "r") as f:
                revert_sql = f.read()
        except FileNotFoundError as e:
            revert_sql = str(e)
        result["data"] = {
            "diff_stdout": diff_stdout,
            "patch_stdout": patch_sql,
            "revert_stdout": revert_sql,
        }
    else:
        result["data"] = {
            "diff_stdout": diff_stdout,
            "patch_stdout": "",
            "revert_stdout": "",
        }

    return HttpResponse(json.dumps(result), content_type="application/json")


@cache_page(60 * 5, key_prefix="insRes")
def instance_resource(request):
    """
    Get instance resources: database, schema, table, column.
    :param request:
    :return:
    """
    instance_id = request.GET.get("instance_id")
    instance_name = request.GET.get("instance_name")
    db_name = request.GET.get("db_name", "")
    schema_name = request.GET.get("schema_name", "")
    tb_name = request.GET.get("tb_name", "")

    resource_type = request.GET.get("resource_type")
    if instance_id:
        instance = Instance.objects.get(id=instance_id)
    else:
        try:
            instance = Instance.objects.get(instance_name=instance_name)
        except Instance.DoesNotExist:
            result = {"status": 1, "msg": "Instance does not exist", "data": []}
            return HttpResponse(json.dumps(result), content_type="application/json")
    result = {"status": 0, "msg": "ok", "data": []}

    try:
        query_engine = get_engine(instance=instance)
        db_name = query_engine.escape_string(db_name)
        schema_name = query_engine.escape_string(schema_name)
        tb_name = query_engine.escape_string(tb_name)
        if resource_type == "database":
            resource = query_engine.get_all_databases()
            resource.rows = filter_db_list(
                db_list=resource.rows,
                db_name_regex=query_engine.instance.show_db_name_regex,
                is_match_regex=True,
            )
            resource.rows = filter_db_list(
                db_list=resource.rows,
                db_name_regex=query_engine.instance.denied_db_name_regex,
                is_match_regex=False,
            )
        elif resource_type == "schema" and db_name:
            resource = query_engine.get_all_schemas(db_name=db_name)
        elif resource_type == "table" and db_name:
            resource = query_engine.get_all_tables(
                db_name=db_name, schema_name=schema_name
            )
        elif resource_type == "column" and db_name and tb_name:
            resource = query_engine.get_all_columns_by_tb(
                db_name=db_name, tb_name=tb_name, schema_name=schema_name
            )
        else:
            raise TypeError("Unsupported resource type or incomplete parameters!")
    except Exception as msg:
        result["status"] = 1
        result["msg"] = str(msg)
    else:
        if resource.error:
            result["status"] = 1
            result["msg"] = resource.error
        else:
            result["data"] = resource.rows
    return HttpResponse(json.dumps(result), content_type="application/json")


def describe(request):
    """Get table structure."""
    instance_name = request.POST.get("instance_name")
    try:
        instance = Instance.objects.get(instance_name=instance_name)
    except Instance.DoesNotExist:
        result = {"status": 1, "msg": "Instance does not exist", "data": []}
        return HttpResponse(json.dumps(result), content_type="application/json")
    db_name = request.POST.get("db_name")
    schema_name = request.POST.get("schema_name")
    tb_name = request.POST.get("tb_name")

    result = {"status": 0, "msg": "ok", "data": []}

    try:
        query_engine = get_engine(instance=instance)
        db_name = query_engine.escape_string(db_name)
        schema_name = query_engine.escape_string(schema_name)
        tb_name = query_engine.escape_string(tb_name)
        query_result = query_engine.describe_table(
            db_name, tb_name, schema_name=schema_name
        )
        result["data"] = query_result.__dict__
    except Exception as msg:
        result["status"] = 1
        result["msg"] = str(msg)
    if result["data"].get("error"):
        result["status"] = 1
        result["msg"] = result["data"]["error"]
    return HttpResponse(json.dumps(result), content_type="application/json")
