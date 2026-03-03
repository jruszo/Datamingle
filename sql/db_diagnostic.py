import logging
import traceback
import MySQLdb

# import simplejson as json
import json
from django.contrib.auth.decorators import permission_required

from django.http import HttpResponse

from sql.engines import get_engine
from common.utils.extend_json_encoder import ExtendJSONEncoder, ExtendJSONEncoderBytes
from sql.utils.resource_group import user_instances
from .models import Instance

logger = logging.getLogger("default")


# Diagnostics -- process list
@permission_required("sql.process_view", raise_exception=True)
def process(request):
    instance_name = request.POST.get("instance_name")
    command_type = request.POST.get("command_type")
    request_kwargs = {
        key: value for key, value in request.POST.items() if key != "command_type"
    }

    try:
        instance = user_instances(request.user).get(instance_name=instance_name)
    except Instance.DoesNotExist:
        result = {
            "status": 1,
            "msg": "Your group is not associated with this instance",
            "data": [],
        }
        return HttpResponse(json.dumps(result), content_type="application/json")

    query_engine = get_engine(instance=instance)
    query_result = None
    # processlist is handled in the base engine class to simplify this logic.
    # When adding a new DB type for process view, update frontend only.
    query_result = query_engine.processlist(command_type=command_type, **request_kwargs)
    if query_result:
        if not query_result.error:
            processlist = query_result.to_dict()
            result = {"status": 0, "msg": "ok", "rows": processlist}
        else:
            result = {"status": 1, "msg": query_result.error}

    # Return query result.
    # ExtendJSONEncoderBytes uses json module.
    # bigint_as_string is only supported by simplejson.
    return HttpResponse(
        json.dumps(result, cls=ExtendJSONEncoderBytes), content_type="application/json"
    )


# Diagnostics -- build request by thread ID.
# This only verifies that the target thread ID is still running.
@permission_required("sql.process_kill", raise_exception=True)
def create_kill_session(request):
    instance_name = request.POST.get("instance_name")
    thread_ids = request.POST.get("ThreadIDs")

    try:
        instance = user_instances(request.user).get(instance_name=instance_name)
    except Instance.DoesNotExist:
        result = {
            "status": 1,
            "msg": "Your group is not associated with this instance",
            "data": [],
        }
        return HttpResponse(json.dumps(result), content_type="application/json")

    result = {"status": 0, "msg": "ok", "data": []}
    query_engine = get_engine(instance=instance)
    try:
        result["data"] = query_engine.get_kill_command(json.loads(thread_ids))
    except AttributeError:
        result = {
            "status": 1,
            "msg": "{} database type is not currently supported for request "
            "generation by process ID".format(instance.db_type),
            "data": [],
        }
        return HttpResponse(json.dumps(result), content_type="application/json")
    # Return query result.
    return HttpResponse(
        json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
        content_type="application/json",
    )


# Diagnostics -- terminate session (actual kill operation)
@permission_required("sql.process_kill", raise_exception=True)
def kill_session(request):
    instance_name = request.POST.get("instance_name")
    thread_ids = request.POST.get("ThreadIDs")
    result = {"status": 0, "msg": "ok", "data": []}

    try:
        instance = user_instances(request.user).get(instance_name=instance_name)
    except Instance.DoesNotExist:
        result = {
            "status": 1,
            "msg": "Your group is not associated with this instance",
            "data": [],
        }
        return HttpResponse(json.dumps(result), content_type="application/json")

    engine = get_engine(instance=instance)
    r = None
    if instance.db_type in ["mysql", "doris"]:
        r = engine.kill(json.loads(thread_ids))
    elif instance.db_type == "mongo":
        r = engine.kill_op(json.loads(thread_ids))
    elif instance.db_type == "oracle":
        r = engine.kill_session(json.loads(thread_ids))
    else:
        result = {
            "status": 1,
            "msg": "{} database type is not currently supported for session "
            "termination".format(instance.db_type),
            "data": [],
        }
        return HttpResponse(json.dumps(result), content_type="application/json")

    if r and r.error:
        result = {"status": 1, "msg": r.error, "data": []}
    # Return query result.
    return HttpResponse(
        json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
        content_type="application/json",
    )


# Diagnostics -- tablespace information
@permission_required("sql.tablespace_view", raise_exception=True)
def tablespace(request):
    instance_name = request.POST.get("instance_name")
    offset = int(request.POST.get("offset", 0))
    limit = int(request.POST.get("limit", 14))
    try:
        instance = user_instances(request.user).get(instance_name=instance_name)
    except Instance.DoesNotExist:
        result = {
            "status": 1,
            "msg": "Your group is not associated with this instance",
            "data": [],
        }
        return HttpResponse(json.dumps(result), content_type="application/json")

    query_engine = get_engine(instance=instance)
    try:
        query_result = query_engine.tablespace(offset, limit)
    except AttributeError:
        result = {
            "status": 1,
            "msg": "{} database type is not currently supported for tablespace "
            "queries".format(instance.db_type),
            "data": [],
        }
        return HttpResponse(json.dumps(result), content_type="application/json")

    if query_result:
        if not query_result.error:
            table_space = query_result.to_dict()
            r = query_engine.tablespace_count()
            total = r.rows[0][0]
            result = {"status": 0, "msg": "ok", "rows": table_space, "total": total}
        else:
            result = {"status": 1, "msg": query_result.error}
    # Return query result.
    return HttpResponse(
        json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
        content_type="application/json",
    )


# Diagnostics -- lock waits
@permission_required("sql.trxandlocks_view", raise_exception=True)
def trxandlocks(request):
    instance_name = request.POST.get("instance_name")

    try:
        instance = user_instances(request.user).get(instance_name=instance_name)
    except Instance.DoesNotExist:
        result = {
            "status": 1,
            "msg": "Your group is not associated with this instance",
            "data": [],
        }
        return HttpResponse(json.dumps(result), content_type="application/json")

    query_engine = get_engine(instance=instance)
    if instance.db_type == "mysql":
        query_result = query_engine.trxandlocks()
    elif instance.db_type == "oracle":
        query_result = query_engine.lock_info()
    else:
        result = {
            "status": 1,
            "msg": "{} database type is not currently supported for lock wait "
            "queries".format(instance.db_type),
            "data": [],
        }
        return HttpResponse(json.dumps(result), content_type="application/json")

    if not query_result.error:
        trxandlocks = query_result.to_dict()
        result = {"status": 0, "msg": "ok", "rows": trxandlocks}
    else:
        result = {"status": 1, "msg": query_result.error}

    # Return query result.
    return HttpResponse(
        json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
        content_type="application/json",
    )


# Diagnostics -- long transactions
@permission_required("sql.trx_view", raise_exception=True)
def innodb_trx(request):
    instance_name = request.POST.get("instance_name")

    try:
        instance = user_instances(request.user).get(instance_name=instance_name)
    except Instance.DoesNotExist:
        result = {
            "status": 1,
            "msg": "Your group is not associated with this instance",
            "data": [],
        }
        return HttpResponse(json.dumps(result), content_type="application/json")

    query_engine = get_engine(instance=instance)
    try:
        query_result = query_engine.get_long_transaction()
    except AttributeError:
        result = {
            "status": 1,
            "msg": "{} database type is not currently supported for long "
            "transaction queries".format(instance.db_type),
            "data": [],
        }
        return HttpResponse(json.dumps(result), content_type="application/json")

    if not query_result.error:
        trx = query_result.to_dict()
        result = {"status": 0, "msg": "ok", "rows": trx}
    else:
        result = {"status": 1, "msg": query_result.error}

    # Return query result.
    return HttpResponse(
        json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
        content_type="application/json",
    )
