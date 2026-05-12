# -*- coding: UTF-8 -*-
"""
@author: hhyo
@license: Apache Licence
@file: instance_database.py
@time: 2019/09/19
"""

import MySQLdb

import simplejson as json
from django.contrib.auth.decorators import permission_required
from django.http import JsonResponse, HttpResponse
from django_redis import get_redis_connection

from common.utils.extend_json_encoder import ExtendJSONEncoder
from sql.engines import get_engine, ResultSet
from sql.models import Instance, InstanceDatabase, Users
from sql.utils.resource_group import user_instances

__author__ = "hhyo"


@permission_required("sql.menu_database", raise_exception=True)
def databases(request):
    """Get database list for an instance."""
    instance_id = request.POST.get("instance_id")
    saved = True if request.POST.get("saved") == "true" else False  # Saved in Archery

    if not instance_id:
        return JsonResponse({"status": 0, "msg": "", "data": []})

    try:
        instance = user_instances(request.user, db_type=["mysql", "mongo"]).get(
            id=instance_id
        )
    except Instance.DoesNotExist:
        return JsonResponse(
            {
                "status": 1,
                "msg": "Your group is not associated with this instance",
                "data": [],
            }
        )

    # Get already configured databases.
    cnf_dbs = dict()
    for db in InstanceDatabase.objects.filter(instance=instance).values(
        "id", "db_name", "owner", "owner_display", "remark"
    ):
        db["saved"] = True
        cnf_dbs[f"{db['db_name']}"] = db

    query_engine = get_engine(instance=instance)
    query_result = query_engine.get_all_databases_summary()
    if not query_result.error:
        # Merge configured owner information into database list.
        rows = []
        for row in query_result.rows:
            if row["db_name"] in cnf_dbs.keys():
                row = dict(row, **cnf_dbs[row["db_name"]])
            rows.append(row)
        if saved:
            rows = [row for row in rows if row["saved"]]
        result = {"status": 0, "msg": "ok", "rows": rows}
    else:
        result = {"status": 1, "msg": query_result.error}

    # Close connection.
    query_engine.close()
    return HttpResponse(
        json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
        content_type="application/json",
    )


@permission_required("sql.menu_database", raise_exception=True)
def create(request):
    """Create database."""
    instance_id = request.POST.get("instance_id", 0)
    db_name = request.POST.get("db_name")
    owner = request.POST.get("owner", "")
    remark = request.POST.get("remark", "")

    if not all([db_name]):
        return JsonResponse(
            {
                "status": 1,
                "msg": "Incomplete parameters, please verify and submit",
                "data": [],
            }
        )

    try:
        instance = user_instances(request.user, db_type=["mysql", "mongo"]).get(
            id=instance_id
        )
    except Instance.DoesNotExist:
        return JsonResponse(
            {
                "status": 1,
                "msg": "Your group is not associated with this instance",
                "data": [],
            }
        )

    try:
        owner_display = Users.objects.get(username=owner).display
    except Users.DoesNotExist:
        return JsonResponse({"status": 1, "msg": "Owner does not exist", "data": []})

    engine = get_engine(instance=instance)
    if instance.db_type == "mysql":
        # escape
        db_name = engine.escape_string(db_name)
        exec_result = engine.execute(
            db_name="information_schema", sql=f"create database {db_name};"
        )
    elif instance.db_type == "mongo":
        exec_result = ResultSet()
        try:
            conn = engine.get_connection()
            db = conn[db_name]
            db.create_collection(
                name=f"archery-{db_name}"
            )  # Mongo creates a visible DB only after data exists; create a helper collection.
        except Exception as e:
            exec_result.error = f"Failed to create database, error: {str(e)}"

    # Close connection.
    engine.close()
    if exec_result.error:
        return JsonResponse({"status": 1, "msg": exec_result.error})
    # Save to metadata table.
    else:
        InstanceDatabase.objects.create(
            instance=instance,
            db_name=db_name,
            owner=owner,
            owner_display=owner_display,
            remark=remark,
        )
        # Clear instance-resource cache.
        r = get_redis_connection("default")
        for key in r.scan_iter(match="*insRes*", count=2000):
            r.delete(key)

    return JsonResponse({"status": 0, "msg": "", "data": []})


@permission_required("sql.menu_database", raise_exception=True)
def edit(request):
    """Edit or register database metadata."""
    instance_id = request.POST.get("instance_id", 0)
    db_name = request.POST.get("db_name")
    owner = request.POST.get("owner", "")
    remark = request.POST.get("remark", "")

    if not all([db_name]):
        return JsonResponse(
            {
                "status": 1,
                "msg": "Incomplete parameters, please verify and submit",
                "data": [],
            }
        )

    try:
        instance = user_instances(request.user, db_type=["mysql", "mongo"]).get(
            id=instance_id
        )
    except Instance.DoesNotExist:
        return JsonResponse(
            {
                "status": 1,
                "msg": "Your group is not associated with this instance",
                "data": [],
            }
        )

    try:
        owner_display = Users.objects.get(username=owner).display
    except Users.DoesNotExist:
        return JsonResponse({"status": 1, "msg": "Owner does not exist", "data": []})

    # Update or insert metadata.
    InstanceDatabase.objects.update_or_create(
        instance=instance,
        db_name=db_name,
        defaults={"owner": owner, "owner_display": owner_display, "remark": remark},
    )
    return JsonResponse({"status": 0, "msg": "", "data": []})
