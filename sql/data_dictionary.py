# -*- coding: UTF-8 -*-
import datetime
import os
from urllib.parse import quote

import MySQLdb
import simplejson as json
from django.template import loader
from django.conf import settings
from sql.engines import get_engine
from django.contrib.auth.decorators import permission_required
from django.http import HttpResponse, JsonResponse, FileResponse

from common.utils.extend_json_encoder import ExtendJSONEncoder
from sql.utils.resource_group import user_instances
from .models import Instance


@permission_required("sql.menu_data_dictionary", raise_exception=True)
def table_list(request):
    """Get table list from the data dictionary."""
    instance_name = request.GET.get("instance_name", "")
    db_name = request.GET.get("db_name", "")
    db_type = request.GET.get("db_type", "")

    if instance_name and db_name:
        try:
            instance = Instance.objects.get(
                instance_name=instance_name, db_type=db_type
            )
            query_engine = get_engine(instance=instance)
            db_name = query_engine.escape_string(db_name)
            data = query_engine.get_group_tables_by_db(db_name=db_name)
            res = {"status": 0, "data": data}
        except Instance.DoesNotExist:
            res = {"status": 1, "msg": "Instance.DoesNotExist"}
        except Exception as e:
            res = {"status": 1, "msg": str(e)}
    else:
        res = {"status": 1, "msg": "Invalid request!"}
    return HttpResponse(
        json.dumps(res, cls=ExtendJSONEncoder, bigint_as_string=True),
        content_type="application/json",
    )


@permission_required("sql.menu_data_dictionary", raise_exception=True)
def table_info(request):
    """Get table details from the data dictionary."""
    instance_name = request.GET.get("instance_name", "")
    db_name = request.GET.get("db_name", "")
    tb_name = request.GET.get("tb_name", "")
    db_type = request.GET.get("db_type", "")

    if instance_name and db_name and tb_name:
        data = {}
        try:
            instance = Instance.objects.get(
                instance_name=instance_name, db_type=db_type
            )
            query_engine = get_engine(instance=instance)
            db_name = query_engine.escape_string(db_name)
            tb_name = query_engine.escape_string(tb_name)
            data["meta_data"] = query_engine.get_table_meta_data(
                db_name=db_name, tb_name=tb_name
            )
            data["desc"] = query_engine.get_table_desc_data(
                db_name=db_name, tb_name=tb_name
            )
            data["index"] = query_engine.get_table_index_data(
                db_name=db_name, tb_name=tb_name
            )

            # MySQL can provide CREATE TABLE SQL; this is unavailable for MSSQL here.
            if instance.db_type == "mysql":
                _create_sql = query_engine.query(
                    db_name, "show create table `%s`;" % tb_name
                )
                data["create_sql"] = _create_sql.rows
            res = {"status": 0, "data": data}
        except Instance.DoesNotExist:
            res = {"status": 1, "msg": "Instance.DoesNotExist"}
        except Exception as e:
            res = {"status": 1, "msg": str(e)}
    else:
        res = {"status": 1, "msg": "Invalid request!"}
    return HttpResponse(
        json.dumps(res, cls=ExtendJSONEncoder, bigint_as_string=True),
        content_type="application/json",
    )


def get_export_full_path(base_dir: str, instance_name: str, db_name: str) -> str:
    """validate if the instance_name and db_name provided is secure"""
    fullpath = os.path.normpath(
        os.path.join(base_dir, f"{instance_name}_{db_name}.html")
    )
    if not fullpath.startswith(base_dir):
        return ""
    return fullpath


@permission_required("sql.data_dictionary_export", raise_exception=True)
def export(request):
    """Export data dictionary."""
    instance_name = request.GET.get("instance_name", "")
    db_name = request.GET.get("db_name", "")

    try:
        instance = user_instances(
            request.user, db_type=["mysql", "mssql", "oracle"]
        ).get(instance_name=instance_name)
        query_engine = get_engine(instance=instance)
    except Instance.DoesNotExist:
        return JsonResponse(
            {
                "status": 1,
                "msg": "Your group is not associated with this instance.",
                "data": [],
            }
        )

    # Regular users can only export dictionary data for a specific database.
    if db_name:
        dbs = [query_engine.escape_string(db_name)]
    # Admins can export dictionary data for the entire instance.
    elif request.user.is_superuser:
        dbs = query_engine.get_all_databases().rows
    else:
        return JsonResponse(
            {
                "status": 1,
                "msg": "Only admins can export dictionary data for the full instance!",
                "data": [],
            }
        )

    # Get data and write to output directory.
    path = os.path.join(settings.BASE_DIR, "downloads", "dictionary")
    os.makedirs(path, exist_ok=True)
    for db in dbs:
        table_metas = query_engine.get_tables_metas_data(db_name=db)
        context = {
            "db_name": db_name,
            "tables": table_metas,
            "export_time": datetime.datetime.now(),
        }
        data = loader.render_to_string(
            template_name="dictionaryexport.html", context=context, request=request
        )
        fullpath = get_export_full_path(path, instance_name, db)
        if not fullpath:
            return JsonResponse(
                {"status": 1, "msg": "Invalid instance name or db name", "data": []}
            )
        with open(fullpath, "w", encoding="utf-8") as fp:
            fp.write(data)
    # Close connection.
    query_engine.close()
    if db_name:
        fullpath = get_export_full_path(path, instance_name, db)
        if not fullpath:
            return JsonResponse(
                {"status": 1, "msg": "Invalid instance name or db name", "data": []}
            )
        response = FileResponse(open(fullpath, "rb"))
        response["Content-Type"] = "application/octet-stream"
        response["Content-Disposition"] = (
            f'attachment;filename="{quote(instance_name)}_{quote(db_name)}.html"'
        )
        return response

    else:
        return JsonResponse(
            {
                "status": 0,
                "msg": (
                    f"Data dictionary export for instance {instance_name} succeeded. "
                    "Please download it from the downloads directory."
                ),
                "data": [],
            }
        )
