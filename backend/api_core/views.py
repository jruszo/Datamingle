# -*- coding: UTF-8 -*-
import platform
import sys
import MySQLdb
from importlib import metadata

from common.config import SysConfig
from common.task_queue import task_backend_info
from django.db import connection
from django_redis import get_redis_connection
from django.http import JsonResponse

from common.utils.permission import superuser_required
import archery


def info(request):
    system_info = {
        "archery": {"version": archery.display_version},
        "task_backend": task_backend_info(full=False),
    }
    return JsonResponse(system_info)


@superuser_required
def debug(request):
    # Return full details when requested.
    full = request.GET.get("full")

    # System configuration.
    config = SysConfig()
    config.get_all_config()
    sys_config = config.sys_config

    # MySQL information.
    cursor = connection.cursor()
    mysql_info = {
        "mysql_server_info": cursor.db.mysql_server_info,
        "timezone_name": cursor.db.timezone_name,
    }

    # Redis information.
    try:
        redis_conn = get_redis_connection("default")
        full_redis_info = redis_conn.info()
        redis_info = {
            "redis_version": full_redis_info.get("redis_version"),
            "redis_mode": full_redis_info.get("redis_mode"),
            "role": full_redis_info.get("role"),
            "maxmemory_human": full_redis_info.get("maxmemory_human"),
            "used_memory_human": full_redis_info.get("used_memory_human"),
        }
    except Exception:
        redis_info = "Failed to get Redis info."
        full_redis_info = redis_info

    task_backend = task_backend_info(full=bool(full))

    # goInception information.
    go_inception_host = sys_config.get("go_inception_host")
    go_inception_port = sys_config.get("go_inception_port", 0)
    go_inception_user = sys_config.get("go_inception_user", "")
    go_inception_password = sys_config.get("go_inception_password", "")

    # goInception
    try:
        goinc_conn = MySQLdb.connect(
            host=go_inception_host,
            port=int(go_inception_port),
            user=go_inception_user,
            passwd=go_inception_password,
            connect_timeout=1,
            cursorclass=MySQLdb.cursors.DictCursor,
        )
        cursor = goinc_conn.cursor()
        cursor.execute("inception get variables")
        rows = cursor.fetchall()
        full_goinception_info = dict()
        for row in rows:
            full_goinception_info[row.get("Variable_name")] = row.get("Value")
        goinception_info = {
            "version": full_goinception_info.get("version"),
            "max_allowed_packet": full_goinception_info.get("max_allowed_packet"),
            "lang": full_goinception_info.get("lang"),
            "osc_on": full_goinception_info.get("osc_on"),
            "osc_bin_dir": full_goinception_info.get("osc_bin_dir"),
            "ghost_on": full_goinception_info.get("ghost_on"),
        }
    except Exception:
        goinception_info = "Failed to get goInception info."
        full_goinception_info = goinception_info

    # PACKAGES
    installed_packages_list = sorted(
        f"{dist.metadata['Name']}=={dist.version}"
        for dist in metadata.distributions()
        if dist.metadata["Name"]
    )

    # Mask sensitive information.
    secret_keys = [
        "feishu_app_secret",
        "mail_smtp_password",
        "go_inception_password",
        "wx_app_secret",
        "aliyun_access_key_secret",
        "tencent_secret_key",
        "celery_broker_url",
        "celery_result_backend",
    ]
    sys_config.update({k: "******" for k in secret_keys})
    for key in (
        "inception_remote_backup_host",
        "inception_remote_backup_port",
        "inception_remote_backup_user",
        "inception_remote_backup_password",
        "enable_backup_switch",
    ):
        sys_config.pop(key, None)

    # Final output.
    system_info = {
        "archery": {"version": archery.display_version},
        "task_backend": task_backend,
        "inception": {
            "goinception_info": full_goinception_info if full else goinception_info,
        },
        "runtime_info": {
            "python_version": platform.python_version(),
            "mysql_info": mysql_info,
            "redis_info": full_redis_info if full else redis_info,
            "sys_argv": sys.argv,
            "platform": platform.uname(),
        },
        "sys_config": sys_config,
        "packages": installed_packages_list,
    }
    return JsonResponse(system_info)
