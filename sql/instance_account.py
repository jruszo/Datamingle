# -*- coding: UTF-8 -*-
import MySQLdb
import simplejson as json
from django.contrib.auth.decorators import permission_required
from django.contrib.auth.password_validation import validate_password
from django.core.exceptions import ValidationError

from django.http import HttpResponse, JsonResponse
from sql.utils.instance_management import (
    SUPPORTED_MANAGEMENT_DB_TYPE,
    get_instanceaccount_unique_value,
    get_instanceaccount_unique_key,
)
from common.utils.extend_json_encoder import ExtendJSONEncoder
from sql.engines import get_engine, ResultSet
from sql.utils.resource_group import user_instances
from .models import Instance, InstanceAccount


@permission_required("sql.menu_instance_account", raise_exception=True)
def users(request):
    """Get instance account list."""
    instance_id = request.POST.get("instance_id")
    saved = True if request.POST.get("saved") == "true" else False  # Saved in Archery

    if not instance_id:
        return JsonResponse({"status": 0, "msg": "", "data": []})
    try:
        instance = user_instances(
            request.user, db_type=SUPPORTED_MANAGEMENT_DB_TYPE
        ).get(id=instance_id)
    except Instance.DoesNotExist:
        return JsonResponse(
            {
                "status": 1,
                "msg": "Your group is not associated with this instance",
                "data": [],
            }
        )

    # Get configured users.
    cnf_users = dict()
    for user in InstanceAccount.objects.filter(instance=instance).values(
        "id", "user", "host", "db_name", "remark"
    ):
        user["saved"] = True
        cnf_users[get_instanceaccount_unique_value(instance.db_type, user)] = user
    # Get all users.
    query_engine = get_engine(instance=instance)
    query_result = query_engine.get_instance_users_summary()
    if not query_result.error:
        rows = []
        key = get_instanceaccount_unique_key(db_type=instance.db_type)
        for row in query_result.rows:
            # Merge database result with configured metadata.
            if row[key] in cnf_users.keys():
                row = dict(row, **cnf_users[row[key]])
            rows.append(row)
        # Apply filters.
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


@permission_required("sql.instance_account_manage", raise_exception=True)
def create(request):
    """Create database account."""
    instance_id = request.POST.get("instance_id", 0)
    db_name = request.POST.get("db_name")
    user = request.POST.get("user")
    host = request.POST.get("host")
    password1 = request.POST.get("password1")
    password2 = request.POST.get("password2")
    remark = request.POST.get("remark", "")

    try:
        instance = user_instances(
            request.user, db_type=SUPPORTED_MANAGEMENT_DB_TYPE
        ).get(id=instance_id)
    except Instance.DoesNotExist:
        return JsonResponse(
            {
                "status": 1,
                "msg": "Your group is not associated with this instance",
                "data": [],
            }
        )

    if (
        instance.db_type == "mysql" and not all([user, host, password1, password2])
    ) or (
        instance.db_type == "mongo" and not all([db_name, user, password1, password2])
    ):
        return JsonResponse(
            {"status": 1, "msg": "Incomplete parameters, please verify and submit", "data": []}
        )

    if password1 != password2:
        return JsonResponse(
            {"status": 1, "msg": "The two passwords do not match", "data": []}
        )

    # TODO: currently use built-in validation; add custom validators later.
    try:
        validate_password(password1, user=None, password_validators=None)
    except ValidationError as msg:
        return JsonResponse({"status": 1, "msg": f"{msg}", "data": []})

    engine = get_engine(instance=instance)
    exec_result = engine.create_instance_user(
        db_name=db_name, user=user, host=host, password1=password1, remark=remark
    )
    # Close connection.
    engine.close()
    if exec_result.error:
        return JsonResponse({"status": 1, "msg": exec_result.error})

    # Save to metadata table.
    else:
        accounts = [InstanceAccount(**row) for row in exec_result.rows]
        InstanceAccount.objects.bulk_create(accounts)

    return JsonResponse({"status": 0, "msg": "", "data": []})


@permission_required("sql.instance_account_manage", raise_exception=True)
def edit(request):
    """Update or register database account."""
    instance_id = request.POST.get("instance_id", 0)
    db_name = request.POST.get("db_name", "")
    user = request.POST.get("user")
    host = request.POST.get("host", "")
    password = request.POST.get("password")
    remark = request.POST.get("remark", "")

    try:
        instance = user_instances(
            request.user, db_type=SUPPORTED_MANAGEMENT_DB_TYPE
        ).get(id=instance_id)
    except Instance.DoesNotExist:
        return JsonResponse(
            {
                "status": 1,
                "msg": "Your group is not associated with this instance",
                "data": [],
            }
        )

    if (instance.db_type == "mysql" and not all([user, host])) or (
        instance.db_type == "mongo" and not all([db_name, user])
    ):
        return JsonResponse(
            {"status": 1, "msg": "Incomplete parameters, please verify and submit", "data": []}
        )

    # Save to metadata table.
    if password:
        InstanceAccount.objects.update_or_create(
            instance=instance,
            user=user,
            host=host,
            db_name=db_name,
            defaults={"password": password, "remark": remark},
        )
    else:
        InstanceAccount.objects.update_or_create(
            instance=instance,
            user=user,
            host=host,
            db_name=db_name,
            defaults={"remark": remark},
        )
    return JsonResponse({"status": 0, "msg": "", "data": []})


@permission_required("sql.instance_account_manage", raise_exception=True)
def grant(request):
    """Build privilege-change SQL and execute account privilege changes."""
    instance_id = request.POST.get("instance_id", 0)
    grant_sql = ""

    try:
        instance = user_instances(
            request.user, db_type=SUPPORTED_MANAGEMENT_DB_TYPE
        ).get(id=instance_id)
    except Instance.DoesNotExist:
        return JsonResponse(
            {
                "status": 1,
                "msg": "Your group is not associated with this instance",
                "data": [],
            }
        )

    engine = get_engine(instance=instance)
    if instance.db_type == "mysql":
        user_host = request.POST.get("user_host")
        op_type = int(request.POST.get("op_type"))
        priv_type = int(request.POST.get("priv_type"))
        privs = json.loads(request.POST.get("privs"))

        # escape
        user_host = engine.escape_string(user_host)

        # Global privileges
        if priv_type == 0:
            global_privs = privs["global_privs"]
            if not all([global_privs]):
                return JsonResponse(
                    {"status": 1, "msg": "Incomplete information, please verify and submit", "data": []}
                )
            global_privs = ["GRANT OPTION" if g == "GRANT" else g for g in global_privs]
            if op_type == 0:
                grant_sql = f"GRANT {','.join(global_privs)} ON *.* TO {user_host};"
            elif op_type == 1:
                grant_sql = f"REVOKE {','.join(global_privs)} ON *.* FROM {user_host};"

        # Database privileges
        elif priv_type == 1:
            db_privs = privs["db_privs"]
            db_name = request.POST.getlist("db_name[]")
            if not all([db_privs, db_name]):
                return JsonResponse(
                    {"status": 1, "msg": "Incomplete information, please verify and submit", "data": []}
                )
            for db in db_name:
                db_privs = ["GRANT OPTION" if d == "GRANT" else d for d in db_privs]
                if op_type == 0:
                    grant_sql += (
                        f"GRANT {','.join(db_privs)} ON `{db}`.* TO {user_host};"
                    )
                elif op_type == 1:
                    grant_sql += (
                        f"REVOKE {','.join(db_privs)} ON `{db}`.* FROM {user_host};"
                    )
        # Table privileges
        elif priv_type == 2:
            tb_privs = privs["tb_privs"]
            db_name = request.POST.get("db_name")
            tb_name = request.POST.getlist("tb_name[]")
            if not all([tb_privs, db_name, tb_name]):
                return JsonResponse(
                    {"status": 1, "msg": "Incomplete information, please verify and submit", "data": []}
                )
            for tb in tb_name:
                tb_privs = ["GRANT OPTION" if t == "GRANT" else t for t in tb_privs]
                if op_type == 0:
                    grant_sql += f"GRANT {','.join(tb_privs)} ON `{db_name}`.`{tb}` TO {user_host};"
                elif op_type == 1:
                    grant_sql += f"REVOKE {','.join(tb_privs)} ON `{db_name}`.`{tb}` FROM {user_host};"
        # Column privileges
        elif priv_type == 3:
            col_privs = privs["col_privs"]
            db_name = request.POST.get("db_name")
            tb_name = request.POST.get("tb_name")
            col_name = request.POST.getlist("col_name[]")
            if not all([col_privs, db_name, tb_name, col_name]):
                return JsonResponse(
                    {"status": 1, "msg": "Incomplete information, please verify and submit", "data": []}
                )
            for priv in col_privs:
                if op_type == 0:
                    grant_sql += f"GRANT {priv}(`{'`,`'.join(col_name)}`) ON `{db_name}`.`{tb_name}` TO {user_host};"
                elif op_type == 1:
                    grant_sql += f"REVOKE {priv}(`{'`,`'.join(col_name)}`) ON `{db_name}`.`{tb_name}` FROM {user_host};"
        # Execute privilege-change SQL.
        exec_result = engine.execute(db_name="mysql", sql=grant_sql)
    elif instance.db_type == "mongo":
        db_name_user = request.POST.get("db_name_user")
        roles = request.POST.getlist("roles[]")
        arr = db_name_user.split(".")
        db_name = arr[0]
        user = arr[1]
        exec_result = ResultSet()
        try:
            conn = engine.get_connection()
            conn[db_name].command("updateUser", user, roles=roles)
        except Exception as e:
            exec_result.error = str(e)

    # Close connection.
    engine.close()
    if exec_result.error:
        return JsonResponse({"status": 1, "msg": exec_result.error})
    return JsonResponse({"status": 0, "msg": "", "data": grant_sql})


@permission_required("sql.instance_account_manage", raise_exception=True)
def reset_pwd(request):
    """Reset database account password."""
    instance_id = request.POST.get("instance_id", 0)
    db_name_user = request.POST.get("db_name_user")
    db_name = request.POST.get("db_name", "")
    user_host = request.POST.get("user_host")
    user = request.POST.get("user")
    host = request.POST.get("host", "")
    reset_pwd1 = request.POST.get("reset_pwd1")
    reset_pwd2 = request.POST.get("reset_pwd2")

    try:
        instance = user_instances(
            request.user, db_type=SUPPORTED_MANAGEMENT_DB_TYPE
        ).get(id=instance_id)
    except Instance.DoesNotExist:
        return JsonResponse(
            {
                "status": 1,
                "msg": "Your group is not associated with this instance",
                "data": [],
            }
        )

    if (
        instance.db_type == "mysql" and not all([user, host, reset_pwd1, reset_pwd2])
    ) or (
        instance.db_type == "mongo" and not all([db_name, user, reset_pwd1, reset_pwd2])
    ):
        return JsonResponse(
            {"status": 1, "msg": "Incomplete parameters, please verify and submit", "data": []}
        )

    if reset_pwd1 != reset_pwd2:
        return JsonResponse(
            {"status": 1, "msg": "The two passwords do not match", "data": []}
        )

    # TODO: currently use built-in validation; add custom validators later.
    try:
        validate_password(reset_pwd1, user=None, password_validators=None)
    except ValidationError as msg:
        return JsonResponse({"status": 1, "msg": f"{msg}", "data": []})

    engine = get_engine(instance=instance)
    exec_result = engine.reset_instance_user_pwd(
        user_host=user_host, db_name_user=db_name_user, reset_pwd=reset_pwd1
    )
    # Close connection.
    engine.close()
    if exec_result.error:
        result = {"status": 1, "msg": exec_result.error}
        return HttpResponse(json.dumps(result), content_type="application/json")
    # Save to metadata table.
    else:
        InstanceAccount.objects.update_or_create(
            instance=instance,
            user=user,
            host=host,
            db_name=db_name,
            defaults={"password": reset_pwd1},
        )

    return JsonResponse({"status": 0, "msg": "", "data": []})


@permission_required("sql.instance_account_manage", raise_exception=True)
def lock(request):
    """Lock or unlock account."""
    instance_id = request.POST.get("instance_id", 0)
    user_host = request.POST.get("user_host")
    is_locked = request.POST.get("is_locked")
    lock_sql = ""

    if not all([user_host]):
        return JsonResponse(
            {"status": 1, "msg": "Incomplete parameters, please verify and submit", "data": []}
        )

    try:
        instance = user_instances(request.user, db_type=["mysql"]).get(id=instance_id)
    except Instance.DoesNotExist:
        return JsonResponse(
            {
                "status": 1,
                "msg": "Your group is not associated with this instance",
                "data": [],
            }
        )

    # escape
    engine = get_engine(instance=instance)
    user_host = engine.escape_string(user_host)

    if is_locked == "N":
        lock_sql = f"ALTER USER {user_host} ACCOUNT LOCK;"
    elif is_locked == "Y":
        lock_sql = f"ALTER USER {user_host} ACCOUNT UNLOCK;"

    exec_result = engine.execute(db_name="mysql", sql=lock_sql)
    if exec_result.error:
        return JsonResponse({"status": 1, "msg": exec_result.error})
    return JsonResponse({"status": 0, "msg": "", "data": []})


@permission_required("sql.instance_account_manage", raise_exception=True)
def delete(request):
    """Delete account."""
    instance_id = request.POST.get("instance_id", 0)
    db_name_user = request.POST.get("db_name_user")
    db_name = request.POST.get("db_name")
    user_host = request.POST.get("user_host")
    user = request.POST.get("user")
    host = request.POST.get("host")

    try:
        instance = user_instances(
            request.user, db_type=SUPPORTED_MANAGEMENT_DB_TYPE
        ).get(id=instance_id)
    except Instance.DoesNotExist:
        return JsonResponse(
            {
                "status": 1,
                "msg": "Your group is not associated with this instance",
                "data": [],
            }
        )

    if (instance.db_type == "mysql" and not all([user_host])) or (
        instance.db_type == "mongo" and not all([db_name_user])
    ):
        return JsonResponse(
            {"status": 1, "msg": "Incomplete parameters, please verify and submit", "data": []}
        )

    engine = get_engine(instance=instance)
    exec_result = engine.drop_instance_user(
        user_host=user_host, db_name_user=db_name_user
    )
    # Close connection.
    engine.close()
    if exec_result.error:
        return JsonResponse({"status": 1, "msg": exec_result.error})
    # Delete related metadata record.
    else:
        InstanceAccount.objects.filter(
            instance=instance, user=user, host=host, db_name=db_name
        ).delete()

    return JsonResponse({"status": 0, "msg": "", "data": []})
