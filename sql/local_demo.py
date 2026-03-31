from collections import OrderedDict

from django.contrib.auth.models import Group, Permission
from django.db import transaction

from common.utils.const import WorkflowType
from sql.models import Instance, InstanceTag, ResourceGroup, Users, WorkflowAuditSetting

DEMO_APP_PASSWORD = "demo123"
DEMO_DB_PASSWORD = "demo123"

AUTH_GROUP_PERMISSION_CODES = OrderedDict(
    {
        "Default": [
            "menu_sqlworkflow",
            "menu_query",
            "menu_sqlquery",
            "menu_queryapplylist",
            "menu_document",
        ],
        "RD": [
            "menu_dashboard",
            "menu_sqlcheck",
            "menu_sqlworkflow",
            "menu_sqlanalyze",
            "menu_query",
            "menu_sqlquery",
            "menu_queryapplylist",
            "menu_sqloptimize",
            "menu_sqladvisor",
            "menu_slowquery",
            "menu_data_dictionary",
            "menu_tools",
            "menu_archive",
            "menu_document",
            "sql_submit",
            "sql_execute",
            "sql_analyze",
            "optimize_sqladvisor",
            "optimize_soar",
            "query_applypriv",
            "query_submit",
            "archive_apply",
        ],
        "DBA": [
            "menu_dashboard",
            "menu_sqlcheck",
            "menu_sqlworkflow",
            "menu_sqlanalyze",
            "menu_query",
            "menu_sqlquery",
            "menu_queryapplylist",
            "menu_sqloptimize",
            "menu_sqladvisor",
            "menu_slowquery",
            "menu_instance",
            "menu_instance_list",
            "menu_dbdiagnostic",
            "menu_database",
            "menu_instance_account",
            "menu_param",
            "menu_data_dictionary",
            "menu_tools",
            "menu_archive",
            "menu_my2sql",
            "menu_schemasync",
            "menu_system",
            "menu_document",
            "menu_openapi",
            "sql_submit",
            "sql_review",
            "sql_execute_for_resource_group",
            "sql_execute",
            "sql_analyze",
            "optimize_sqladvisor",
            "optimize_sqltuning",
            "optimize_soar",
            "query_applypriv",
            "query_mgtpriv",
            "query_review",
            "query_submit",
            "query_all_instances",
            "query_resource_group_instance",
            "process_view",
            "process_kill",
            "tablespace_view",
            "trx_view",
            "trxandlocks_view",
            "instance_account_manage",
            "param_view",
            "param_edit",
            "data_dictionary_export",
            "archive_apply",
            "archive_review",
            "archive_mgt",
        ],
        "PM": [
            "menu_dashboard",
            "menu_sqlcheck",
            "menu_sqlworkflow",
            "menu_sqlanalyze",
            "menu_query",
            "menu_sqlquery",
            "menu_queryapplylist",
            "menu_sqloptimize",
            "menu_sqladvisor",
            "menu_slowquery",
            "menu_data_dictionary",
            "menu_tools",
            "menu_archive",
            "menu_document",
            "sql_submit",
            "sql_review",
            "sql_execute_for_resource_group",
            "sql_execute",
            "sql_analyze",
            "optimize_sqladvisor",
            "optimize_soar",
            "query_applypriv",
            "query_review",
            "query_submit",
            "archive_apply",
            "archive_review",
        ],
        "QA": [
            "menu_dashboard",
            "menu_sqlcheck",
            "menu_sqlworkflow",
            "menu_query",
            "menu_sqlquery",
            "menu_queryapplylist",
            "menu_data_dictionary",
            "menu_document",
            "sql_submit",
            "sql_execute",
            "query_applypriv",
            "query_submit",
        ],
    }
)

DEMO_RESOURCE_GROUPS = OrderedDict(
    {
        "single_stage": {
            "group_name": "Demo Workflow Single Stage",
            "approval_groups": ["DBA"],
        },
        "multi_stage": {
            "group_name": "Demo Workflow Multi Stage",
            "approval_groups": ["PM", "DBA"],
        },
    }
)

DEMO_USERS = OrderedDict(
    {
        "demo_admin": {
            "display": "Demo Admin",
            "is_superuser": True,
            "is_staff": True,
            "auth_groups": [],
            "resource_groups": [],
            "role_summary": "Full-access local admin",
        },
        "demo_requester": {
            "display": "Demo Requester",
            "is_superuser": False,
            "is_staff": False,
            "auth_groups": ["RD"],
            "resource_groups": ["single_stage", "multi_stage"],
            "role_summary": "Primary workflow submitter for manual UX checks",
        },
        "demo_pm": {
            "display": "Demo PM Reviewer",
            "is_superuser": False,
            "is_staff": False,
            "auth_groups": ["PM"],
            "resource_groups": ["multi_stage"],
            "role_summary": "First-stage reviewer for multi-stage approval flow",
        },
        "demo_dba": {
            "display": "Demo DBA",
            "is_superuser": False,
            "is_staff": False,
            "auth_groups": ["DBA"],
            "resource_groups": ["single_stage", "multi_stage"],
            "role_summary": "Single-stage reviewer and final approver/executor",
        },
    }
)

DEMO_INSTANCES = OrderedDict(
    {
        "mysql": {
            "instance_name": "demo-mysql-workflow",
            "type": "master",
            "db_type": "mysql",
            "host": "mysql_demo",
            "port": 3306,
            "user": "demo_archery",
            "password": DEMO_DB_PASSWORD,
            "db_name": "",
            "charset": "utf8mb4",
            "show_db_name_regex": "^(demo_orders|demo_billing)$",
            "denied_db_name_regex": "",
            "resource_groups": ["single_stage", "multi_stage"],
            "tags": ["can_read", "can_write"],
            "databases": ["demo_orders", "demo_billing"],
        },
        "pgsql": {
            "instance_name": "demo-pgsql-workflow",
            "type": "master",
            "db_type": "pgsql",
            "host": "postgres_demo",
            "port": 5432,
            "user": "demo_archery",
            "password": DEMO_DB_PASSWORD,
            "db_name": "workflow_pg",
            "charset": "UTF8",
            "show_db_name_regex": "^(workflow_pg|analytics_pg)$",
            "denied_db_name_regex": "",
            "resource_groups": ["single_stage", "multi_stage"],
            "tags": ["can_read", "can_write"],
            "databases": ["workflow_pg", "analytics_pg"],
        },
    }
)


def managed_demo_usernames():
    return list(DEMO_USERS.keys())


def managed_demo_instance_names():
    return [item["instance_name"] for item in DEMO_INSTANCES.values()]


def managed_demo_resource_group_names():
    return [item["group_name"] for item in DEMO_RESOURCE_GROUPS.values()]


def seed_local_demo(write_line=None):
    def log(message):
        if write_line:
            write_line(message)

    with transaction.atomic():
        auth_groups = _seed_auth_groups(log)
        resource_groups = _seed_resource_groups(log)
        tags = _seed_instance_tags(log)
        users = _seed_users(auth_groups, resource_groups, log)
        instances = _seed_instances(resource_groups, tags, log)
        _seed_workflow_settings(auth_groups, resource_groups, log)

    return {
        "auth_groups": list(auth_groups.keys()),
        "resource_groups": [group.group_name for group in resource_groups.values()],
        "users": list(users.keys()),
        "instances": [instance.instance_name for instance in instances.values()],
    }


def _seed_auth_groups(log):
    auth_groups = {}
    for name, permission_codes in AUTH_GROUP_PERMISSION_CODES.items():
        group, created = Group.objects.get_or_create(name=name)
        permissions = list(Permission.objects.filter(codename__in=permission_codes))
        group.permissions.set(permissions)
        auth_groups[name] = group
        missing_permissions = sorted(
            set(permission_codes) - {permission.codename for permission in permissions}
        )
        state = "created" if created else "updated"
        log(f"Auth group {state}: {name}")
        if missing_permissions:
            log(
                "Missing permissions for {}: {}".format(
                    name, ", ".join(missing_permissions)
                )
            )
    return auth_groups


def _seed_resource_groups(log):
    resource_groups = {}
    for index, (key, config) in enumerate(DEMO_RESOURCE_GROUPS.items(), start=1):
        resource_group, created = ResourceGroup.objects.update_or_create(
            group_name=config["group_name"],
            defaults={
                "group_parent_id": 0,
                "group_sort": index,
                "group_level": 1,
                "is_deleted": 0,
                "ding_webhook": "",
                "feishu_webhook": "",
                "qywx_webhook": "",
            },
        )
        resource_groups[key] = resource_group
        log(
            "Resource group {}: {}".format(
                "created" if created else "updated", resource_group.group_name
            )
        )
    return resource_groups


def _seed_instance_tags(log):
    tags = {}
    for tag_code, tag_name in [
        ("can_write", "Supports release"),
        ("can_read", "Supports query"),
    ]:
        tag, created = InstanceTag.objects.update_or_create(
            tag_code=tag_code,
            defaults={"tag_name": tag_name, "active": True},
        )
        tags[tag_code] = tag
        log("Instance tag {}: {}".format("created" if created else "updated", tag_code))
    return tags


def _seed_users(auth_groups, resource_groups, log):
    users = {}
    for username, config in DEMO_USERS.items():
        user, created = Users.objects.get_or_create(username=username)
        user.display = config["display"]
        user.email = ""
        user.is_active = True
        user.is_staff = config["is_staff"]
        user.is_superuser = config["is_superuser"]
        user.set_password(DEMO_APP_PASSWORD)
        user.save()
        user.groups.set([auth_groups[name] for name in config["auth_groups"]])
        user.resource_group.set(
            [resource_groups[name] for name in config["resource_groups"]]
        )
        users[username] = user
        log("Demo user {}: {}".format("created" if created else "updated", username))
    return users


def _seed_instances(resource_groups, tags, log):
    instances = {}
    for key, config in DEMO_INSTANCES.items():
        instance, created = Instance.objects.update_or_create(
            instance_name=config["instance_name"],
            defaults={
                "type": config["type"],
                "db_type": config["db_type"],
                "host": config["host"],
                "port": config["port"],
                "user": config["user"],
                "password": config["password"],
                "db_name": config["db_name"],
                "charset": config["charset"],
                "show_db_name_regex": config["show_db_name_regex"],
                "denied_db_name_regex": config["denied_db_name_regex"],
                "mode": "",
                "is_ssl": False,
                "verify_ssl": False,
                "service_name": None,
                "sid": None,
                "tunnel": None,
            },
        )
        instance.resource_group.set(
            [resource_groups[name] for name in config["resource_groups"]]
        )
        instance.instance_tag.set([tags[name] for name in config["tags"]])
        instances[key] = instance
        log(
            "Demo instance {}: {}".format(
                "created" if created else "updated", instance.instance_name
            )
        )
    return instances


def _seed_workflow_settings(auth_groups, resource_groups, log):
    for key, config in DEMO_RESOURCE_GROUPS.items():
        resource_group = resource_groups[key]
        audit_auth_groups = ",".join(
            str(auth_groups[group_name].id) for group_name in config["approval_groups"]
        )
        _, created = WorkflowAuditSetting.objects.update_or_create(
            group_id=resource_group.group_id,
            workflow_type=WorkflowType.SQL_REVIEW,
            defaults={
                "group_name": resource_group.group_name,
                "audit_auth_groups": audit_auth_groups,
            },
        )
        log(
            "Workflow setting {}: {} -> {}".format(
                "created" if created else "updated",
                resource_group.group_name,
                " -> ".join(config["approval_groups"]),
            )
        )
