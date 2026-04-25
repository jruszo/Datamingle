from api_core.legacy_tests import TestInstance
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Permission
from django.test import TestCase, override_settings
from django.urls import Resolver404, resolve
import tempfile
from unittest.mock import patch

from api_instances.serializers import (
    InstanceCreateSerializer,
    InstanceResourceSerializer,
)
from api_instances.views import _safe_dictionary_export_path
from sql.models import (
    Instance,
    InstanceAccount,
    InstanceDatabase,
    ParamHistory,
    ParamTemplate,
    ResourceGroup,
)


class FakeDictionaryResult:
    def __init__(self, rows, column_list=None, error="", full_sql=""):
        self.rows = rows
        self.column_list = column_list or []
        self.error = error
        self.full_sql = full_sql

    def to_dict(self):
        if not self.column_list:
            raise ValueError("FakeDictionaryResult requires column_list for to_dict().")

        records = []
        for row in self.rows:
            if len(self.column_list) == 1 and not isinstance(row, (list, tuple)):
                row = (row,)
            if len(row) != len(self.column_list):
                raise ValueError(
                    "FakeDictionaryResult row length does not match column_list."
                )
            records.append(dict(zip(self.column_list, row)))
        return records


class FakeDictionaryEngine:
    def __init__(self, instance):
        self.instance = instance
        self.closed = False
        self.executed = []
        self.queries = []

    def escape_string(self, value):
        return value

    def close(self):
        self.closed = True

    def get_all_databases(self):
        return FakeDictionaryResult(["appdb", "mysql", "hidden"])

    def get_all_databases_summary(self):
        return FakeDictionaryResult(
            [
                {
                    "db_name": "appdb",
                    "table_rows": 12,
                    "data_length": 128,
                    "index_length": 32,
                    "data_total": 160,
                },
                {
                    "db_name": "analytics",
                    "table_rows": 4,
                    "data_length": 64,
                    "index_length": 16,
                    "data_total": 80,
                },
            ]
        )

    def get_group_tables_by_db(self, db_name):
        return {"a": [["accounts", "Account table"]], "o": [["orders", "Order table"]]}

    def get_table_meta_data(self, db_name, tb_name, **kwargs):
        return {
            "column_list": ["table_name", "table_rows"],
            "rows": [tb_name, 12],
        }

    def get_table_desc_data(self, db_name, tb_name, **kwargs):
        return {
            "column_list": ["Column Name", "Column Type"],
            "rows": [["id", "int"], ["name", "varchar(64)"]],
        }

    def get_table_index_data(self, db_name, tb_name, **kwargs):
        return {
            "column_list": ["Column Name", "Index Name"],
            "rows": [["id", "PRIMARY"]],
        }

    def get_tables_metas_data(self, db_name, **kwargs):
        return [{"TABLE_NAME": "accounts", "TABLE_COMMENT": "Account table"}]

    def query(self, db_name, sql):
        self.queries.append((db_name, sql))
        return FakeDictionaryResult(
            [["accounts", "CREATE TABLE accounts (id int)"]],
            ["Table", "Create Table"],
        )

    def execute(self, db_name, sql):
        self.executed.append((db_name, sql))
        return FakeDictionaryResult([])

    def get_instance_users_summary(self):
        return FakeDictionaryResult(
            [
                {
                    "user_host": "`app`@`%`",
                    "user": "app",
                    "host": "%",
                    "privileges": ["SELECT"],
                    "is_locked": "N",
                },
                {
                    "user_host": "`analytics`@`%`",
                    "user": "analytics",
                    "host": "%",
                    "privileges": ["SELECT"],
                    "is_locked": "N",
                },
            ]
        )

    def create_instance_user(self, db_name, user, host, password1, remark):
        return FakeDictionaryResult(
            [
                {
                    "instance": self.instance,
                    "db_name": db_name or "",
                    "user": user,
                    "host": host or "",
                    "password": password1,
                    "remark": remark,
                }
            ]
        )

    def reset_instance_user_pwd(self, user_host, db_name_user, reset_pwd):
        return FakeDictionaryResult([])

    def drop_instance_user(self, user_host, db_name_user):
        return FakeDictionaryResult([])

    def get_variables(self, variables=None):
        rows = [["max_connections", "100"], ["read_only", "OFF"]]
        if variables:
            wanted = {variable.lower() for variable in variables}
            rows = [row for row in rows if row[0].lower() in wanted]
        return FakeDictionaryResult(rows)

    def set_variable(self, variable_name, variable_value):
        return FakeDictionaryResult(
            [], full_sql=f"set global {variable_name}={variable_value};"
        )

    def processlist(self, command_type, **kwargs):
        return FakeDictionaryResult(
            [[101, "app", "127.0.0.1", "appdb", "Query", 3, "executing", "select 1"]],
            ["id", "user", "host", "db", "command", "time", "state", "info"],
        )

    def get_kill_command(self, thread_ids):
        return "".join(f"kill {thread_id};" for thread_id in thread_ids)

    def kill(self, thread_ids):
        return FakeDictionaryResult([], full_sql=self.get_kill_command(thread_ids))

    def tablespace(self, offset=0, row_count=14):
        return FakeDictionaryResult(
            [["appdb", "orders", "InnoDB", 128, 1000, 96, 32, 0, 0]],
            [
                "table_schema",
                "table_name",
                "engine",
                "total_size",
                "table_rows",
                "data_size",
                "index_size",
                "data_free",
                "pct_free",
            ],
        )

    def tablespace_count(self):
        return FakeDictionaryResult([[1]], ["count"])

    def get_long_transaction(self):
        return FakeDictionaryResult(
            [[101, "RUNNING", "select 1"]],
            ["trx_mysql_thread_id", "trx_state", "trx_query"],
        )

    def trxandlocks(self):
        return FakeDictionaryResult(
            [[101, 102, "select waiting"]],
            ["Waiting Thread ID", "Blocking Thread ID", "Waiting Transaction SQL"],
        )


class InstanceSerializerTests(TestCase):
    def test_instance_create_serializer_trims_host_and_optional_strings(self):
        serializer = InstanceCreateSerializer(
            data={
                "instance_name": " demo ",
                "type": "master",
                "db_type": "mysql",
                "host": " host.example ",
                "port": 3306,
                "user": " demo_user ",
                "db_name": " demo_db ",
                "charset": " utf8mb4 ",
                "service_name": " svc ",
                "sid": " sid ",
            }
        )

        self.assertTrue(serializer.is_valid(), serializer.errors)
        self.assertEqual(serializer.validated_data["host"], "host.example")
        self.assertEqual(serializer.validated_data["user"], "demo_user")
        self.assertEqual(serializer.validated_data["db_name"], "demo_db")
        self.assertEqual(serializer.validated_data["charset"], "utf8mb4")
        self.assertEqual(serializer.validated_data["service_name"], "svc")
        self.assertEqual(serializer.validated_data["sid"], "sid")

    def test_instance_resource_serializer_allows_blank_optional_names(self):
        serializer = InstanceResourceSerializer(
            data={
                "instance_id": 1,
                "resource_type": "database",
                "db_name": "",
                "schema_name": "",
                "tb_name": "",
            }
        )

        self.assertTrue(serializer.is_valid(), serializer.errors)


class DataDictionaryApiTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(
            username="dictionary_user", password="test"
        )
        self.group = ResourceGroup.objects.create(group_name="Dictionary Group")
        self.user.resource_group.add(self.group)
        self.instance = Instance.objects.create(
            instance_name="dictionary-mysql",
            type="master",
            db_type="mysql",
            host="127.0.0.1",
            port=3306,
            user="root",
            show_db_name_regex="^app",
        )
        self.instance.resource_group.add(self.group)
        self.client.force_login(self.user)

    def add_permission(self, codename):
        self.user.user_permissions.add(Permission.objects.get(codename=codename))

    def test_instances_requires_data_dictionary_permission(self):
        response = self.client.get("/api/v1/instance/data-dictionary/instances/")

        self.assertEqual(response.status_code, 403)

    def test_instances_lists_user_visible_supported_instances(self):
        self.add_permission("menu_data_dictionary")

        response = self.client.get("/api/v1/instance/data-dictionary/instances/")

        self.assertEqual(response.status_code, 200)
        payload = response.json()["data"]
        self.assertEqual(payload[0]["id"], self.instance.id)
        self.assertEqual(payload[0]["instance_name"], "dictionary-mysql")
        self.assertEqual(payload[0]["db_type"], "mysql")

    @patch("api_instances.views.get_engine")
    def test_databases_are_filtered_by_instance_visibility_rules(self, get_engine):
        self.add_permission("menu_data_dictionary")
        get_engine.side_effect = lambda instance: FakeDictionaryEngine(instance)

        response = self.client.get(
            "/api/v1/instance/data-dictionary/databases/",
            {"instance_id": self.instance.id},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["data"], {"count": 1, "result": ["appdb"]})

    @patch("api_instances.views.get_engine")
    def test_tables_returns_grouped_table_comments(self, get_engine):
        self.add_permission("menu_data_dictionary")
        get_engine.side_effect = lambda instance: FakeDictionaryEngine(instance)

        response = self.client.get(
            "/api/v1/instance/data-dictionary/tables/",
            {"instance_id": self.instance.id, "db_name": "appdb"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["data"]["count"], 2)
        self.assertEqual(
            response.json()["data"]["result"][0],
            {"group": "a", "tables": [["accounts", "Account table"]]},
        )

    @patch("api_instances.views.get_engine")
    def test_table_detail_returns_metadata_columns_indexes_and_create_sql(
        self, get_engine
    ):
        self.add_permission("menu_data_dictionary")
        engine = FakeDictionaryEngine(self.instance)
        get_engine.return_value = engine

        response = self.client.get(
            "/api/v1/instance/data-dictionary/table/",
            {
                "instance_id": self.instance.id,
                "db_name": "appdb",
                "table_name": "accounts",
            },
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()["data"]
        self.assertEqual(payload["meta_data"]["rows"], ["accounts", 12])
        self.assertEqual(payload["desc"]["rows"][0], ["id", "int"])
        self.assertEqual(payload["index"]["rows"][0], ["id", "PRIMARY"])
        self.assertEqual(
            payload["create_sql"][0], ["accounts", "CREATE TABLE accounts (id int)"]
        )
        self.assertEqual(engine.queries[-1], ("appdb", "show create table `accounts`;"))

    def test_safe_dictionary_export_path_rejects_path_escape(self):
        self.assertEqual(
            _safe_dictionary_export_path("/tmp/export", "../other", "appdb"), ""
        )

    @patch("api_instances.views.get_engine")
    def test_export_requires_export_permission(self, get_engine):
        self.add_permission("menu_data_dictionary")
        get_engine.side_effect = lambda instance: FakeDictionaryEngine(instance)

        response = self.client.get(
            "/api/v1/instance/data-dictionary/export/",
            {"instance_id": self.instance.id, "db_name": "appdb"},
        )

        self.assertEqual(response.status_code, 403)

    @patch("api_instances.views.get_engine")
    def test_export_returns_database_dictionary_file(self, get_engine):
        self.add_permission("data_dictionary_export")
        get_engine.side_effect = lambda instance: FakeDictionaryEngine(instance)

        with tempfile.TemporaryDirectory() as temp_dir:
            with override_settings(BASE_DIR=temp_dir):
                response = self.client.get(
                    "/api/v1/instance/data-dictionary/export/",
                    {"instance_id": self.instance.id, "db_name": "appdb"},
                )

        self.assertEqual(response.status_code, 200)
        self.assertIn("dictionary-mysql_appdb.html", response["Content-Disposition"])


class InstanceOperationDatabaseApiTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(
            username="database_user", password="test"
        )
        self.owner = get_user_model().objects.create_user(
            username="database_owner", password="test", display="Database Owner"
        )
        self.group = ResourceGroup.objects.create(group_name="Database Group")
        self.user.resource_group.add(self.group)
        self.instance = Instance.objects.create(
            instance_name="ops-mysql",
            type="master",
            db_type="mysql",
            host="127.0.0.1",
            port=3306,
            user="root",
        )
        self.instance.resource_group.add(self.group)
        self.client.force_login(self.user)

    def add_database_permission(self):
        self.user.user_permissions.add(Permission.objects.get(codename="menu_database"))

    def test_database_list_requires_database_permission(self):
        response = self.client.get(
            "/api/v1/instance-operations/database/",
            {"instance_id": self.instance.id},
        )

        self.assertEqual(response.status_code, 403)

    @patch("api_instances.views.get_engine")
    def test_database_list_merges_saved_owner_metadata(self, get_engine):
        self.add_database_permission()
        InstanceDatabase.objects.create(
            instance=self.instance,
            db_name="appdb",
            owner=self.owner.username,
            owner_display=self.owner.display,
            remark="Owned by app team",
        )
        get_engine.side_effect = lambda instance: FakeDictionaryEngine(instance)

        response = self.client.get(
            "/api/v1/instance-operations/database/",
            {"instance_id": self.instance.id},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()["data"]
        self.assertEqual(payload["count"], 2)
        self.assertTrue(payload["results"][0]["saved"])
        self.assertEqual(payload["results"][0]["owner"], "database_owner")

    @patch("api_instances.views._clear_instance_resource_cache")
    @patch("api_instances.views.get_engine")
    def test_database_create_executes_and_saves_metadata(self, get_engine, clear_cache):
        self.add_database_permission()
        engine = FakeDictionaryEngine(self.instance)
        get_engine.return_value = engine

        response = self.client.post(
            "/api/v1/instance-operations/database/",
            {
                "instance_id": self.instance.id,
                "db_name": "appdb",
                "owner": self.owner.username,
                "remark": "Owned by app team",
            },
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 201)
        self.assertTrue(
            InstanceDatabase.objects.filter(
                instance=self.instance, db_name="appdb", owner=self.owner.username
            ).exists()
        )
        self.assertEqual(
            engine.executed[-1], ("information_schema", "create database `appdb`;")
        )
        clear_cache.assert_called_once()

    @patch("api_instances.views._clear_instance_resource_cache")
    @patch("api_instances.views.get_engine")
    def test_database_create_quotes_mysql_identifier(self, get_engine, clear_cache):
        self.add_database_permission()
        engine = FakeDictionaryEngine(self.instance)
        get_engine.return_value = engine

        response = self.client.post(
            "/api/v1/instance-operations/database/",
            {
                "instance_id": self.instance.id,
                "db_name": "app`db",
                "owner": self.owner.username,
            },
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(
            engine.executed[-1], ("information_schema", "create database `app``db`;")
        )

    def test_database_update_registers_metadata(self):
        self.add_database_permission()

        response = self.client.put(
            "/api/v1/instance-operations/database/metadata/",
            {
                "instance_id": self.instance.id,
                "db_name": "appdb",
                "owner": self.owner.username,
                "remark": "Updated",
            },
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        record = InstanceDatabase.objects.get(instance=self.instance, db_name="appdb")
        self.assertEqual(record.owner_display, "Database Owner")
        self.assertEqual(record.remark, "Updated")


class InstanceOperationAccountApiTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(
            username="account_user", password="test"
        )
        self.group = ResourceGroup.objects.create(group_name="Account Group")
        self.user.resource_group.add(self.group)
        self.instance = Instance.objects.create(
            instance_name="ops-account-mysql",
            type="master",
            db_type="mysql",
            host="127.0.0.1",
            port=3306,
            user="root",
        )
        self.instance.resource_group.add(self.group)
        self.client.force_login(self.user)

    def add_menu_permission(self):
        self.user.user_permissions.add(
            Permission.objects.get(codename="menu_instance_account")
        )

    def add_manage_permission(self):
        self.user.user_permissions.add(
            Permission.objects.get(codename="instance_account_manage")
        )

    def test_account_list_requires_menu_permission(self):
        response = self.client.get(
            "/api/v1/instance-operations/account/",
            {"instance_id": self.instance.id},
        )

        self.assertEqual(response.status_code, 403)

    @patch("api_instances.views.get_engine")
    def test_account_list_merges_saved_metadata(self, get_engine):
        self.add_menu_permission()
        InstanceAccount.objects.create(
            instance=self.instance,
            user="app",
            host="%",
            db_name="",
            remark="Managed by app team",
        )
        get_engine.side_effect = lambda instance: FakeDictionaryEngine(instance)

        response = self.client.get(
            "/api/v1/instance-operations/account/",
            {"instance_id": self.instance.id},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()["data"]
        self.assertEqual(payload["count"], 2)
        self.assertTrue(payload["results"][0]["saved"])
        self.assertEqual(payload["results"][0]["remark"], "Managed by app team")

    @patch("api_instances.views.get_engine")
    def test_account_create_requires_manage_permission(self, get_engine):
        self.add_menu_permission()
        get_engine.side_effect = lambda instance: FakeDictionaryEngine(instance)

        response = self.client.post(
            "/api/v1/instance-operations/account/",
            {
                "instance_id": self.instance.id,
                "user": "app",
                "host": "%",
                "password": "StrongPass123!",
                "remark": "Managed by app team",
            },
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 403)

    @patch("api_instances.views.get_engine")
    def test_account_create_executes_and_saves_metadata(self, get_engine):
        self.add_manage_permission()
        get_engine.side_effect = lambda instance: FakeDictionaryEngine(instance)

        response = self.client.post(
            "/api/v1/instance-operations/account/",
            {
                "instance_id": self.instance.id,
                "user": "app",
                "host": "%",
                "password": "StrongPass123!",
                "remark": "Managed by app team",
            },
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 201)
        self.assertTrue(
            InstanceAccount.objects.filter(
                instance=self.instance, user="app", host="%"
            ).exists()
        )
        self.assertEqual(
            InstanceAccount.objects.get(
                instance=self.instance, user="app", host="%"
            ).password,
            "",
        )
        self.assertIsNotNone(response.json()["data"]["id"])

    def test_account_metadata_update_registers_account(self):
        self.add_manage_permission()

        response = self.client.put(
            "/api/v1/instance-operations/account/metadata/",
            {
                "instance_id": self.instance.id,
                "user": "app",
                "host": "%",
                "remark": "Updated",
            },
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        account = InstanceAccount.objects.get(instance=self.instance, user="app")
        self.assertEqual(account.remark, "Updated")

    @patch("api_instances.views.get_engine")
    def test_account_password_reset_does_not_store_password(self, get_engine):
        self.add_manage_permission()
        InstanceAccount.objects.create(
            instance=self.instance,
            user="app",
            host="%",
            db_name="",
            remark="Existing remark",
        )
        get_engine.side_effect = lambda instance: FakeDictionaryEngine(instance)

        response = self.client.post(
            "/api/v1/instance-operations/account/password/",
            {
                "instance_id": self.instance.id,
                "user": "app",
                "host": "%",
                "user_host": "`app`@`%`",
                "password": "StrongerPass123!",
            },
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        account = InstanceAccount.objects.get(instance=self.instance, user="app")
        self.assertEqual(account.password, "")
        self.assertEqual(account.remark, "Existing remark")

    @patch("api_instances.views.get_engine")
    def test_account_grant_returns_mysql_grant_sql(self, get_engine):
        self.add_manage_permission()
        engine = FakeDictionaryEngine(self.instance)
        get_engine.return_value = engine

        response = self.client.post(
            "/api/v1/instance-operations/account/grant/",
            {
                "instance_id": self.instance.id,
                "user_host": "`app`@`%`",
                "op_type": 0,
                "priv_type": 0,
                "privs": {"global_privs": ["SELECT"]},
            },
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json()["data"]["grant_sql"],
            "GRANT SELECT ON *.* TO `app`@`%`;",
        )
        self.assertEqual(
            engine.executed[-1], ("mysql", "GRANT SELECT ON *.* TO `app`@`%`;")
        )

    @patch("api_instances.views.get_engine")
    def test_account_grant_rejects_unsupported_mysql_privilege(self, get_engine):
        self.add_manage_permission()
        get_engine.side_effect = lambda instance: FakeDictionaryEngine(instance)

        response = self.client.post(
            "/api/v1/instance-operations/account/grant/",
            {
                "instance_id": self.instance.id,
                "user_host": "`app`@`%`",
                "op_type": 0,
                "priv_type": 0,
                "privs": {"global_privs": ["SELECT; DROP USER root"]},
            },
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)

    @patch("api_instances.views.get_engine")
    def test_account_grant_rejects_invalid_type_values(self, get_engine):
        self.add_manage_permission()
        get_engine.side_effect = lambda instance: FakeDictionaryEngine(instance)

        response = self.client.post(
            "/api/v1/instance-operations/account/grant/",
            {
                "instance_id": self.instance.id,
                "user_host": "`app`@`%`",
                "op_type": "grant",
                "priv_type": 0,
                "privs": {"global_privs": ["SELECT"]},
            },
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("op_type", response.json())

    @patch("api_instances.views.get_engine")
    def test_account_grant_quotes_database_identifier(self, get_engine):
        self.add_manage_permission()
        engine = FakeDictionaryEngine(self.instance)
        get_engine.return_value = engine

        response = self.client.post(
            "/api/v1/instance-operations/account/grant/",
            {
                "instance_id": self.instance.id,
                "user_host": "`app`@`%`",
                "op_type": 0,
                "priv_type": 1,
                "privs": {"db_privs": ["SELECT"]},
                "db_names": ["app`db"],
            },
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json()["data"]["grant_sql"],
            "GRANT SELECT ON `app``db`.* TO `app`@`%`;",
        )

    @patch("api_instances.views.get_engine")
    def test_account_lock_quotes_mysql_account_identifier(self, get_engine):
        self.add_manage_permission()
        engine = FakeDictionaryEngine(self.instance)
        get_engine.return_value = engine

        response = self.client.post(
            "/api/v1/instance-operations/account/lock/",
            {
                "instance_id": self.instance.id,
                "user_host": "`app``name`@`%`",
                "locked": True,
            },
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            engine.executed[-1],
            ("mysql", "ALTER USER `app``name`@`%` ACCOUNT LOCK;"),
        )

    @patch("api_instances.views.get_engine")
    def test_account_delete_removes_saved_metadata(self, get_engine):
        self.add_manage_permission()
        InstanceAccount.objects.create(
            instance=self.instance, user="app", host="%", db_name="", remark=""
        )
        get_engine.side_effect = lambda instance: FakeDictionaryEngine(instance)

        response = self.client.delete(
            "/api/v1/instance-operations/account/delete/",
            {
                "instance_id": self.instance.id,
                "user": "app",
                "host": "%",
                "user_host": "`app`@`%`",
            },
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        self.assertFalse(
            InstanceAccount.objects.filter(
                instance=self.instance, user="app", host="%"
            ).exists()
        )


class InstanceOperationParamApiTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(
            username="param_user", password="test", display="Param User"
        )
        self.group = ResourceGroup.objects.create(group_name="Parameter Group")
        self.user.resource_group.add(self.group)
        self.instance = Instance.objects.create(
            instance_name="ops-param-mysql",
            type="master",
            db_type="mysql",
            host="127.0.0.1",
            port=3306,
            user="root",
        )
        self.instance.resource_group.add(self.group)
        self.client.force_login(self.user)

    def add_menu_permission(self):
        self.user.user_permissions.add(Permission.objects.get(codename="menu_param"))

    def add_view_permission(self):
        self.user.user_permissions.add(Permission.objects.get(codename="param_view"))

    def add_edit_permission(self):
        self.user.user_permissions.add(Permission.objects.get(codename="param_edit"))

    def create_template(self, editable=True):
        return ParamTemplate.objects.create(
            db_type="mysql",
            variable_name="max_connections",
            default_value="151",
            editable=editable,
            valid_values="[1-100000]",
            description="Maximum simultaneous connections",
        )

    def test_param_instances_requires_menu_permission(self):
        response = self.client.get("/api/v1/instance-operations/param/instances/")

        self.assertEqual(response.status_code, 403)

    def test_param_instances_lists_visible_instances(self):
        self.add_menu_permission()

        response = self.client.get("/api/v1/instance-operations/param/instances/")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["data"][0]["id"], self.instance.id)

    @patch("api_instances.views.get_engine")
    def test_param_list_merges_template_metadata(self, get_engine):
        self.add_view_permission()
        self.create_template(editable=True)
        get_engine.side_effect = lambda instance: FakeDictionaryEngine(instance)

        response = self.client.get(
            "/api/v1/instance-operations/param/",
            {"instance_id": self.instance.id, "editable": "true"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()["data"]
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["results"][0]["variable_name"], "max_connections")
        self.assertTrue(payload["results"][0]["configured"])
        self.assertTrue(payload["results"][0]["editable"])

    def test_param_history_filters_visible_instance(self):
        self.add_view_permission()
        ParamHistory.objects.create(
            instance=self.instance,
            variable_name="max_connections",
            old_var="100",
            new_var="200",
            set_sql="set global max_connections=200;",
            user_name=self.user.username,
            user_display=self.user.display,
        )

        response = self.client.get(
            "/api/v1/instance-operations/param/history/",
            {"instance_id": self.instance.id},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()["data"]
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["results"][0]["new_var"], "200")

    def test_param_history_ignores_invalid_pagination_values(self):
        self.add_view_permission()
        ParamHistory.objects.create(
            instance=self.instance,
            variable_name="max_connections",
            old_var="100",
            new_var="200",
            set_sql="set global max_connections=200;",
            user_name=self.user.username,
            user_display=self.user.display,
        )

        response = self.client.get(
            "/api/v1/instance-operations/param/history/",
            {"instance_id": self.instance.id, "page": "bad", "size": "bad"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["data"]["count"], 1)

    @patch("api_instances.views.get_engine")
    def test_param_edit_requires_edit_permission(self, get_engine):
        self.add_view_permission()
        self.create_template(editable=True)
        get_engine.side_effect = lambda instance: FakeDictionaryEngine(instance)

        response = self.client.post(
            "/api/v1/instance-operations/param/edit/",
            {
                "instance_id": self.instance.id,
                "variable_name": "max_connections",
                "runtime_value": "200",
            },
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 403)

    @patch("api_instances.views.get_engine")
    def test_param_edit_sets_variable_and_records_history(self, get_engine):
        self.add_edit_permission()
        self.create_template(editable=True)
        get_engine.side_effect = lambda instance: FakeDictionaryEngine(instance)

        response = self.client.post(
            "/api/v1/instance-operations/param/edit/",
            {
                "instance_id": self.instance.id,
                "variable_name": "max_connections",
                "runtime_value": "200",
            },
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        history = ParamHistory.objects.get(instance=self.instance)
        self.assertEqual(history.old_var, "100")
        self.assertEqual(history.new_var, "200")
        self.assertEqual(history.user_display, "Param User")

    @patch("api_instances.views.get_engine")
    def test_param_edit_rejects_unexpected_runtime_row_shape(self, get_engine):
        self.add_edit_permission()
        self.create_template(editable=True)
        engine = FakeDictionaryEngine(self.instance)
        engine.get_variables = lambda variables=None: FakeDictionaryResult(
            [["max_connections"]]
        )
        get_engine.return_value = engine

        response = self.client.post(
            "/api/v1/instance-operations/param/edit/",
            {
                "instance_id": self.instance.id,
                "variable_name": "max_connections",
                "runtime_value": "200",
            },
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("unexpected row shape", response.json()["errors"])


class InstanceOperationDiagnosticApiTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(
            username="diagnostic_user", password="test"
        )
        self.group = ResourceGroup.objects.create(group_name="Diagnostic Group")
        self.user.resource_group.add(self.group)
        self.instance = Instance.objects.create(
            instance_name="ops-diagnostic-mysql",
            type="master",
            db_type="mysql",
            host="127.0.0.1",
            port=3306,
            user="root",
        )
        self.instance.resource_group.add(self.group)
        self.client.force_login(self.user)

    def add_permission(self, codename):
        self.user.user_permissions.add(Permission.objects.get(codename=codename))

    def test_diagnostic_instances_requires_menu_permission(self):
        response = self.client.get("/api/v1/instance-operations/diagnostic/instances/")

        self.assertEqual(response.status_code, 403)

    def test_diagnostic_instances_lists_visible_instances(self):
        self.add_permission("menu_dbdiagnostic")

        response = self.client.get("/api/v1/instance-operations/diagnostic/instances/")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["data"][0]["id"], self.instance.id)

    @patch("api_instances.views.get_engine")
    def test_process_list_requires_process_view_permission(self, get_engine):
        self.add_permission("menu_dbdiagnostic")
        get_engine.side_effect = lambda instance: FakeDictionaryEngine(instance)

        response = self.client.get(
            "/api/v1/instance-operations/diagnostic/processes/",
            {"instance_id": self.instance.id},
        )

        self.assertEqual(response.status_code, 403)

    @patch("api_instances.views.get_engine")
    def test_process_list_returns_rows(self, get_engine):
        self.add_permission("process_view")
        get_engine.side_effect = lambda instance: FakeDictionaryEngine(instance)

        response = self.client.get(
            "/api/v1/instance-operations/diagnostic/processes/",
            {"instance_id": self.instance.id, "command_type": "All"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()["data"]
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["results"][0]["id"], 101)

    @patch("api_instances.views.get_engine")
    def test_kill_preview_and_kill_succeed_with_permission(self, get_engine):
        self.add_permission("process_kill")
        get_engine.side_effect = lambda instance: FakeDictionaryEngine(instance)

        preview = self.client.post(
            "/api/v1/instance-operations/diagnostic/kill/preview/",
            {"instance_id": self.instance.id, "thread_ids": [101]},
            content_type="application/json",
        )
        kill = self.client.post(
            "/api/v1/instance-operations/diagnostic/kill/",
            {"instance_id": self.instance.id, "thread_ids": [101]},
            content_type="application/json",
        )

        self.assertEqual(preview.status_code, 200)
        self.assertEqual(preview.json()["data"]["kill_sql"], "kill 101;")
        self.assertEqual(kill.status_code, 200)

    @patch("api_instances.views.get_engine")
    def test_kill_requires_process_kill_permission(self, get_engine):
        self.add_permission("menu_dbdiagnostic")
        get_engine.side_effect = lambda instance: FakeDictionaryEngine(instance)

        response = self.client.post(
            "/api/v1/instance-operations/diagnostic/kill/",
            {"instance_id": self.instance.id, "thread_ids": [101]},
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 403)

    @patch("api_instances.views.get_engine")
    def test_tablespace_transactions_and_locks_return_rows(self, get_engine):
        self.add_permission("tablespace_view")
        self.add_permission("trx_view")
        self.add_permission("trxandlocks_view")
        get_engine.side_effect = lambda instance: FakeDictionaryEngine(instance)

        tablespace = self.client.get(
            "/api/v1/instance-operations/diagnostic/tablespace/",
            {"instance_id": self.instance.id},
        )
        transactions = self.client.get(
            "/api/v1/instance-operations/diagnostic/transactions/",
            {"instance_id": self.instance.id},
        )
        locks = self.client.get(
            "/api/v1/instance-operations/diagnostic/locks/",
            {"instance_id": self.instance.id},
        )

        self.assertEqual(tablespace.status_code, 200)
        self.assertEqual(tablespace.json()["data"]["count"], 1)
        self.assertEqual(transactions.status_code, 200)
        self.assertEqual(
            transactions.json()["data"]["results"][0]["trx_state"], "RUNNING"
        )
        self.assertEqual(locks.status_code, 200)
        self.assertEqual(locks.json()["data"]["results"][0]["Blocking Thread ID"], 102)

    @patch("api_instances.views.get_engine")
    def test_tablespace_ignores_invalid_pagination_values(self, get_engine):
        self.add_permission("tablespace_view")
        get_engine.side_effect = lambda instance: FakeDictionaryEngine(instance)

        response = self.client.get(
            "/api/v1/instance-operations/diagnostic/tablespace/",
            {"instance_id": self.instance.id, "page": "bad", "size": "bad"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["data"]["count"], 1)

    @patch("api_instances.views.get_engine")
    def test_process_list_rejects_unsupported_query_params(self, get_engine):
        self.add_permission("process_view")
        get_engine.side_effect = lambda instance: FakeDictionaryEngine(instance)

        response = self.client.get(
            "/api/v1/instance-operations/diagnostic/processes/",
            {"instance_id": self.instance.id, "base_sql": "select 1"},
        )

        self.assertEqual(response.status_code, 400)
        get_engine.assert_not_called()


class RetiredLegacyRouteTests(TestCase):
    def test_removed_legacy_bootstrap_routes_return_404(self):
        removed_routes = [
            "/sqlanalyze/",
            "/sqladvisor/",
            "/slowquery/",
            "/slowquery_advisor/",
            "/my2sql/",
            "/schemasync/",
            "/dbaprinciples/",
            "/query/explain/",
            "/sql_analyze/generate/",
            "/sql_analyze/analyze/",
            "/binlog/list/",
            "/binlog/my2sql/",
            "/binlog/del_log/",
            "/instance/schemasync/",
            "/slowquery/review/",
            "/slowquery/review_history/",
            "/slowquery/optimize_sqladvisor/",
            "/slowquery/optimize_sqltuning/",
            "/slowquery/optimize_soar/",
            "/slowquery/optimize_sqltuningadvisor/",
            "/slowquery/report/",
        ]

        for route in removed_routes:
            with self.subTest(route=route):
                with self.assertRaises(Resolver404):
                    resolve(route)
