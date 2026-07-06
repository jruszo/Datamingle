import json
from datetime import datetime, timedelta
from unittest.mock import patch, Mock, ANY

import MySQLdb
from django.test import TestCase

from common.config import SysConfig
from sql.engines import ResultSet, ReviewSet
from sql.engines.models import ReviewResult
from sql.engines.mysql import MysqlEngine
from sql.models import Instance, SqlWorkflow, SqlWorkflowContent


class TestMysql(TestCase):
    def setUp(self):
        self.ins1 = Instance(
            instance_name="some_ins",
            type="slave",
            db_type="mysql",
            host="some_host",
            port=1366,
            user="ins_user",
            password="some_str",
        )
        self.ins1.save()
        self.sys_config = SysConfig()
        self.wf = SqlWorkflow.objects.create(
            workflow_name="some_name",
            team_id=1,
            team_name="g1",
            engineer_display="",
            audit_auth_groups="some_group",
            create_time=datetime.now() - timedelta(days=1),
            status="workflow_finish",
            is_backup=True,
            instance=self.ins1,
            db_name="some_db",
            syntax_type=1,
        )
        SqlWorkflowContent.objects.create(workflow=self.wf)

    def tearDown(self):
        self.ins1.delete()
        self.sys_config.purge()
        SqlWorkflow.objects.all().delete()
        SqlWorkflowContent.objects.all().delete()

    @patch("MySQLdb.connect")
    def test_engine_base_info(self, _conn):
        new_engine = MysqlEngine(instance=self.ins1)
        self.assertEqual(new_engine.name, "MySQL")
        self.assertEqual(new_engine.info, "MySQL engine")

    @patch("MySQLdb.connect")
    def testGetConnection(self, connect):
        new_engine = MysqlEngine(instance=self.ins1)
        new_engine.get_connection()
        connect.assert_called_once()

    @patch.object(MysqlEngine, "get_connection")
    def test_get_inventory_details_closes_cursor_and_falls_back_hostname(
        self, mock_get_connection
    ):
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.get_server_info.return_value = "8.0.36"
        mock_cursor.fetchone.return_value = None
        mock_get_connection.return_value = mock_conn

        new_engine = MysqlEngine(instance=self.ins1)
        details = new_engine.get_inventory_details()

        self.assertEqual(details, {"hostname": "some_host", "version": "8.0.36"})
        mock_cursor.close.assert_called_once()

    @patch.object(MysqlEngine, "get_connection")
    def test_get_inventory_details_returns_defaults_when_server_info_fails(
        self, mock_get_connection
    ):
        mock_conn = Mock()
        mock_conn.get_server_info.side_effect = RuntimeError("version boom")
        mock_get_connection.return_value = mock_conn

        new_engine = MysqlEngine(instance=self.ins1)
        details = new_engine.get_inventory_details()

        self.assertEqual(details, {"hostname": "some_host", "version": ""})
        mock_conn.cursor.assert_not_called()

    @patch.object(MysqlEngine, "get_connection")
    def test_get_inventory_details_closes_cursor_and_returns_defaults_on_query_error(
        self, mock_get_connection
    ):
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.get_server_info.return_value = "8.0.36"
        mock_cursor.execute.side_effect = RuntimeError("hostname boom")
        mock_get_connection.return_value = mock_conn

        new_engine = MysqlEngine(instance=self.ins1)
        details = new_engine.get_inventory_details()

        self.assertEqual(details, {"hostname": "some_host", "version": ""})
        mock_cursor.close.assert_called_once()

    @patch("MySQLdb.connect")
    def testQuery(self, connect):
        cur = Mock()
        connect.return_value.cursor = cur
        cur.return_value.execute = Mock()
        cur.return_value.fetchmany.return_value = (("v1", "v2"),)
        cur.return_value.description = (
            ("k1", "some_other_des"),
            ("k2", "some_other_des"),
        )
        new_engine = MysqlEngine(instance=self.ins1)
        query_result = new_engine.query(sql="some_str", limit_num=100)
        cur.return_value.execute.assert_called()
        cur.return_value.fetchmany.assert_called_once_with(size=100)
        connect.return_value.close.assert_called_once()
        self.assertIsInstance(query_result, ResultSet)

    @patch("MySQLdb.connect")
    def test_get_tables_metas_data(self, connect):
        """Add unit test method: test_get_tables_metas_data."""
        mock_conn = Mock()
        mock_cursor = Mock()
        connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Mock query result.
        tables_result = [{"TABLE_SCHEMA": "test_db", "TABLE_NAME": "test_table"}]

        columns_result = [
            {
                "COLUMN_NAME": "id",
                "COLUMN_TYPE": "int",
                "COLUMN_DEFAULT": None,
                "IS_NULLABLE": "NO",
                "EXTRA": "auto_increment",
                "COLUMN_KEY": "PRI",
                "COLUMN_COMMENT": "",
            },
            {
                "COLUMN_NAME": "name",
                "COLUMN_TYPE": "varchar(255)",
                "COLUMN_DEFAULT": None,
                "IS_NULLABLE": "YES",
                "EXTRA": "",
                "COLUMN_KEY": "",
                "COLUMN_COMMENT": "",
            },
        ]
        # Create instance of class under test.
        new_engine = MysqlEngine(instance=self.ins1)
        # Mock self.query method.
        new_engine.query = Mock()
        new_engine.query.side_effect = [
            Mock(rows=tables_result),  # Mock tbs result.
            Mock(rows=columns_result),  # Mock columns result.
        ]
        # Call the method under test.
        result = new_engine.get_tables_metas_data(db_name="test_db")

        # Assert returned result matches expected output.
        expected_result = [
            {
                "ENGINE_KEYS": [
                    {"key": "COLUMN_NAME", "value": "Column Name"},
                    {"key": "COLUMN_TYPE", "value": "Data Type"},
                    {"key": "COLUMN_DEFAULT", "value": "Default Value"},
                    {"key": "IS_NULLABLE", "value": "Nullable"},
                    {"key": "EXTRA", "value": "Auto Increment"},
                    {"key": "COLUMN_KEY", "value": "Primary Key"},
                    {"key": "COLUMN_COMMENT", "value": "Comment"},
                ],
                "TABLE_INFO": {"TABLE_SCHEMA": "test_db", "TABLE_NAME": "test_table"},
                "COLUMNS": [
                    {
                        "COLUMN_NAME": "id",
                        "COLUMN_TYPE": "int",
                        "COLUMN_DEFAULT": None,
                        "IS_NULLABLE": "NO",
                        "EXTRA": "auto_increment",
                        "COLUMN_KEY": "PRI",
                        "COLUMN_COMMENT": "",
                    },
                    {
                        "COLUMN_NAME": "name",
                        "COLUMN_TYPE": "varchar(255)",
                        "COLUMN_DEFAULT": None,
                        "IS_NULLABLE": "YES",
                        "EXTRA": "",
                        "COLUMN_KEY": "",
                        "COLUMN_COMMENT": "",
                    },
                ],
            }
        ]

        self.assertEqual(result, expected_result)

    @patch.object(MysqlEngine, "query")
    def testAllDb(self, mock_query):
        db_result = ResultSet()
        db_result.rows = [("db_1",), ("db_2",)]
        mock_query.return_value = db_result
        new_engine = MysqlEngine(instance=self.ins1)
        dbs = new_engine.get_all_databases()
        self.assertEqual(dbs.rows, ["db_1", "db_2"])

    @patch.object(MysqlEngine, "query")
    def testAllTables(self, mock_query):
        table_result = ResultSet()
        table_result.rows = [("tb_1", "some_des"), ("tb_2", "some_des")]
        mock_query.return_value = table_result
        new_engine = MysqlEngine(instance=self.ins1)
        tables = new_engine.get_all_tables("some_db")
        mock_query.assert_called_once_with(db_name="some_db", sql=ANY)
        self.assertEqual(tables.rows, ["tb_1", "tb_2"])

    @patch.object(MysqlEngine, "query")
    def testAllColumns(self, mock_query):
        db_result = ResultSet()
        db_result.rows = [("col_1", "type"), ("col_2", "type2")]
        mock_query.return_value = db_result
        new_engine = MysqlEngine(instance=self.ins1)
        dbs = new_engine.get_all_columns_by_tb("some_db", "some_tb")
        self.assertEqual(dbs.rows, ["col_1", "col_2"])

    @patch.object(MysqlEngine, "query")
    def testDescribe(self, mock_query):
        new_engine = MysqlEngine(instance=self.ins1)
        new_engine.describe_table("some_db", "some_db")
        mock_query.assert_called_once()

    def testQueryCheck(self):
        new_engine = MysqlEngine(instance=self.ins1)
        sql_without_limit = "-- test\n select user from usertable"
        check_result = new_engine.query_check(db_name="some_db", sql=sql_without_limit)
        self.assertEqual(check_result["filtered_sql"], "select user from usertable")

    def test_query_check_wrong_sql(self):
        new_engine = MysqlEngine(instance=self.ins1)
        wrong_sql = "-- test"
        check_result = new_engine.query_check(db_name="some_db", sql=wrong_sql)
        self.assertDictEqual(
            check_result,
            {
                "msg": "Unsupported query syntax type!",
                "bad_query": True,
                "filtered_sql": "-- test",
                "has_star": False,
            },
        )

    def test_query_check_update_sql(self):
        new_engine = MysqlEngine(instance=self.ins1)
        update_sql = "update user set id=0"
        check_result = new_engine.query_check(db_name="some_db", sql=update_sql)
        self.assertDictEqual(
            check_result,
            {
                "msg": "Unsupported query syntax type!",
                "bad_query": True,
                "filtered_sql": "update user set id=0",
                "has_star": False,
            },
        )

    def test_filter_sql_with_delimiter(self):
        new_engine = MysqlEngine(instance=self.ins1)
        sql_without_limit = "select user from usertable;"
        check_result = new_engine.filter_sql(sql=sql_without_limit, limit_num=100)
        self.assertEqual(check_result, "select user from usertable limit 100;")

    def test_filter_sql_without_delimiter(self):
        new_engine = MysqlEngine(instance=self.ins1)
        sql_without_limit = "select user from usertable"
        check_result = new_engine.filter_sql(sql=sql_without_limit, limit_num=100)
        self.assertEqual(check_result, "select user from usertable limit 100;")

    def test_filter_sql_with_limit(self):
        new_engine = MysqlEngine(instance=self.ins1)
        sql_without_limit = "select user from usertable limit 10"
        check_result = new_engine.filter_sql(sql=sql_without_limit, limit_num=1)
        self.assertEqual(check_result, "select user from usertable limit 1;")

    def test_filter_sql_with_limit_min(self):
        new_engine = MysqlEngine(instance=self.ins1)
        sql_without_limit = "select user from usertable limit 10"
        check_result = new_engine.filter_sql(sql=sql_without_limit, limit_num=100)
        self.assertEqual(check_result, "select user from usertable limit 10;")

    def test_filter_sql_with_limit_offset(self):
        new_engine = MysqlEngine(instance=self.ins1)
        sql_without_limit = "select user from usertable limit 10 offset 100"
        check_result = new_engine.filter_sql(sql=sql_without_limit, limit_num=1)
        self.assertEqual(check_result, "select user from usertable limit 1 offset 100;")

    def test_filter_sql_with_limit_nn(self):
        new_engine = MysqlEngine(instance=self.ins1)
        sql_without_limit = "select user from usertable limit 10, 100"
        check_result = new_engine.filter_sql(sql=sql_without_limit, limit_num=1)
        self.assertEqual(check_result, "select user from usertable limit 10,1;")

    def test_filter_sql_upper(self):
        new_engine = MysqlEngine(instance=self.ins1)
        sql_without_limit = "SELECT USER FROM usertable LIMIT 10, 100"
        check_result = new_engine.filter_sql(sql=sql_without_limit, limit_num=1)
        self.assertEqual(check_result, "SELECT USER FROM usertable limit 10,1;")

    def test_filter_sql_not_select(self):
        new_engine = MysqlEngine(instance=self.ins1)
        sql_without_limit = "show create table usertable;"
        check_result = new_engine.filter_sql(sql=sql_without_limit, limit_num=1)
        self.assertEqual(check_result, "show create table usertable;")

    @patch("sql.engines.mysql.simple_column_mask", return_value=ResultSet())
    def test_query_masking(self, simple_column_mask):
        query_result = ResultSet()
        new_engine = MysqlEngine(instance=self.ins1)
        masking_result = new_engine.query_masking(
            db_name="archery", sql="select 1", resultset=query_result
        )
        self.assertIsInstance(masking_result, ResultSet)
        simple_column_mask.assert_called_once_with(self.ins1, query_result)

    @patch("sql.engines.mysql.simple_column_mask", return_value=ResultSet())
    def test_query_masking_not_select(self, simple_column_mask):
        query_result = ResultSet()
        new_engine = MysqlEngine(instance=self.ins1)
        masking_result = new_engine.query_masking(
            db_name="archery", sql="explain select 1", resultset=query_result
        )
        self.assertEqual(masking_result, query_result)
        simple_column_mask.assert_not_called()

    def test_execute_check_select_sql(self):
        sql = "select * from user"
        row = ReviewResult(
            id=1,
            errlevel=2,
            stagestatus="Rejected unsupported statement",
            errormessage=(
                "Only DML and DDL statements are supported. "
                "Use SQL query feature for SELECT statements!"
            ),
            sql=sql,
        )
        new_engine = MysqlEngine(instance=self.ins1)
        check_result = new_engine.execute_check(db_name="archery", sql=sql)
        self.assertIsInstance(check_result, ReviewSet)
        self.assertEqual(check_result.rows[0].__dict__, row.__dict__)

    def test_execute_check_critical_sql(self):
        self.sys_config.set("critical_ddl_regex", "^|update")
        self.sys_config.get_all_config()
        sql = "update user set id=1"
        row = ReviewResult(
            id=1,
            errlevel=2,
            stagestatus="Rejected high-risk SQL",
            errormessage=(
                "Submitting statements matching " + "^|update" + " is prohibited!"
            ),
            sql=sql,
        )
        new_engine = MysqlEngine(instance=self.ins1)
        check_result = new_engine.execute_check(db_name="archery", sql=sql)
        self.assertIsInstance(check_result, ReviewSet)
        self.assertEqual(check_result.rows[0].__dict__, row.__dict__)

    def test_execute_check_normal_sql(self):
        sql = "update user set id=1"
        row = ReviewResult(
            id=1,
            errlevel=0,
            stagestatus="Audit completed",
            errormessage="None",
            sql=sql,
            affected_rows=0,
            execute_time=0,
        )
        new_engine = MysqlEngine(instance=self.ins1)
        check_result = new_engine.execute_check(db_name="archery", sql=sql)
        self.assertIsInstance(check_result, ReviewSet)
        self.assertEqual(check_result.rows[0].__dict__, row.__dict__)
        self.assertEqual(check_result.syntax_type, 2)

    def test_execute_check_rejects_unsupported_statement(self):
        sql = "grant select on *.* to demo@'%'"
        new_engine = MysqlEngine(instance=self.ins1)
        check_result = new_engine.execute_check(db_name=0, sql=sql)

        self.assertEqual(check_result.error_count, 1)
        self.assertEqual(
            check_result.rows[0].stagestatus, "Rejected unsupported statement"
        )

    def test_execute_check_rejects_comment_only_sql(self):
        sql = "-- only a comment\n/* and another comment */;"
        new_engine = MysqlEngine(instance=self.ins1)

        check_result = new_engine.execute_check(db_name="archery", sql=sql)

        self.assertTrue(check_result.checked)
        self.assertEqual(check_result.error_count, 1)
        self.assertEqual(len(check_result.rows), 1)
        self.assertEqual(check_result.rows[0].errlevel, 2)
        self.assertEqual(check_result.rows[0].stagestatus, "Rejected empty SQL")
        self.assertEqual(
            check_result.rows[0].errormessage, "No executable SQL statements found."
        )

    @patch.object(MysqlEngine, "get_connection")
    @patch.object(MysqlEngine, "query")
    def test_execute_workflow_direct_for_dml(self, mock_query, mock_get_connection):
        self.wf.syntax_type = 2
        self.wf.sqlworkflowcontent.sql_content = "update user set id=1;"
        self.wf.sqlworkflowcontent.review_content = json.dumps(
            [
                {
                    "id": 1,
                    "errlevel": 0,
                    "stagestatus": "Audit completed",
                    "errormessage": "None",
                    "sql": "update user set id=1",
                    "affected_rows": 0,
                    "execute_time": 0,
                }
            ]
        )
        self.wf.save(update_fields=["syntax_type"])
        self.wf.sqlworkflowcontent.save(update_fields=["sql_content", "review_content"])

        mock_query.return_value.rows = ((0,),)
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.rowcount = 3
        mock_conn.cursor.return_value = mock_cursor
        mock_get_connection.return_value = mock_conn

        new_engine = MysqlEngine(instance=self.ins1)
        execute_result = new_engine.execute_workflow(self.wf)

        mock_cursor.execute.assert_called_once_with("update user set id=1")
        self.assertIsInstance(execute_result, ReviewSet)
        self.assertEqual(execute_result.error, None)
        self.assertEqual(execute_result.rows[0].stagestatus, "Execute Successfully")
        self.assertEqual(execute_result.rows[0].affected_rows, 3)

    @patch.object(MysqlEngine, "get_connection")
    @patch.object(MysqlEngine, "query")
    def test_execute_workflow_direct_for_dml_commits_once(
        self, mock_query, mock_get_connection
    ):
        self.wf.syntax_type = 2
        self.wf.sqlworkflowcontent.sql_content = (
            "update user set id=1;delete from user where id=2;"
        )
        self.wf.sqlworkflowcontent.review_content = json.dumps(
            [
                {
                    "id": 1,
                    "errlevel": 0,
                    "stagestatus": "Audit completed",
                    "errormessage": "None",
                    "sql": "update user set id=1",
                    "affected_rows": 0,
                    "execute_time": 0,
                },
                {
                    "id": 2,
                    "errlevel": 0,
                    "stagestatus": "Audit completed",
                    "errormessage": "None",
                    "sql": "delete from user where id=2",
                    "affected_rows": 0,
                    "execute_time": 0,
                },
            ]
        )
        self.wf.save(update_fields=["syntax_type"])
        self.wf.sqlworkflowcontent.save(update_fields=["sql_content", "review_content"])

        mock_query.return_value.rows = ((0,),)
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.rowcount = 1
        mock_conn.cursor.return_value = mock_cursor
        mock_get_connection.return_value = mock_conn

        new_engine = MysqlEngine(instance=self.ins1)
        execute_result = new_engine.execute_workflow(self.wf)

        self.assertEqual(
            [call.args[0] for call in mock_cursor.execute.call_args_list],
            ["update user set id=1", "delete from user where id=2"],
        )
        mock_conn.commit.assert_called_once()
        mock_conn.rollback.assert_not_called()
        self.assertEqual(execute_result.error, None)
        self.assertEqual(len(execute_result.rows), 2)

    @patch.object(MysqlEngine, "get_connection")
    @patch.object(MysqlEngine, "query")
    def test_execute_workflow_direct_for_dml_rolls_back_all_on_error(
        self, mock_query, mock_get_connection
    ):
        self.wf.syntax_type = 2
        self.wf.sqlworkflowcontent.sql_content = (
            "update user set id=1;delete from user where id=2;"
        )
        self.wf.sqlworkflowcontent.review_content = json.dumps(
            [
                {
                    "id": 1,
                    "errlevel": 0,
                    "stagestatus": "Audit completed",
                    "errormessage": "None",
                    "sql": "update user set id=1",
                    "affected_rows": 0,
                    "execute_time": 0,
                },
                {
                    "id": 2,
                    "errlevel": 0,
                    "stagestatus": "Audit completed",
                    "errormessage": "None",
                    "sql": "delete from user where id=2",
                    "affected_rows": 0,
                    "execute_time": 0,
                },
            ]
        )
        self.wf.save(update_fields=["syntax_type"])
        self.wf.sqlworkflowcontent.save(update_fields=["sql_content", "review_content"])

        mock_query.return_value.rows = ((0,),)
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = [None, RuntimeError("boom")]
        mock_conn.cursor.return_value = mock_cursor
        mock_get_connection.return_value = mock_conn

        new_engine = MysqlEngine(instance=self.ins1)
        execute_result = new_engine.execute_workflow(self.wf)

        mock_conn.commit.assert_not_called()
        mock_conn.rollback.assert_called_once()
        self.assertEqual(execute_result.error, "Execution failed")
        self.assertEqual(execute_result.rows[-1].stagestatus, "Execute Failed")

    @patch.object(MysqlEngine, "get_connection")
    @patch.object(MysqlEngine, "query")
    def test_execute_workflow_direct_for_ddl(self, mock_query, mock_get_connection):
        self.wf.sqlworkflowcontent.sql_content = (
            "alter table demo add column note varchar(64);"
        )
        self.wf.sqlworkflowcontent.review_content = json.dumps(
            [
                {
                    "id": 1,
                    "stage": "CHECKED",
                    "errlevel": 0,
                    "stagestatus": "Audit completed",
                    "errormessage": "",
                    "sql": "alter table demo add column note varchar(64)",
                    "affected_rows": 0,
                    "sequence": "0_0_00000001",
                    "backup_dbname": "",
                    "execute_time": "0",
                    "sqlsha1": "",
                    "backup_time": "",
                    "actual_affected_rows": "",
                }
            ]
        )
        self.wf.sqlworkflowcontent.save(update_fields=["sql_content", "review_content"])

        mock_query.return_value.rows = ((0,),)
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_connection.return_value = mock_conn

        new_engine = MysqlEngine(instance=self.ins1)
        execute_result = new_engine.execute_workflow(self.wf)

        mock_cursor.execute.assert_called_once_with(
            "alter table demo add column note varchar(64)"
        )
        self.assertEqual(execute_result.error, None)
        self.assertEqual(execute_result.rows[0].stagestatus, "Execute Successfully")

    @patch("MySQLdb.connect.cursor.execute")
    @patch("MySQLdb.connect.cursor")
    @patch("MySQLdb.connect")
    def test_execute(self, _connect, _cursor, _execute):
        new_engine = MysqlEngine(instance=self.ins1)
        execute_result = new_engine.execute(self.wf)
        self.assertIsInstance(execute_result, ResultSet)

    @patch("MySQLdb.connect")
    def test_server_version(self, _connect):
        _connect.return_value.get_server_info.return_value = "5.7.20-16log"
        new_engine = MysqlEngine(instance=self.ins1)
        server_version = new_engine.server_version
        self.assertTupleEqual(server_version, (5, 7, 20))

    @patch.object(MysqlEngine, "query")
    def test_get_variables_not_filter(self, _query):
        new_engine = MysqlEngine(instance=self.ins1)
        new_engine.get_variables()
        _query.assert_called_once()

    @patch("MySQLdb.connect")
    @patch.object(MysqlEngine, "query")
    def test_get_variables_filter(self, _query, _connect):
        _connect.return_value.get_server_info.return_value = "5.7.20-16log"
        new_engine = MysqlEngine(instance=self.ins1)
        new_engine.get_variables(variables=["binlog_format"])
        _query.assert_called()

    @patch.object(MysqlEngine, "query")
    def test_set_variable(self, _query):
        new_engine = MysqlEngine(instance=self.ins1)
        new_engine.set_variable("binlog_format", "ROW")
        _query.assert_called_once_with(sql="set global binlog_format=ROW;")

    def test_mysql_auto_backup_is_disabled_without_goinception(self):
        new_engine = MysqlEngine(instance=self.ins1)

        self.assertFalse(new_engine.auto_backup)

    @patch.object(MysqlEngine, "query")
    def test_kill_connection(self, _query):
        new_engine = MysqlEngine(instance=self.ins1)
        new_engine.kill_connection(100)
        _query.assert_called_once_with(sql="kill 100")

    @patch("MySQLdb.connect")
    @patch.object(MysqlEngine, "query")
    def test_seconds_behind_master(self, _query, mock_connect):
        # Mock connection to avoid real connection attempts.
        mock_conn = Mock()
        mock_conn.get_server_info.return_value = "8.0.0"
        mock_conn.thread_id.return_value = 12345
        mock_connect.return_value = mock_conn

        new_engine = MysqlEngine(instance=self.ins1)
        new_engine.seconds_behind_master
        _query.assert_called_once_with(
            sql="show slave status",
            close_conn=False,
            cursorclass=MySQLdb.cursors.DictCursor,
        )

    @patch.object(MysqlEngine, "query")
    def test_processlist(self, _query):
        new_engine = MysqlEngine(instance=self.ins1)
        _query.return_value = ResultSet()
        for command_type in ["Query", "All", "Not Sleep"]:
            r = new_engine.processlist(command_type)
            self.assertIsInstance(r, ResultSet)

    @patch.object(MysqlEngine, "query")
    def test_get_kill_command(self, _query):
        new_engine = MysqlEngine(instance=self.ins1)
        _query.return_value.rows = (("kill 1;",), ("kill 2;",))
        r = new_engine.get_kill_command([1, 2])
        self.assertEqual(r, "kill 1;kill 2;")

    @patch("MySQLdb.connect.cursor.execute")
    @patch("MySQLdb.connect.cursor")
    @patch("MySQLdb.connect")
    @patch.object(MysqlEngine, "query")
    def test_kill(self, _query, _connect, _cursor, _execute):
        new_engine = MysqlEngine(instance=self.ins1)
        _query.return_value.rows = (("kill 1;",), ("kill 2;",))
        _execute.return_value = ResultSet()
        r = new_engine.kill([1, 2])
        self.assertIsInstance(r, ResultSet)

    @patch.object(MysqlEngine, "query")
    def test_tablespace(self, _query):
        new_engine = MysqlEngine(instance=self.ins1)
        _query.return_value = ResultSet()
        r = new_engine.tablespace()
        self.assertIsInstance(r, ResultSet)

    @patch.object(MysqlEngine, "query")
    def test_tablespace_count(self, _query):
        new_engine = MysqlEngine(instance=self.ins1)
        _query.return_value = ResultSet()
        r = new_engine.tablespace_count()
        self.assertIsInstance(r, ResultSet)

    @patch.object(MysqlEngine, "query")
    @patch("MySQLdb.connect")
    def test_trxandlocks(self, _connect, _query):
        new_engine = MysqlEngine(instance=self.ins1)
        _connect.return_value = Mock()
        for v in ["5.7.0", "8.0.1"]:
            _connect.return_value.get_server_info.return_value = v
            _query.return_value = ResultSet()
            r = new_engine.trxandlocks()
            self.assertIsInstance(r, ResultSet)

    @patch.object(MysqlEngine, "query")
    def test_get_long_transaction(self, _query):
        new_engine = MysqlEngine(instance=self.ins1)
        _query.return_value = ResultSet()
        r = new_engine.get_long_transaction()
        self.assertIsInstance(r, ResultSet)

    @patch.object(MysqlEngine, "get_bind_users")
    @patch.object(MysqlEngine, "query")
    def test_get_all_databases_summary(self, _query, _get_bind_users):
        db_result1 = ResultSet()
        db_result1.rows = [("some_db", "utf8mb4", "utf8mb4_general_ci")]
        _query.return_value = db_result1
        _get_bind_users.return_value = [("'some_user'@'%'", "cooperate_sign")]
        new_engine = MysqlEngine(instance=self.ins1)
        dbs = new_engine.get_all_databases_summary()
        self.assertEqual(
            dbs.rows,
            [
                {
                    "db_name": "some_db",
                    "charset": "utf8mb4",
                    "collation": "utf8mb4_general_ci",
                    "grantees": ["'some_user'@'%'"],
                    "saved": False,
                }
            ],
        )

    @patch("MySQLdb.connect")
    @patch.object(MysqlEngine, "query")
    def test_get_instance_users_summary(self, _query, _connect):
        result = ResultSet()
        result.error = "query error"
        _query.return_value = result
        new_engine = MysqlEngine(instance=self.ins1)
        user_summary = new_engine.get_instance_users_summary()
        self.assertEqual(user_summary.error, "query error")

        result_with_lock = ResultSet()
        result_with_lock.rows = [("'root'@'localhost'", "root", "localhost", "N")]
        _query.return_value = result_with_lock
        _connect.return_value.get_server_info.return_value = "5.7.20-16log"
        self.assertTupleEqual(new_engine.server_version, (5, 7, 20))
        user_summary_with_lock = new_engine.get_instance_users_summary()
        self.assertEqual(
            user_summary_with_lock.rows,
            [
                {
                    "host": "localhost",
                    "is_locked": "N",
                    "privileges": [("'root'@'localhost'", "root", "localhost", "N")],
                    "saved": False,
                    "user": "root",
                    "user_host": "'root'@'localhost'",
                }
            ],
        )

    @patch("MySQLdb.connect")
    @patch.object(MysqlEngine, "query")
    def test_get_instance_users_summary_without_lock(self, _query, _connect):
        result_without_lock = ResultSet()
        result_without_lock.rows = [("'root'@'localhost'", "root", "localhost")]
        _query.return_value = result_without_lock
        mysql_engine = MysqlEngine(instance=self.ins1)
        _connect.return_value.get_server_info.return_value = "5.7.5-16log"
        self.assertTupleEqual(mysql_engine.server_version, (5, 7, 5))
        user_summary_without_lock = mysql_engine.get_instance_users_summary()
        self.assertEqual(
            user_summary_without_lock.rows,
            [
                {
                    "host": "localhost",
                    "is_locked": None,
                    "privileges": [("'root'@'localhost'", "root", "localhost")],
                    "saved": False,
                    "user": "root",
                    "user_host": "'root'@'localhost'",
                }
            ],
        )

    @patch("MySQLdb.connect")
    @patch.object(MysqlEngine, "execute")
    def test_create_instance_user(self, _execute, _connect):
        _execute.return_value = ResultSet()
        new_engine = MysqlEngine(instance=self.ins1)
        result = new_engine.create_instance_user(
            user="some_user", host="%", password1="123456", remark=""
        )
        self.assertEqual(
            result.rows,
            [
                {
                    "instance": self.ins1,
                    "user": "some_user",
                    "host": "%",
                    "password": "123456",
                    "remark": "",
                }
            ],
        )
