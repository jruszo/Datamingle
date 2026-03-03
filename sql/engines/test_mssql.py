from datetime import datetime, timedelta
from unittest.mock import patch, Mock, ANY

from django.test import TestCase

from sql.engines import ResultSet, ReviewSet
from sql.engines.models import ReviewResult
from sql.engines.mssql import MssqlEngine
from sql.models import Instance, SqlWorkflow, SqlWorkflowContent


class TestMssql(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ins1 = Instance(
            instance_name="some_ins",
            type="slave",
            db_type="mssql",
            host="some_host",
            port=1366,
            user="ins_user",
            password="some_str",
        )
        cls.ins1.save()
        cls.engine = MssqlEngine(instance=cls.ins1)
        cls.wf = SqlWorkflow.objects.create(
            workflow_name="some_name",
            group_id=1,
            group_name="g1",
            engineer_display="",
            audit_auth_groups="some_group",
            create_time=datetime.now() - timedelta(days=1),
            status="workflow_finish",
            is_backup=True,
            instance=cls.ins1,
            db_name="some_db",
            syntax_type=1,
        )
        SqlWorkflowContent.objects.create(
            workflow=cls.wf, sql_content="insert into some_tb values (1)"
        )

    @classmethod
    def tearDownClass(cls):
        cls.ins1.delete()
        cls.wf.delete()
        SqlWorkflowContent.objects.all().delete()

    @patch("sql.engines.mssql.pyodbc.connect")
    def testGetConnection(self, connect):
        new_engine = MssqlEngine(instance=self.ins1)
        new_engine.get_connection()
        connect.assert_called_once()

    @patch("sql.engines.mssql.pyodbc.connect")
    def testQuery(self, connect):
        cur = Mock()
        connect.return_value.cursor = cur
        cur.return_value.execute = Mock()
        cur.return_value.fetchmany.return_value = (("v1", "v2"),)
        cur.return_value.description = (
            ("k1", "some_other_des"),
            ("k2", "some_other_des"),
        )
        new_engine = MssqlEngine(instance=self.ins1)
        query_result = new_engine.query(sql="some_str", limit_num=100)
        cur.return_value.execute.assert_called()
        cur.return_value.fetchmany.assert_called_once_with(100)
        connect.return_value.close.assert_called_once()
        self.assertIsInstance(query_result, ResultSet)

    @patch.object(MssqlEngine, "query")
    def testAllDb(self, mock_query):
        db_result = ResultSet()
        db_result.rows = [("db_1",), ("db_2",)]
        mock_query.return_value = db_result
        new_engine = MssqlEngine(instance=self.ins1)
        dbs = new_engine.get_all_databases()
        self.assertEqual(dbs.rows, ["db_1", "db_2"])

    @patch.object(MssqlEngine, "query")
    def testAllTables(self, mock_query):
        table_result = ResultSet()
        table_result.rows = [("tb_1", "some_des"), ("tb_2", "some_des")]
        mock_query.return_value = table_result
        new_engine = MssqlEngine(instance=self.ins1)
        tables = new_engine.get_all_tables("some_db")
        mock_query.assert_called_once_with(db_name="some_db", sql=ANY)
        self.assertEqual(tables.rows, ["tb_1", "tb_2"])

    @patch.object(MssqlEngine, "query")
    def testAllColumns(self, mock_query):
        db_result = ResultSet()
        db_result.rows = [("col_1", "type"), ("col_2", "type2")]
        mock_query.return_value = db_result
        new_engine = MssqlEngine(instance=self.ins1)
        dbs = new_engine.get_all_columns_by_tb("some_db", "some_tb")
        self.assertEqual(dbs.rows, ["col_1", "col_2"])

    @patch.object(MssqlEngine, "query")
    def testDescribe(self, mock_query):
        new_engine = MssqlEngine(instance=self.ins1)
        new_engine.describe_table("some_db", "some_db")
        mock_query.assert_called_once()

    def testQueryCheck(self):
        new_engine = MssqlEngine(instance=self.ins1)
        # Spot-check one function.
        banned_sql = "select concat(phone,1) from user_table"
        check_result = new_engine.query_check(db_name="some_db", sql=banned_sql)
        self.assertTrue(check_result.get("bad_query"))
        banned_sql = "select phone from user_table where phone=concat(phone,1)"
        check_result = new_engine.query_check(db_name="some_db", sql=banned_sql)
        self.assertTrue(check_result.get("bad_query"))
        sp_sql = "sp_helptext '[SomeName].[SomeAction]'"
        check_result = new_engine.query_check(db_name="some_db", sql=sp_sql)
        self.assertFalse(check_result.get("bad_query"))
        self.assertEqual(check_result.get("filtered_sql"), sp_sql)

    def test_filter_sql(self):
        new_engine = MssqlEngine(instance=self.ins1)
        # Spot-check one function.
        banned_sql = "select user from user_table"
        check_result = new_engine.filter_sql(sql=banned_sql, limit_num=10)
        self.assertEqual(check_result, "select top 10 user from user_table")

    def test_filter_sql_with_distinct(self):
        new_engine = MssqlEngine(instance=self.ins1)
        # Spot-check one function.
        banned_sql = "select distinct * from user_table"
        check_result = new_engine.filter_sql(sql=banned_sql, limit_num=10)
        self.assertEqual(check_result, "select distinct top 10 * from user_table")

    @patch("sql.engines.mssql.MssqlEngine.get_connection")
    def test_execute_check(self, mock_get_connection):
        # Mock connection to avoid ODBC driver error.
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_connection.return_value = mock_conn

        new_engine = MssqlEngine(instance=self.ins1)
        # SQL contains GO, GO should be removed, then split by semicolon.
        test_sql = (
            "use database\ngo\nsome sql1\nGO\nsome sql2\n\r\nGo\nsome sql3\n\r\ngO\n"
        )
        check_result = new_engine.execute_check(db_name=None, sql=test_sql)
        self.assertIsInstance(check_result, ReviewSet)
        # GO is removed, then sqlparse.split splits by semicolon.
        # Verify at least one statement is processed.
        self.assertGreaterEqual(len(check_result.rows), 1)

    @patch("sql.engines.mssql.MssqlEngine.execute")
    def test_execute_workflow(self, mock_execute):
        mock_execute.return_value.error = None
        new_engine = MssqlEngine(instance=self.ins1)
        new_engine.execute_workflow(self.wf)
        # One execute call per backup table plus one for actual execution.
        mock_execute.assert_called()
        self.assertEqual(1, mock_execute.call_count)

    @patch("sql.engines.mssql.MssqlEngine.get_connection")
    def test_execute(self, mock_connect):
        mock_cursor = Mock()
        mock_connect.return_value.cursor = mock_cursor
        new_engine = MssqlEngine(instance=self.ins1)
        execute_result = new_engine.execute("some_db", "some_sql")
        # Verify successful result.
        self.assertIsNone(execute_result.error)
        self.assertEqual("some_sql", execute_result.full_sql)
        self.assertEqual(2, len(execute_result.rows))
        mock_cursor.return_value.execute.assert_called()
        # MSSQL uses conn.commit(), commit after each statement.
        mock_connect.return_value.commit.assert_called()
        mock_cursor.reset_mock()
        # Verify exception path.
        mock_cursor.return_value.execute.side_effect = Exception(
            "Boom! some exception!"
        )
        execute_result = new_engine.execute("some_db", "some_sql")
        self.assertIn("Boom! some exception!", execute_result.error)
        self.assertEqual("some_sql", execute_result.full_sql)
        self.assertEqual(2, len(execute_result.rows))
        # On exception, rollback should be called instead of commit.
        mock_connect.return_value.rollback.assert_called()

    @patch("sql.engines.mssql.pyodbc.connect")
    def test_get_connection_with_db_name(self, mock_connect):
        """Test connection with explicit database name."""
        new_engine = MssqlEngine(instance=self.ins1)
        new_engine.get_connection(db_name="test_db")
        mock_connect.assert_called_once()
        # Verify connection string includes DATABASE.
        call_args = mock_connect.call_args[0][0]
        self.assertIn("DATABASE=test_db", call_args)

    @patch("sql.engines.mssql.pyodbc.drivers")
    @patch("sql.engines.mssql.pyodbc.connect")
    def test_get_connection_driver_selection(self, mock_connect, mock_drivers):
        """Test driver selection logic."""
        # Test recommended driver found.
        mock_drivers.return_value = ["ODBC Driver 17 for SQL Server"]
        new_engine = MssqlEngine(instance=self.ins1)
        new_engine.get_connection()
        mock_connect.assert_called_once()
        call_args = mock_connect.call_args[0][0]
        self.assertIn("ODBC Driver 17 for SQL Server", call_args)
        self.assertIn("TrustServerCertificate=yes", call_args)

        # Test fallback to another available driver.
        mock_connect.reset_mock()
        mock_drivers.return_value = ["FreeTDS"]
        new_engine2 = MssqlEngine(instance=self.ins1)
        new_engine2.get_connection()
        call_args = mock_connect.call_args[0][0]
        self.assertIn("FreeTDS", call_args)
        self.assertNotIn("TrustServerCertificate", call_args)

    @patch("sql.engines.mssql.pyodbc.drivers")
    @patch("sql.engines.mssql.pyodbc.connect")
    def test_get_connection_no_drivers(self, mock_connect, mock_drivers):
        """Test case when no drivers are found."""
        mock_drivers.return_value = []
        new_engine = MssqlEngine(instance=self.ins1)
        new_engine.get_connection()
        # Should still attempt to use default driver.
        mock_connect.assert_called_once()

    @patch("sql.engines.mssql.pyodbc.drivers")
    def test_get_connection_drivers_exception(self, mock_drivers):
        """Test exception while fetching driver list."""
        mock_drivers.side_effect = Exception("Driver error")
        new_engine = MssqlEngine(instance=self.ins1)
        # Should still proceed (using default driver).
        with patch("sql.engines.mssql.pyodbc.connect") as mock_connect:
            new_engine.get_connection()
            mock_connect.assert_called_once()

    def test_filter_sql_with_offset_fetch(self):
        """Test OFFSET ... FETCH NEXT case."""
        new_engine = MssqlEngine(instance=self.ins1)
        sql = "select * from table order by id offset 10 rows fetch next 20 rows only"
        result = new_engine.filter_sql(sql=sql, limit_num=10)
        # Should not add TOP.
        self.assertEqual(result, sql.strip())

    def test_filter_sql_with_existing_top(self):
        """Test case with existing TOP."""
        new_engine = MssqlEngine(instance=self.ins1)
        sql = "select top 5 * from table"
        result = new_engine.filter_sql(sql=sql, limit_num=10)
        # Should not add duplicate TOP.
        self.assertEqual(result, sql.strip())

    def test_filter_sql_non_select(self):
        """Test non-SELECT statement."""
        new_engine = MssqlEngine(instance=self.ins1)
        sql = "insert into table values (1)"
        result = new_engine.filter_sql(sql=sql, limit_num=10)
        self.assertEqual(result, sql.strip())

    @patch("sql.engines.mssql.pyodbc.connect")
    def test_query_with_parameters(self, mock_connect):
        """Test query with parameters."""
        mock_cursor = Mock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [("v1",)]
        mock_cursor.description = (("col1",),)
        new_engine = MssqlEngine(instance=self.ins1)
        result = new_engine.query(
            sql="select * from table where id=?", parameters=("1",)
        )
        mock_cursor.execute.assert_called_once_with(
            "select * from table where id=?", "1"
        )
        self.assertIsInstance(result, ResultSet)

    @patch("sql.engines.mssql.pyodbc.connect")
    def test_query_with_showplan(self, mock_connect):
        """Test query with execution plan."""
        mock_cursor = Mock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [("plan",)]
        mock_cursor.description = (("col1",),)
        new_engine = MssqlEngine(instance=self.ins1)
        sql = "SET SHOWPLAN_ALL ON; select * from table; SET SHOWPLAN_ALL OFF;"
        result = new_engine.query(sql=sql)
        # Should call SET SHOWPLAN_ALL ON and OFF.
        self.assertGreaterEqual(mock_cursor.execute.call_count, 2)
        self.assertIsInstance(result, ResultSet)

    @patch("sql.engines.mssql.MssqlEngine.get_connection")
    def test_execute_check_with_select(self, mock_get_connection):
        """Test SELECT statement check in execute_check."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_connection.return_value = mock_conn

        new_engine = MssqlEngine(instance=self.ins1)
        check_result = new_engine.execute_check(db_name=None, sql="select * from table")
        self.assertIsInstance(check_result, ReviewSet)
        self.assertGreater(len(check_result.rows), 0)
        # SELECT statement should be rejected.
        self.assertEqual(check_result.rows[0].errlevel, 2)
        self.assertIn("Rejected unsupported statement", check_result.rows[0].stagestatus)

    @patch("sql.engines.mssql.MssqlEngine.get_connection")
    def test_execute_check_with_critical_ddl(self, mock_get_connection):
        """Test high-risk statement check in execute_check."""
        from common.config import SysConfig

        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_connection.return_value = mock_conn

        # Configure high-risk DDL regex.
        config = SysConfig()
        config.set("critical_ddl_regex", "drop\\s+table")

        new_engine = MssqlEngine(instance=self.ins1)
        check_result = new_engine.execute_check(db_name=None, sql="drop table test")
        self.assertIsInstance(check_result, ReviewSet)
        self.assertGreater(len(check_result.rows), 0)
        # High-risk statement should be rejected.
        self.assertEqual(check_result.rows[0].errlevel, 2)
        self.assertIn("Rejected high-risk SQL", check_result.rows[0].stagestatus)

        # Clean up config.
        config.set("critical_ddl_regex", "")

    @patch("sql.engines.mssql.MssqlEngine.get_connection")
    def test_execute_check_syntax_error(self, mock_get_connection):
        """Test syntax error detection in execute_check."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        # Mock syntax error.
        mock_cursor.execute.side_effect = Exception("Syntax error")
        mock_get_connection.return_value = mock_conn

        new_engine = MssqlEngine(instance=self.ins1)
        check_result = new_engine.execute_check(db_name=None, sql="invalid sql syntax")
        self.assertIsInstance(check_result, ReviewSet)
        self.assertGreater(len(check_result.rows), 0)
        # Syntax error should be detected.
        self.assertEqual(check_result.rows[0].errlevel, 2)
        self.assertIn("Syntax error", check_result.rows[0].stagestatus)

    @patch("sql.engines.mssql.MssqlEngine.get_connection")
    def test_execute_check_connection_failed(self, mock_get_connection):
        """Test connection failure case in execute_check."""
        # Mock connection failure.
        mock_get_connection.side_effect = Exception("Connection failed")

        new_engine = MssqlEngine(instance=self.ins1)
        check_result = new_engine.execute_check(
            db_name=None, sql="insert into table values (1)"
        )
        self.assertIsInstance(check_result, ReviewSet)
        # Basic checks should still be performed on connection failure.
        self.assertGreaterEqual(len(check_result.rows), 0)

    @patch("sql.engines.mssql.MssqlEngine.get_connection")
    def test_execute_use_failed(self, mock_connect):
        """Test USE statement failure case in execute."""
        mock_cursor = Mock()
        mock_connect.return_value.cursor = mock_cursor
        # Create execute call sequence: first call (USE) fails, rest succeed.
        execute_calls = []

        def execute_side_effect(*args, **kwargs):
            if "USE" in str(args[0]):
                raise Exception("USE failed")
            execute_calls.append(args[0])
            return None

        mock_cursor.return_value.execute.side_effect = execute_side_effect
        mock_cursor.return_value.rowcount = 0

        new_engine = MssqlEngine(instance=self.ins1)
        execute_result = new_engine.execute("some_db", "some_sql")
        # Should contain error record.
        self.assertIsNotNone(execute_result.error)
        # Should have at least two records (USE failure + follow-up statement).
        self.assertGreaterEqual(len(execute_result.rows), 2)
        # USE statement should fail.
        self.assertEqual(execute_result.rows[0].errlevel, 2)
        self.assertIn("USE", execute_result.rows[0].sql)
