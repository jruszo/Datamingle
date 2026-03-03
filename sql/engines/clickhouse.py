# -*- coding: UTF-8 -*-
from clickhouse_driver import connect
from clickhouse_driver.util.escape import escape_chars_map
from sql.utils.sql_utils import get_syntax_type
from .models import ResultSet, ReviewResult, ReviewSet
from common.utils.timer import FuncTimer
from common.config import SysConfig
from . import EngineBase
import sqlparse
import logging
import re

logger = logging.getLogger("default")


class ClickHouseEngine(EngineBase):
    test_query = "SELECT 1"

    def __init__(self, instance=None):
        super(ClickHouseEngine, self).__init__(instance=instance)
        self.config = SysConfig()

    def get_connection(self, db_name=None):
        if self.conn:
            return self.conn
        if db_name:
            self.conn = connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=db_name,
                connect_timeout=10,
            )
        else:
            self.conn = connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                connect_timeout=10,
            )
        return self.conn

    name = "ClickHouse"
    info = "ClickHouse engine"

    def escape_string(self, value: str) -> str:
        """Escape string parameters."""
        return "%s" % "".join(escape_chars_map.get(c, c) for c in value)

    @property
    def auto_backup(self):
        """Whether backup is supported."""
        return False

    @property
    def server_version(self):
        sql = "select value from system.build_options where name = 'VERSION_FULL';"
        result = self.query(sql=sql)
        version = result.rows[0][0].split(" ")[1]
        return tuple([int(n) for n in version.split(".")[:3]])

    def get_table_engine(self, tb_name):
        """Get engine type of a table."""
        db, tb = tb_name.split(".")
        sql = f"""select engine from system.tables where database=%(db)s and name=%(tb)s"""
        query_result = self.query(sql=sql, parameters={"db": db, "tb": tb})
        if query_result.rows:
            result = {"status": 1, "engine": query_result.rows[0][0]}
        else:
            result = {"status": 0, "engine": "None"}
        return result

    def get_all_databases(self):
        """Get database list and return a ResultSet."""
        sql = "show databases"
        result = self.query(sql=sql)
        db_list = [
            row[0]
            for row in result.rows
            if row[0]
            not in ("system", "INFORMATION_SCHEMA", "information_schema", "datasets")
        ]
        result.rows = db_list
        return result

    def get_all_tables(self, db_name, **kwargs):
        """Get table list and return a ResultSet."""
        sql = "show tables"
        result = self.query(db_name=db_name, sql=sql)
        tb_list = [row[0] for row in result.rows]
        result.rows = tb_list
        return result

    def get_all_columns_by_tb(self, db_name, tb_name, **kwargs):
        """Get all columns and return a ResultSet."""
        sql = f"""select
            name,
            type,
            comment
        from
            system.columns
        where
            database = %(db_name)s
        and table = %(tb_name)s;"""
        result = self.query(
            db_name=db_name,
            sql=sql,
            parameters={"db_name": db_name, "tb_name": tb_name},
        )
        column_list = [row[0] for row in result.rows]
        result.rows = column_list
        return result

    def describe_table(self, db_name, tb_name, **kwargs):
        """Return ResultSet, similar to a query."""
        tb_name = self.escape_string(tb_name)
        sql = f"show create table `{tb_name}`;"
        result = self.query(db_name=db_name, sql=sql)

        result.rows[0] = (tb_name,) + (
            result.rows[0][0].replace("(", "(\n ").replace(",", ",\n "),
        )
        return result

    def query(
        self,
        db_name=None,
        sql="",
        limit_num=0,
        close_conn=True,
        parameters=None,
        **kwargs,
    ):
        """Return a ResultSet."""
        result_set = ResultSet(full_sql=sql)
        try:
            conn = self.get_connection(db_name=db_name)
            cursor = conn.cursor()
            cursor.execute(sql, parameters)
            if int(limit_num) > 0:
                rows = cursor.fetchmany(size=int(limit_num))
            else:
                rows = cursor.fetchall()
            fields = cursor.description

            result_set.column_list = [i[0] for i in fields] if fields else []
            result_set.rows = rows
            result_set.affected_rows = len(rows)
        except Exception as e:
            logger.warning(
                f"ClickHouse statement execution failed, sql: {sql}, error: {e}"
            )
            result_set.error = str(e).split("Stack trace")[0]
        finally:
            if close_conn:
                self.close()
        return result_set

    def query_check(self, db_name=None, sql=""):
        # Check query statement, strip comments, and split.
        result = {"msg": "", "bad_query": False, "filtered_sql": sql, "has_star": False}
        # Remove comments, validate syntax, and keep the first valid SQL.
        try:
            sql = sqlparse.format(sql, strip_comments=True)
            sql = sqlparse.split(sql)[0]
            result["filtered_sql"] = sql.strip()
        except IndexError:
            result["bad_query"] = True
            result["msg"] = "No valid SQL statement found"
        if re.match(r"^select|^show|^explain|^with", sql, re.I) is None:
            result["bad_query"] = True
            result["msg"] = "Unsupported query syntax type!"
        if "*" in sql:
            result["has_star"] = True
            result["msg"] = "SQL statement contains * "
        # ClickHouse starts officially supporting EXPLAIN from 20.6.3.
        if re.match(r"^explain", sql, re.I) and self.server_version < (20, 6, 3):
            result["bad_query"] = True
            result["msg"] = (
                "Current ClickHouse instance version is below 20.6.3; EXPLAIN is not supported!"
            )
        # Use EXPLAIN first to verify SELECT syntax.
        if re.match(r"^select", sql, re.I) and self.server_version >= (20, 6, 3):
            explain_result = self.query(db_name=db_name, sql=f"explain {sql}")
            if explain_result.error:
                result["bad_query"] = True
                result["msg"] = explain_result.error

        return result

    def filter_sql(self, sql="", limit_num=0):
        # Add LIMIT for query SQL; normalize all limit forms to limit n.
        sql = sql.rstrip(";").strip()
        if re.match(r"^select", sql, re.I):
            # LIMIT N
            limit_n = re.compile(r"limit\s+(\d+)\s*$", re.I)
            # LIMIT M OFFSET N
            limit_offset = re.compile(r"limit\s+(\d+)\s+offset\s+(\d+)\s*$", re.I)
            # LIMIT M,N
            offset_comma_limit = re.compile(r"limit\s+(\d+)\s*,\s*(\d+)\s*$", re.I)
            if limit_n.search(sql):
                sql_limit = limit_n.search(sql).group(1)
                limit_num = min(int(limit_num), int(sql_limit))
                sql = limit_n.sub(f"limit {limit_num};", sql)
            elif limit_offset.search(sql):
                sql_limit = limit_offset.search(sql).group(1)
                sql_offset = limit_offset.search(sql).group(2)
                limit_num = min(int(limit_num), int(sql_limit))
                sql = limit_offset.sub(f"limit {limit_num} offset {sql_offset};", sql)
            elif offset_comma_limit.search(sql):
                sql_offset = offset_comma_limit.search(sql).group(1)
                sql_limit = offset_comma_limit.search(sql).group(2)
                limit_num = min(int(limit_num), int(sql_limit))
                sql = offset_comma_limit.sub(f"limit {sql_offset},{limit_num};", sql)
            else:
                sql = f"{sql} limit {limit_num};"
        else:
            sql = f"{sql};"
        return sql

    def explain_check(self, check_result, db_name=None, line=0, statement=""):
        """Check SQL syntax via EXPLAIN AST and return ReviewSet row."""
        result = ReviewResult(
            id=line,
            errlevel=0,
            stagestatus="Audit completed",
            errormessage="None",
            sql=statement,
            affected_rows=0,
            execute_time=0,
        )
        # EXPLAIN AST supports non-SELECT checks from ClickHouse 21.1.2+.
        if self.server_version >= (21, 1, 2):
            explain_result = self.query(db_name=db_name, sql=f"explain ast {statement}")
            if explain_result.error:
                result = ReviewResult(
                    id=line,
                    errlevel=2,
                    stagestatus="Rejected SQL failed check",
                    errormessage=f"EXPLAIN syntax check failed: {explain_result.error}",
                    sql=statement,
                )
        return result

    def execute_check(self, db_name=None, sql=""):
        """Run checks before workflow execution and return a ReviewSet."""
        sql = sqlparse.format(sql, strip_comments=True)
        sql_list = sqlparse.split(sql)

        # Check disabled/high-risk statements.
        check_result = ReviewSet(full_sql=sql)
        line = 1
        critical_ddl_regex = self.config.get("critical_ddl_regex", "")
        p = re.compile(critical_ddl_regex)
        check_result.syntax_type = 2  # TODO Workflow type: 0 others, 1 DDL, 2 DML

        for statement in sql_list:
            statement = statement.rstrip(";")
            # Disabled statements.
            if re.match(r"^select|^show", statement, re.M | re.IGNORECASE):
                result = ReviewResult(
                    id=line,
                    errlevel=2,
                    stagestatus="Rejected unsupported statement",
                    errormessage=(
                        "Only DML and DDL statements are supported. "
                        "Use SQL query feature for SELECT statements."
                    ),
                    sql=statement,
                )
            # High-risk statements.
            elif critical_ddl_regex and p.match(statement.strip().lower()):
                result = ReviewResult(
                    id=line,
                    errlevel=2,
                    stagestatus="Rejected high-risk SQL",
                    errormessage=(
                        "Submitting statements that match "
                        + critical_ddl_regex
                        + " is prohibited!"
                    ),
                    sql=statement,
                )
            # ALTER statements.
            elif re.match(r"^alter", statement, re.M | re.IGNORECASE):
                # ALTER TABLE statements.
                if re.match(
                    r"^alter\s+table\s+(.+?)\s+", statement, re.M | re.IGNORECASE
                ):
                    table_name = re.match(
                        r"^alter\s+table\s+(.+?)\s+", statement, re.M | re.IGNORECASE
                    ).group(1)
                    if "." not in table_name:
                        table_name = f"{db_name}.{table_name}"
                    table_engine = self.get_table_engine(table_name)["engine"]
                    table_exist = self.get_table_engine(table_name)["status"]
                    if table_exist == 1:
                        if not table_engine.endswith(
                            "MergeTree"
                        ) and table_engine not in ("Merge", "Distributed"):
                            result = ReviewResult(
                                id=line,
                                errlevel=2,
                                stagestatus="Rejected unsupported SQL",
                                errormessage=(
                                    "ALTER TABLE only supports *MergeTree, Merge and "
                                    "Distributed table engines!"
                                ),
                                sql=statement,
                            )
                        else:
                            # DELETE/UPDATE are variants of ALTER.
                            if re.match(
                                r"^alter\s+table\s+(.+?)\s+(delete|update)\s+",
                                statement,
                                re.M | re.IGNORECASE,
                            ):
                                if not table_engine.endswith("MergeTree"):
                                    result = ReviewResult(
                                        id=line,
                                        errlevel=2,
                                        stagestatus="Rejected unsupported SQL",
                                        errormessage=(
                                            "DELETE and UPDATE only support "
                                            "*MergeTree table engines!"
                                        ),
                                        sql=statement,
                                    )
                                else:
                                    result = self.explain_check(
                                        check_result, db_name, line, statement
                                    )
                            else:
                                result = self.explain_check(
                                    check_result, db_name, line, statement
                                )
                    else:
                        result = ReviewResult(
                            id=line,
                            errlevel=2,
                            stagestatus="Table not found",
                            errormessage=f"Table {table_name} does not exist!",
                            sql=statement,
                        )
                # Other ALTER statements.
                else:
                    result = self.explain_check(check_result, db_name, line, statement)
            # TRUNCATE statements.
            elif re.match(
                r"^truncate\s+table\s+(.+?)(\s|$)", statement, re.M | re.IGNORECASE
            ):
                table_name = re.match(
                    r"^truncate\s+table\s+(.+?)(\s|$)", statement, re.M | re.IGNORECASE
                ).group(1)
                if "." not in table_name:
                    table_name = f"{db_name}.{table_name}"
                table_engine = self.get_table_engine(table_name)["engine"]
                table_exist = self.get_table_engine(table_name)["status"]
                if table_exist == 1:
                    if table_engine in ("View", "File", "URL", "Buffer", "Null"):
                        result = ReviewResult(
                            id=line,
                            errlevel=2,
                            stagestatus="Rejected unsupported SQL",
                            errormessage=(
                                "TRUNCATE does not support View, File, URL, Buffer, "
                                "or Null table engines!"
                            ),
                            sql=statement,
                        )
                    else:
                        result = self.explain_check(
                            check_result, db_name, line, statement
                        )
                else:
                    result = ReviewResult(
                        id=line,
                        errlevel=2,
                        stagestatus="Table not found",
                        errormessage=f"Table {table_name} does not exist!",
                        sql=statement,
                    )
            # INSERT statements: EXPLAIN cannot fully validate them.
            elif re.match(r"^insert", statement, re.M | re.IGNORECASE):
                if re.match(
                    r"^insert\s+into\s+([a-zA-Z_][0-9a-zA-Z_.]+)([\w\W]*?)(values|format|select)(\s+|\()",
                    statement,
                    re.M | re.IGNORECASE,
                ):
                    table_name = re.match(
                        r"^insert\s+into\s+([a-zA-Z_][0-9a-zA-Z_.]+)([\w\W]*?)(values|format|select)(\s+|\()",
                        statement,
                        re.M | re.IGNORECASE,
                    ).group(1)
                    if "." not in table_name:
                        table_name = f"{db_name}.{table_name}"
                    table_exist = self.get_table_engine(table_name)["status"]
                    if table_exist == 1:
                        result = ReviewResult(
                            id=line,
                            errlevel=0,
                            stagestatus="Audit completed",
                            errormessage="None",
                            sql=statement,
                            affected_rows=0,
                            execute_time=0,
                        )
                    else:
                        result = ReviewResult(
                            id=line,
                            errlevel=2,
                            stagestatus="Table not found",
                            errormessage=f"Table {table_name} does not exist!",
                            sql=statement,
                        )
                else:
                    result = ReviewResult(
                        id=line,
                        errlevel=2,
                        stagestatus="Rejected unsupported SQL",
                        errormessage="Invalid INSERT syntax!",
                        sql=statement,
                    )
            # Other statements use simple EXPLAIN AST check.
            else:
                result = self.explain_check(check_result, db_name, line, statement)

            # Keep checking syntax type if DDL not identified yet.
            if check_result.syntax_type == 2:
                if get_syntax_type(statement, parser=False, db_type="mysql") == "DDL":
                    check_result.syntax_type = 1
            check_result.rows += [result]
            line += 1
        # Count warnings and errors.
        for r in check_result.rows:
            if r.errlevel == 1:
                check_result.warning_count += 1
            if r.errlevel == 2:
                check_result.error_count += 1
        return check_result

    def execute_workflow(self, workflow):
        """Execute workflow and return a ReviewSet."""
        sql = workflow.sqlworkflowcontent.sql_content
        execute_result = ReviewSet(full_sql=sql)
        sqls = sqlparse.format(sql, strip_comments=True)
        sql_list = sqlparse.split(sqls)

        line = 1
        for statement in sql_list:
            with FuncTimer() as t:
                result = self.execute(
                    db_name=workflow.db_name, sql=statement, close_conn=True
                )
            if not result.error:
                execute_result.rows.append(
                    ReviewResult(
                        id=line,
                        errlevel=0,
                        stagestatus="Execute Successfully",
                        errormessage="None",
                        sql=statement,
                        affected_rows=0,
                        execute_time=t.cost,
                    )
                )
                line += 1
            else:
                # Append current failed statement to execution result.
                execute_result.error = result.error
                execute_result.rows.append(
                    ReviewResult(
                        id=line,
                        errlevel=2,
                        stagestatus="Execute Failed",
                        errormessage=f"Exception: {result.error}",
                        sql=statement,
                        affected_rows=0,
                        execute_time=0,
                    )
                )
                line += 1
                # Append remaining statements as skipped due to previous failure.
                for statement in sql_list[line - 1 :]:
                    execute_result.rows.append(
                        ReviewResult(
                            id=line,
                            errlevel=0,
                            stagestatus="Audit completed",
                            errormessage="Previous statement failed, not executed",
                            sql=statement,
                            affected_rows=0,
                            execute_time=0,
                        )
                    )
                    line += 1
                break
        return execute_result

    def execute(self, db_name=None, sql="", close_conn=True, parameters=None):
        """Execute raw statement."""
        result = ResultSet(full_sql=sql)
        conn = self.get_connection(db_name=db_name)
        try:
            cursor = conn.cursor()
            for statement in sqlparse.split(sql):
                cursor.execute(statement, parameters)
            cursor.close()
        except Exception as e:
            logger.warning(
                f"ClickHouse statement execution failed, sql: {sql}, error: {e}"
            )
            result.error = str(e).split("Stack trace")[0]
        if close_conn:
            self.close()
        return result

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
