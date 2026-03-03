# -*- coding: UTF-8 -*-
"""
@author: hhyo、yyukai
@license: Apache Licence
@file: pgsql.py
@time: 2019/03/29
"""

import json
import re
import psycopg2
import logging
import traceback
import sqlparse

from common.config import SysConfig
from common.utils.timer import FuncTimer
from sql.utils.sql_utils import get_syntax_type
from . import EngineBase
from .models import ResultSet, ReviewSet, ReviewResult
from sql.utils.data_masking import simple_column_mask

__author__ = "hhyo、yyukai"

logger = logging.getLogger("default")


class PgSQLEngine(EngineBase):
    test_query = "SELECT 1"

    def get_connection(self, db_name=None):
        db_name = db_name or self.db_name or "postgres"
        if self.conn:
            return self.conn
        self.conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            client_encoding=self.instance.charset,
            dbname=db_name,
            connect_timeout=10,
        )
        return self.conn

    name = "PgSQL"

    info = "PgSQL engine"

    def get_all_databases(self):
        """
        Get database list.
        :return:
        """
        result = self.query(sql=f"SELECT datname FROM pg_database;")
        db_list = [
            row[0] for row in result.rows if row[0] not in ["template0", "template1"]
        ]
        result.rows = db_list
        return result

    def get_all_schemas(self, db_name, **kwargs):
        """
        Get schema list.
        :return:
        """
        result = self.query(
            db_name=db_name, sql=f"select schema_name from information_schema.schemata;"
        )
        schema_list = [
            row[0]
            for row in result.rows
            if row[0]
            not in [
                "information_schema",
                "pg_catalog",
                "pg_toast_temp_1",
                "pg_temp_1",
                "pg_toast",
            ]
        ]
        result.rows = schema_list
        return result

    def get_all_tables(self, db_name, **kwargs):
        """
        Get table list.
        :param db_name:
        :param schema_name:
        :return:
        """
        schema_name = kwargs.get("schema_name")
        sql = f"""SELECT table_name
        FROM information_schema.tables
        where table_schema =%(schema_name)s;"""
        result = self.query(
            db_name=db_name, sql=sql, parameters={"schema_name": schema_name}
        )
        tb_list = [row[0] for row in result.rows if row[0] not in ["test"]]
        result.rows = tb_list
        return result

    def get_all_columns_by_tb(self, db_name, tb_name, **kwargs):
        """
        Get column list.
        :param db_name:
        :param tb_name:
        :param schema_name:
        :return:
        """
        schema_name = kwargs.get("schema_name")
        sql = f"""SELECT column_name
        FROM information_schema.columns
        where table_name=%(tb_name)s
        and table_schema=%(schema_name)s;"""
        result = self.query(
            db_name=db_name,
            sql=sql,
            parameters={"schema_name": schema_name, "tb_name": tb_name},
        )
        column_list = [row[0] for row in result.rows]
        result.rows = column_list
        return result

    def describe_table(self, db_name, tb_name, **kwargs):
        """
        Get table schema information.
        :param db_name:
        :param tb_name:
        :param schema_name:
        :return:
        """
        schema_name = kwargs.get("schema_name")
        sql = f"""select
        col.column_name,
        col.data_type,
        col.character_maximum_length,
        col.numeric_precision,
        col.numeric_scale,
        col.is_nullable,
        col.column_default,
        des.description
        from
        information_schema.columns col left join pg_description des on
        col.table_name::regclass = des.objoid
        and col.ordinal_position = des.objsubid
        where table_name = %(tb_name)s
        and col.table_schema = %(schema_name)s
        order by ordinal_position;"""
        result = self.query(
            db_name=db_name,
            schema_name=schema_name,
            sql=sql,
            parameters={"schema_name": schema_name, "tb_name": tb_name},
        )
        return result

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
        if re.match(r"^select|^explain", sql, re.I) is None:
            result["bad_query"] = True
            result["msg"] = "Unsupported query syntax type!"
        if "*" in sql:
            result["has_star"] = True
            result["msg"] += "SQL statement contains * "
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
        schema_name = kwargs.get("schema_name")
        result_set = ResultSet(full_sql=sql)
        try:
            conn = self.get_connection(db_name=db_name)
            conn.autocommit = False
            max_execution_time = kwargs.get("max_execution_time", 0)
            cursor = conn.cursor()
            try:
                cursor.execute(f"SET statement_timeout TO {max_execution_time};")
            except:
                pass
            cursor.execute("SET transaction ISOLATION LEVEL READ COMMITTED READ ONLY;")
            if schema_name:
                cursor.execute(
                    f"SET search_path TO %(schema_name)s;", {"schema_name": schema_name}
                )
            cursor.execute(sql, parameters)
            # effect_row = cursor.rowcount
            if int(limit_num) > 0:
                rows = cursor.fetchmany(size=int(limit_num))
            else:
                rows = cursor.fetchall()
            conn.commit()
            fields = cursor.description
            column_type_codes = [i[1] for i in fields] if fields else []
            # JSON/JSONB type codes: 114 for json, 3802 for jsonb.
            JSON_TYPE_CODE = 114
            JSONB_TYPE_CODE = 3802
            # Iterate rows and convert json/jsonb columns where needed.
            converted_rows = []
            for row in rows:
                new_row = []
                for idx, col_value in enumerate(row):
                    # Index should not go out of range.
                    column_type_code = (
                        column_type_codes[idx] if idx < len(column_type_codes) else None
                    )
                    # Convert only for json/jsonb columns.
                    if column_type_code in [JSON_TYPE_CODE, JSONB_TYPE_CODE]:
                        if isinstance(col_value, (dict, list)):
                            new_row.append(
                                json.dumps(col_value, ensure_ascii=False)
                            )  # Convert to JSON string.
                        else:
                            new_row.append(col_value)
                    else:
                        new_row.append(col_value)
                converted_rows.append(tuple(new_row))

            result_set.column_list = [i[0] for i in fields] if fields else []
            result_set.rows = converted_rows
            result_set.affected_rows = len(converted_rows)
        except Exception as e:
            conn.rollback()
            logger.warning(
                f"PgSQL command execution failed, statement: {sql}, details: {traceback.format_exc()}"
            )
            result_set.error = str(e)
        finally:
            if close_conn:
                self.close()
        return result_set

    def filter_sql(self, sql="", limit_num=0):
        # Add LIMIT for query SQL. TODO: optimize limit rewriting.
        sql_lower = sql.lower().rstrip(";").strip()
        if re.match(r"^select", sql_lower):
            if re.search(r"limit\s+(\d+)$", sql_lower) is None:
                if re.search(r"limit\s+\d+\s*,\s*(\d+)$", sql_lower) is None:
                    return f"{sql.rstrip(';')} limit {limit_num};"
        return f"{sql.rstrip(';')};"

    def query_masking(self, db_name=None, sql="", resultset=None):
        """Simple field masking rule, only effective for SELECT."""
        if re.match(r"^select", sql, re.I):
            filtered_result = simple_column_mask(self.instance, resultset)
            filtered_result.is_masked = True
        else:
            filtered_result = resultset
        return filtered_result

    def execute_check(self, db_name=None, sql=""):
        """Run checks before workflow execution and return a ReviewSet."""
        config = SysConfig()
        check_result = ReviewSet(full_sql=sql)
        # Check disabled/high-risk statements.
        line = 1
        critical_ddl_regex = config.get("critical_ddl_regex", "")
        p = re.compile(critical_ddl_regex)
        check_result.syntax_type = 2  # TODO Workflow type: 0 others, 1 DDL, 2 DML
        for statement in sqlparse.split(sql):
            statement = sqlparse.format(statement, strip_comments=True)
            # Disabled statements.
            if re.match(r"^select", statement.lower()):
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

            # Valid statements.
            else:
                result = ReviewResult(
                    id=line,
                    errlevel=0,
                    stagestatus="Audit completed",
                    errormessage="None",
                    sql=statement,
                    affected_rows=0,
                    execute_time=0,
                )
            # Determine workflow syntax type.
            if get_syntax_type(statement) == "DDL":
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

    def execute_workflow(self, workflow, close_conn=True):
        """Execute workflow and return a ReviewSet."""
        sql = workflow.sqlworkflowcontent.sql_content
        execute_result = ReviewSet(full_sql=sql)
        # Strip comments and split statements.
        sql = sqlparse.format(sql, strip_comments=True)
        split_sql = sqlparse.split(sql)
        line = 1
        statement = None
        db_name = workflow.db_name
        try:
            conn = self.get_connection(db_name=db_name)
            conn.autocommit = False
            cursor = conn.cursor()
            cursor.execute("SET transaction ISOLATION LEVEL READ COMMITTED READ WRITE;")
            # Execute split statements one by one and append results.
            for statement in split_sql:
                statement = statement.rstrip(";")
                with FuncTimer() as t:
                    cursor.execute(statement)
                execute_result.rows.append(
                    ReviewResult(
                        id=line,
                        errlevel=0,
                        stagestatus="Execute Successfully",
                        errormessage="None",
                        sql=statement,
                        affected_rows=cursor.rowcount,
                        execute_time=t.cost,
                    )
                )
                line += 1
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.warning(
                f"PGSQL command execution failed, statement: {statement or sql}, details: {traceback.format_exc()}"
            )
            execute_result.error = str(e)
            # Append current failed statement to execution results.
            execute_result.rows.append(
                ReviewResult(
                    id=line,
                    errlevel=2,
                    stagestatus="Execute Failed",
                    errormessage=f"Exception: {e}",
                    sql=statement or sql,
                    affected_rows=0,
                    execute_time=0,
                )
            )
            line += 1
            # Mark following statements as skipped due to previous failure.
            for statement in split_sql[line - 1 :]:
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
        finally:
            if close_conn:
                self.close()
        return execute_result

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def processlist(self, command_type, **kwargs):
        """Get connection information."""
        sql = """
            select psa.pid
                                ,concat('{',array_to_string(pg_blocking_pids(psa.pid),','),'}') block_pids
                                ,psa.leader_pid
                                ,psa.datname,psa.usename
                                ,psa.application_name
                                ,psa.state
                                ,psa.client_addr::text client_addr
                                ,round(GREATEST(EXTRACT(EPOCH FROM (now() - psa.query_start)),0)::numeric,4) elapsed_time_seconds
                ,GREATEST(now() - psa.query_start, INTERVAL '0 second') AS elapsed_time
                        ,(case when psa.leader_pid is null then psa.query end) query
                                ,psa.wait_event_type,psa.wait_event
                                ,psa.query_start
                                ,psa.backend_start
                                ,psa.client_hostname,psa.client_port
                                ,psa.xact_start transaction_start_time
                ,psa.state_change,psa.backend_xid,psa.backend_xmin,psa.backend_type
                                from  pg_stat_activity psa
                                where 1=1
                                AND psa.pid <> pg_backend_pid()
                                $state_not_idle$
                                order by (case 
                                    when psa.state='active' then 10 
                                    when psa.state like 'idle in transaction%' then 5
                                    when psa.state='idle' then 99 else 100 end)
                                    ,elapsed_time_seconds desc
                                ,(case when psa.leader_pid is not null then 1 else 0 end);
            """
        # escape
        command_type = self.escape_string(command_type)
        if not command_type:
            command_type = "Not Idle"

        if command_type == "Not Idle":
            sql = sql.replace("$state_not_idle$", "and psa.state<>'idle'")

        # Apply all template replacements.
        sql = sql.replace("$state_not_idle$", "")
        return self.query("postgres", sql)
