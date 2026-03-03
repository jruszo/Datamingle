# -*- coding: UTF-8 -*-
import logging
import traceback
import re
import sqlparse

import phoenixdb
from . import EngineBase
from .models import ResultSet, ReviewSet, ReviewResult

logger = logging.getLogger("default")


class PhoenixEngine(EngineBase):
    test_query = "SELECT 1"

    name = "phoenix"
    info = "phoenix engine"

    def get_connection(self, db_name=None):
        if self.conn:
            return self.conn

        database_url = f"http://{self.host}:{self.port}/"
        self.conn = phoenixdb.connect(database_url, autocommit=True)
        return self.conn

    def get_all_databases(self):
        """Get database list and return a ResultSet."""
        sql = "SELECT DISTINCT TABLE_SCHEM FROM SYSTEM.CATALOG"
        result = self.query(sql=sql)
        result.rows = [row[0] for row in result.rows if row[0] is not None]
        return result

    def get_all_tables(self, db_name, **kwargs):
        """Get table list and return a ResultSet."""
        sql = f"SELECT DISTINCT TABLE_NAME FROM SYSTEM.CATALOG WHERE TABLE_SCHEM = ?"
        result = self.query(db_name=db_name, sql=sql, parameters=(db_name,))
        result.rows = [row[0] for row in result.rows if row[0] is not None]
        return result

    def get_all_columns_by_tb(self, db_name, tb_name, **kwargs):
        """Get all columns and return a ResultSet."""

        sql = f""" SELECT DISTINCT COLUMN_NAME FROM SYSTEM.CATALOG
 WHERE TABLE_SCHEM = ? AND table_name = ? AND column_name is not null"""
        return self.query(
            sql=sql,
            parameters=(
                db_name,
                tb_name,
            ),
        )

    def describe_table(self, db_name, tb_name, **kwargs):
        """return ResultSet"""
        sql = f"""SELECT COLUMN_NAME,SqlTypeName(DATA_TYPE) FROM SYSTEM.CATALOG
 WHERE TABLE_SCHEM = ? and table_name = ? and column_name is not null"""
        result = self.query(sql=sql, parameters=(db_name, tb_name))
        return result

    def query_check(self, db_name=None, sql=""):
        # Check query statement, strip comments, and split.
        result = {"msg": "", "bad_query": False, "filtered_sql": sql, "has_star": False}
        keyword_warning = ""
        sql_whitelist = ["select", "explain"]
        # Build regex pattern from whitelist.
        whitelist_pattern = "^" + "|^".join(sql_whitelist)
        # Remove comments, validate syntax, and keep the first valid SQL.
        try:
            sql = sql.format(sql, strip_comments=True)
            sql = sqlparse.split(sql)[0]
            result["filtered_sql"] = sql.strip()
            # sql_lower = sql.lower()
        except IndexError:
            result["bad_query"] = True
            result["msg"] = "No valid SQL statement found"
            return result
        if re.match(whitelist_pattern, sql) is None:
            result["bad_query"] = True
            result["msg"] = "Only {} syntax is supported!".format(
                ",".join(sql_whitelist)
            )
            return result
        if result.get("bad_query"):
            result["msg"] = keyword_warning
        return result

    def filter_sql(self, sql="", limit_num=0):
        """Check whether SELECT statement already includes a LIMIT clause."""
        sql = sql.rstrip(";").strip()
        if re.match(r"^select", sql, re.I):
            if not re.compile(r"limit\s+(\d+)\s*((,|offset)\s*\d+)?\s*$", re.I).search(
                sql
            ):
                sql = f"{sql} limit {limit_num}"
        else:
            sql = f"{sql};"
        return sql.strip()

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
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, parameters)
            if int(limit_num) > 0:
                rows = cursor.fetchmany(int(limit_num))
            else:
                rows = cursor.fetchall()
            fields = cursor.description

            result_set.column_list = [i[0] for i in fields] if fields else []
            result_set.rows = [tuple(x) for x in rows]
            result_set.affected_rows = len(result_set.rows)
        except Exception as e:
            logger.warning(
                f"PhoenixDB statement execution failed, sql: {sql}, details: {traceback.format_exc()}"
            )
            result_set.error = str(e)
        finally:
            if close_conn:
                self.close()
        return result_set

    def query_masking(self, db_name=None, sql="", resultset=None):
        """Return resultset without applying additional masking."""
        return resultset

    def execute_check(self, db_name=None, sql=""):
        """Run checks before workflow execution and return a ReviewSet."""
        check_result = ReviewSet(full_sql=sql)
        # Split statements and append passed check results by default.
        rowid = 1
        split_sql = sqlparse.split(sql)
        for statement in split_sql:
            check_result.rows.append(
                ReviewResult(
                    id=rowid,
                    errlevel=0,
                    stagestatus="Audit completed",
                    errormessage="None",
                    sql=statement,
                    affected_rows=0,
                    execute_time=0,
                )
            )
            rowid += 1
        return check_result

    def execute_workflow(self, workflow):
        """PhoenixDB workflow execution does not require backup."""
        return self.execute(
            db_name=workflow.db_name, sql=workflow.sqlworkflowcontent.sql_content
        )

    def execute(self, db_name=None, sql="", close_conn=True, parameters=None):
        """Execute raw statements."""
        execute_result = ReviewSet(full_sql=sql)
        conn = self.get_connection(db_name=db_name)
        cursor = conn.cursor()
        rowid = 1
        split_sql = sqlparse.split(sql)
        for statement in split_sql:
            try:
                cursor.execute(statement.rstrip(";"), parameters)
            except Exception as e:
                logger.error(
                    f"Phoenix command execution failed, sql: {sql}, details: {traceback.format_exc()}"
                )
                execute_result.error = str(e)
                execute_result.rows.append(
                    ReviewResult(
                        id=rowid,
                        errlevel=2,
                        stagestatus="Execute Failed",
                        errormessage=f"Exception: {e}",
                        sql=statement,
                        affected_rows=0,
                        execute_time=0,
                    )
                )
                break
            else:
                execute_result.rows.append(
                    ReviewResult(
                        id=rowid,
                        errlevel=0,
                        stagestatus="Execute Successfully",
                        errormessage="None",
                        sql=statement,
                        affected_rows=cursor.rowcount,
                        execute_time=0,
                    )
                )
            rowid += 1
        if execute_result.error:
            # If failed, append remaining statements as not executed.
            for statement in split_sql[rowid:]:
                execute_result.rows.append(
                    ReviewResult(
                        id=rowid,
                        errlevel=2,
                        stagestatus="Execute Failed",
                        errormessage="Previous statement failed, not executed",
                        sql=statement,
                        affected_rows=0,
                        execute_time=0,
                    )
                )
                rowid += 1

        if close_conn:
            self.close()
        return execute_result

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
