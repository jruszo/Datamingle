# -*- coding: UTF-8 -*-
import logging
import os
import re
import traceback
import MySQLdb
import pymysql
import simplejson as json

from common.config import SysConfig
from sql.models import AliyunRdsConfig
from sql.utils.sql_utils import get_syntax_type
from . import EngineBase
from .models import ResultSet, ReviewSet, ReviewResult

logger = logging.getLogger("default")


class GoInceptionEngine(EngineBase):
    test_query = "INCEPTION GET VARIABLES"

    name = "GoInception"

    info = "GoInception engine"

    def get_connection(self, db_name=None):
        if self.conn:
            return self.conn
        if hasattr(self, "instance"):
            self.conn = MySQLdb.connect(
                host=self.host,
                port=self.port,
                charset=self.instance.charset or "utf8mb4",
                connect_timeout=10,
            )
            return self.conn
        archer_config = SysConfig()
        go_inception_host = archer_config.get(
            "go_inception_host", os.environ.get("GO_INCEPTION_HOST", "127.0.0.1")
        )
        go_inception_port = int(
            archer_config.get(
                "go_inception_port", os.environ.get("GO_INCEPTION_PORT", "4000")
            )
        )
        go_inception_user = archer_config.get(
            "go_inception_user", os.environ.get("GO_INCEPTION_USER", "")
        )
        go_inception_password = archer_config.get(
            "go_inception_password", os.environ.get("GO_INCEPTION_PASSWORD", "")
        )
        self.conn = MySQLdb.connect(
            host=go_inception_host,
            port=go_inception_port,
            user=go_inception_user,
            passwd=go_inception_password,
            charset="utf8mb4",
            connect_timeout=10,
        )
        return self.conn

    @staticmethod
    def get_backup_connection():
        archer_config = SysConfig()
        backup_host = archer_config.get(
            "inception_remote_backup_host",
            os.environ.get("INCEPTION_REMOTE_BACKUP_HOST", "127.0.0.1"),
        )
        backup_port = int(
            archer_config.get(
                "inception_remote_backup_port",
                os.environ.get("INCEPTION_REMOTE_BACKUP_PORT", "3306"),
            )
        )
        backup_user = archer_config.get(
            "inception_remote_backup_user",
            os.environ.get("INCEPTION_REMOTE_BACKUP_USER", ""),
        )
        backup_password = archer_config.get(
            "inception_remote_backup_password",
            os.environ.get("INCEPTION_REMOTE_BACKUP_PASSWORD", ""),
        )
        return MySQLdb.connect(
            host=backup_host,
            port=backup_port,
            user=backup_user,
            passwd=backup_password,
            charset="utf8mb4",
            autocommit=True,
        )

    def escape_string(self, value: str) -> str:
        """Escape string parameters."""
        return pymysql.escape_string(value)

    def execute_check(self, instance=None, db_name=None, sql=""):
        """inception check"""
        # Connect through SSH tunnel if configured.
        host, port, user, password = self.remote_instance_conn(instance)
        check_result = ReviewSet(full_sql=sql)
        # Run inception validation.
        check_result.rows = []
        variables, set_session_sql = get_session_variables(instance)
        # Get real_row_count option.
        real_row_count = SysConfig().get("real_row_count", False)
        real_row_count_option = "--real_row_count=true;" if real_row_count else ""
        inception_sql = f"""/*--user='{user}';--password='{password}';--host='{host}';--port={port};--check=1;{real_row_count_option}*/
                            inception_magic_start;
                            {set_session_sql}
                            use `{db_name}`;
                            {sql.rstrip(';')};
                            inception_magic_commit;"""
        inception_result = self.query(sql=inception_sql)
        check_result.syntax_type = (
            2  # TODO Workflow type: 0 others, 1 DDL, 2 DML; MySQL-only for now.
        )
        for r in inception_result.rows:
            check_result.rows += [ReviewResult(inception_result=r)]
            if r[2] == 1:  # Warning.
                check_result.warning_count += 1
            elif r[2] == 2:  # Error.
                check_result.error_count += 1
            # Continue this check only if no DDL has been identified.
            if check_result.syntax_type == 2:
                if get_syntax_type(r[5], parser=False, db_type="mysql") == "DDL":
                    check_result.syntax_type = 1
        check_result.column_list = inception_result.column_list
        check_result.checked = True
        check_result.error = inception_result.error
        check_result.warning = inception_result.warning
        return check_result

    def execute(self, workflow=None):
        """Execute workflow."""
        instance = workflow.instance
        # Connect through SSH tunnel if configured.
        host, port, user, password = self.remote_instance_conn(instance)
        execute_result = ReviewSet(full_sql=workflow.sqlworkflowcontent.sql_content)
        if workflow.is_backup:
            str_backup = "--backup=1"
        else:
            str_backup = "--backup=0"

        # Submit execution to inception.
        variables, set_session_sql = get_session_variables(instance)
        sql_execute = f"""/*--user='{user}';--password='{password}';--host='{host}';--port={port};--execute=1;--ignore-warnings=1;{str_backup};--sleep=200;--sleep_rows=100*/
                            inception_magic_start;
                            {set_session_sql}
                            use `{workflow.db_name}`;
                            {workflow.sqlworkflowcontent.sql_content.rstrip(';')};
                            inception_magic_commit;"""
        inception_result = self.query(sql=sql_execute)
        # Execution failure: inception crash or connection issue during execution.
        if inception_result.error and not execute_result.rows:
            execute_result.error = inception_result.error
            execute_result.rows = [
                ReviewResult(
                    stage="Execute failed",
                    errlevel=2,
                    stagestatus="Aborted unexpectedly",
                    errormessage=f"goInception Error: {inception_result.error}",
                    sql=workflow.sqlworkflowcontent.sql_content,
                )
            ]
            return execute_result

        # Convert result to ReviewSet.
        for r in inception_result.rows:
            execute_result.rows += [ReviewResult(inception_result=r)]

        # If any row has errLevel 1/2 and status not containing Execute Successfully,
        # mark final execution result as error.
        for r in execute_result.rows:
            if r.errlevel in (1, 2) and not re.search(
                r"Execute Successfully", r.stagestatus
            ):
                execute_result.error = "Line {0} has error/warning: {1}".format(
                    r.id, r.errormessage
                )
                break
        return execute_result

    def query(self, db_name=None, sql="", limit_num=0, close_conn=True, **kwargs):
        """Return a ResultSet."""
        result_set = ResultSet(full_sql=sql)
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            effect_row = cursor.execute(sql)
            if int(limit_num) > 0:
                rows = cursor.fetchmany(size=int(limit_num))
            else:
                rows = cursor.fetchall()
            fields = cursor.description

            result_set.column_list = [i[0] for i in fields] if fields else []
            result_set.rows = rows
            result_set.affected_rows = effect_row
        except Exception as e:
            logger.warning(
                f"goInception statement execution failed, details: {traceback.format_exc()}"
            )
            result_set.error = str(e)
        if close_conn:
            self.close()
        return result_set

    def query_print(self, instance, db_name=None, sql=""):
        """
        Print syntax tree.
        """
        # Connect through SSH tunnel if configured.
        host, port, user, password = self.remote_instance_conn(instance)
        sql = f"""/*--user='{user}';--password='{password}';--host='{host}';--port={port};--enable-query-print;*/
                          inception_magic_start;\
                          use `{db_name}`;
                          {sql.rstrip(';')};
                          inception_magic_commit;"""
        print_info = self.query(db_name=db_name, sql=sql).to_dict()[1]
        if print_info.get("errmsg"):
            raise RuntimeError(print_info.get("errmsg"))
        return print_info

    def query_data_masking(self, instance, db_name=None, sql=""):
        """
        Send SQL to goInception to print syntax tree and get select list.
        Uses masking parameter: https://github.com/hanchuanchuan/goInception/pull/355
        """
        # Connect through SSH tunnel if configured.
        host, port, user, password = self.remote_instance_conn(instance)
        sql = f"""/*--user={user};--password={password};--host={host};--port={port};--masking=1;*/
                          inception_magic_start;
                          use `{db_name}`;
                          {sql}
                          inception_magic_commit;"""
        query_result = self.query(db_name=db_name, sql=sql)
        # Raise immediately if there is an exception.
        if query_result.error:
            raise RuntimeError(f"Inception Error: {query_result.error}")
        if not query_result.rows:
            raise RuntimeError("Inception Error: failed to retrieve syntax information")
        # Handle edge cases where returned content is audit result.
        # https://github.com/hhyo/Archery/issues/1826
        print_info = query_result.to_dict()[0]
        if "error_level" in print_info:
            raise RuntimeError(f'Inception Error: {print_info.get("error_message")}')
        if print_info.get("errlevel") == 0 and print_info.get("errmsg") is None:
            return json.loads(print_info["query_tree"])
        else:
            raise RuntimeError(f'Inception Error: print_info.get("errmsg")')

    def get_rollback(self, workflow):
        """
        Get rollback statements and return them in reverse execution order.
        Return format: ['source statement', 'rollback statement'].
        """
        list_execute_result = json.loads(
            workflow.sqlworkflowcontent.execute_result or "[]"
        )
        # Show rollback statements in reverse order.
        list_execute_result.reverse()
        list_backup_sql = []
        # Create connection.
        conn = self.get_backup_connection()
        cur = conn.cursor()
        for row in list_execute_result:
            try:
                # Get backup_db_name, compatible with old data format '[[]]'.
                if isinstance(row, list):
                    if row[8] == "None":
                        continue
                    backup_db_name = row[8]
                    sequence = row[7]
                    sql = row[5]
                # New data.
                else:
                    if row.get("backup_dbname") in ("None", ""):
                        continue
                    backup_db_name = row.get("backup_dbname")
                    sequence = row.get("sequence")
                    sql = row.get("sql")
                # Get backup table name.
                opid_time = sequence.replace("'", "")
                sql_table = f"""select tablename
                                from {backup_db_name}.$_$Inception_backup_information$_$
                                where opid_time='{opid_time}';"""

                cur.execute(sql_table)
                list_tables = cur.fetchall()
                if list_tables:
                    # Get rollback SQL statements.
                    table_name = list_tables[0][0]
                    sql_back = f"""select rollback_statement
                                   from {backup_db_name}.{table_name}
                                   where opid_time='{opid_time}'"""
                    cur.execute(sql_back)
                    list_backup = cur.fetchall()
                    # Build rollback statement list: ['source', 'rollback'].
                    list_backup_sql.append(
                        [sql, "\n".join([back_info[0] for back_info in list_backup])]
                    )
            except Exception as e:
                logger.error(
                    f"Failed to get rollback statement, details: {traceback.format_exc()}"
                )
                raise Exception(e)
        # Close connection.
        if conn:
            conn.close()
        return list_backup_sql

    def get_variables(self, variables=None):
        """Get instance parameters."""
        if variables:
            sql = f"inception get variables like '{variables[0]}';"
        else:
            sql = "inception get variables;"
        return self.query(sql=sql)

    def set_variable(self, variable_name, variable_value):
        """Set instance parameter value."""
        sql = f"""inception set {variable_name}={variable_value};"""
        return self.query(sql=sql)

    def osc_control(self, **kwargs):
        """Control OSC execution: progress, terminate, pause, resume, etc."""
        sqlsha1 = self.escape_string(kwargs.get("sqlsha1", ""))
        command = self.escape_string(kwargs.get("command", ""))
        if command == "get":
            sql = f"inception get osc_percent '{sqlsha1}';"
        else:
            sql = f"inception {command} osc '{sqlsha1}';"
        return self.query(sql=sql)

    @staticmethod
    def get_table_ref(query_tree, db_name=None):
        __author__ = "xxlrr"
        """
        Parse referenced table info from goInception syntax tree into
        Inception-compatible format.
        Current logic recursively finds the minimal TableRefs subtrees (possibly
        multiple), then finds Source nodes inside those subtrees to extract table
        references.
        The current implementation finds maximal subtrees step by step to derive
        minimal TableRefs indirectly. It evolved empirically from observed
        goInception tree patterns and is kept because it works reliably.
        """
        table_ref = []

        find_queue = [query_tree]
        for tree in find_queue:
            tree = DictTree(tree)

            # nodes = tree.find_max_tree("TableRefs") or tree.find_max_tree("Left", "Right")
            nodes = tree.find_max_tree("TableRefs", "Left", "Right")
            if nodes:
                # assert isinstance(v, dict) is true
                find_queue.extend([v for node in nodes for v in node.values() if v])
            else:
                snodes = tree.find_max_tree("Source")
                if snodes:
                    table_ref.extend(
                        [
                            {
                                "schema": snode["Source"].get("Schema", {}).get("O")
                                or db_name,
                                "name": snode["Source"].get("Name", {}).get("O", ""),
                            }
                            for snode in snodes
                        ]
                    )
                # assert: source node must exists if table_refs node exists.
                # else:
                #     raise Exception("GoInception Error: not found source node")
        return table_ref

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None


class DictTree(dict):
    def find_max_tree(self, *keys):
        __author__ = "xxlrr"
        """Find matching maximal subtrees via breadth-first search."""
        fit = []
        find_queue = [self]
        for tree in find_queue:
            for k, v in tree.items():
                if k in keys:
                    fit.append({k: v})
                elif isinstance(v, dict):
                    find_queue.append(v)
                elif isinstance(v, list):
                    find_queue.extend([n for n in v if isinstance(n, dict)])
        return fit


def get_session_variables(instance):
    """Build goInception session vars dynamically from target instance."""
    variables = {}
    set_session_sql = ""
    if AliyunRdsConfig.objects.filter(instance=instance, is_enable=True).exists():
        variables.update(
            {
                "ghost_aliyun_rds": "on",
                "ghost_allow_on_master": "true",
                "ghost_assume_rbr": "true",
            }
        )
    # Convert to SQL statements.
    for k, v in variables.items():
        set_session_sql += f"inception set session {k} = '{v}';\n"
    return variables, set_session_sql
