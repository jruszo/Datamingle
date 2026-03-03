# -*- coding: UTF-8 -*-
import logging
import pymemcache

from typing import List, Tuple

from django.core.checks.security.base import check_secret_key

from . import EngineBase
from .models import ResultSet, ReviewSet, ReviewResult
from sql.models import SqlWorkflow

logger = logging.getLogger("default")


class MemcachedEngine(EngineBase):
    test_query = "stats"
    name = "Memcached"
    info = "Memcached engine"

    def __init__(self, instance=None):
        super().__init__(instance=instance)
        # Store multiple node hosts: db_name -> host
        # If instance.host contains multiple hosts, they are split by ","
        self.nodes = {}

        if not instance:
            return

        for i, host in enumerate(instance.host.split(",")):
            db_name = f"Node - {i}"
            self.nodes[db_name] = host.strip()

    def get_connection(self, db_name=None):
        db_name = db_name or "Node - 0"

        if db_name not in self.nodes:
            logger.warning(f"Memcached node {db_name} does not exist")
            raise Exception(f"Memcached node {db_name} does not exist")

        node_host = self.nodes[db_name]

        try:
            conn = pymemcache.Client(
                server=(node_host, self.port), connect_timeout=10.0, timeout=10.0
            )
            return conn
        except Exception as e:
            raise Exception(f"Failed to connect to Memcached node {node_host}: {str(e)}")

    def test_connection(self):
        """Test whether the instance connection is available."""
        try:
            conn = self.get_connection(None)
            # Verify connectivity with the version command.
            version = conn.version()
            if version:
                return ResultSet(
                    rows=[[f"Connection successful, version: {version}"]],
                    column_list=["Status"],
                )
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            raise Exception(f"Connection test failed: {str(e)}")

    def get_all_databases(self):
        """Return all available nodes, using each node as a logical database."""
        result_set = ResultSet(column_list=["Node"], rows=[])
        try:
            for db_name in self.nodes:
                result_set.rows.append([db_name])
            return result_set
        except Exception as e:
            logger.error(f"Failed to get all nodes: {str(e)}")
            raise Exception(f"Failed to get all nodes: {str(e)}")

    def get_all_tables(self, db_name, **kwargs):
        return ResultSet(rows=[])

    # Query method in command-table mode.
    def query(
        self,
        db_name=None,
        sql="",
        limit_num=0,
        close_conn=True,
        parameters=None,
        **kwargs,
    ):
        """Execute a query and return a ResultSet in command-table mode."""
        result_set = ResultSet(full_sql=sql)

        try:
            conn = self.get_connection(db_name)
            result_set = _handle_cmd(conn, sql)
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            result_set.error = str(e)
            result_set.rows = [[f"Error: {str(e)}"]]
        finally:
            if close_conn:
                # Only close the default connection and keep node mappings.
                if self.conn:
                    self.conn = None
                # Node connections are not closed because they may be reused.

        return result_set

    def query_check(self, db_name=None, sql=""):
        """Check query command and return {'bad_query': bool, 'filtered_sql': str}."""
        # Basic command validation.

        cmd, cmd_args = _parse_cmd_args(sql)
        allowed_commands = [
            "version",
            "get",
            "gets",
        ]

        if cmd not in allowed_commands:
            return {
                "bad_query": True,
                "filtered_sql": sql,
                "msg": "Only (version, get, gets) commands are supported",
            }

        return {"bad_query": False, "filtered_sql": sql}

    def execute(self, db_name=None, sql="", **kwargs):
        execute_result = ReviewSet(full_sql=sql)

        try:
            conn = self.get_connection(db_name)
            cmd_result = _handle_cmd(conn, sql)

            assert len(cmd_result.rows) == 1, "Command result row count is not 1"
            assert len(cmd_result.rows[0]) == 1, "Command result column count is not 1"

            if cmd_result.rows[0][0] == "FAIL":
                execute_result.rows.append(
                    ReviewResult(
                        id=1,
                        affected_rows=0,
                        sql=sql,
                        stage="Execute",
                        stagestatus="Fail",
                    )
                )
            else:
                execute_result.rows.append(
                    ReviewResult(
                        id=1,
                        affected_rows=1,
                        sql=sql,
                        stage="Execute",
                        stagestatus="Success",
                    )
                )

            execute_result.affected_rows = cmd_result.affected_rows
            execute_result.error = cmd_result.error
        except Exception as e:
            logger.error(f"Statement execution failed: {str(e)}")
            execute_result.error = str(e)
            execute_result.rows = [{"error": str(e)}]

        return execute_result

    def execute_check(self, db_name=None, sql=""):
        """Validate an execution statement."""
        check_result = ReviewSet(full_sql=sql)

        allowed_commands = [
            "set",
            "delete",
            "incr",
            "decr",
            "touch",
        ]
        cmd, cmd_args = _parse_cmd_args(sql)

        if cmd not in allowed_commands:
            check_result.error_count += 1
            check_result.error = f"Unsupported command: {cmd}"
            check_result.rows = [
                ReviewResult(
                    id=1,
                    affected_rows=0,
                    sql=sql,
                    stage="Check",
                    stagestatus="Fail",
                    errlevel=2,
                    errormessage=f"Unsupported command: {cmd}",
                )
            ]
        else:
            check_result.rows = [
                ReviewResult(
                    id=1,
                    affected_rows=1,
                    sql=sql,
                    stage="Check",
                    stagestatus="Success",
                )
            ]
            check_result.checked = True

        return check_result

    def execute_workflow(self, workflow: SqlWorkflow):
        """Execute a workflow and return a ReviewSet."""
        return self.execute(
            db_name=workflow.db_name, sql=workflow.sqlworkflowcontent.sql_content
        )

    def get_execute_percentage(self):
        """Get execution progress."""
        return 100

    @property
    def server_version(self):
        """Return engine server version."""
        try:
            conn = self.get_connection()
            version = conn.version()
            # Try parsing version into a tuple.
            parts = str(version).split(".")
            version_tuple = tuple(
                int(part) if part.isdigit() else 0 for part in parts[:3]
            )
            return version_tuple
        except Exception as e:
            logger.error(f"Failed to get Memcached version: {str(e)}")
            return tuple()


# Command handlers


def _handle_get(conn: pymemcache.Client, sql: str, cmd_args: List[str]):
    """
    Handle get command: get <key>
    """

    result_set = ResultSet(full_sql=sql)

    if len(cmd_args) < 1:
        raise Exception("Invalid get command format")

    try:
        key = cmd_args[0].strip()
        value = conn.get(key)
        result_set.column_list = ["Value"]
        result_set.rows = [[value if value is not None else "None"]]
    except Exception as e:
        raise Exception(f"get command execution failed: {str(e)}")

    result_set.affected_rows = len(result_set.rows)
    return result_set


def _handle_set(conn: pymemcache.Client, sql: str, cmd_args: List[str]):
    """
    Handle set command: set <key> <value> [expiry]
    """

    result_set = ResultSet(full_sql=sql)

    if len(cmd_args) < 2:
        raise Exception("Invalid set command format")

    try:
        key = cmd_args[0].strip()
        value = cmd_args[1].strip()
        expiry = int(cmd_args[2].strip()) if len(cmd_args) > 2 else 0
        ok = conn.set(key, value, expire=expiry)
        result_set.rows = [["OK"] if ok else ["FAIL"]]
        result_set.column_list = ["Status"]
    except Exception as e:
        raise Exception(f"set command execution failed: {str(e)}")

    result_set.affected_rows = len(result_set.rows)
    return result_set


def _handle_delete(conn: pymemcache.Client, sql: str, cmd_args: List[str]):
    """
    Handle delete command: delete <key>
    """

    result_set = ResultSet(full_sql=sql)

    if len(cmd_args) < 1:
        raise Exception("Invalid delete command format")

    try:
        key = cmd_args[0].strip()
        ok = conn.delete(key)
        result_set.rows = [["OK"] if ok else ["FAIL"]]
        result_set.column_list = ["Status"]
    except Exception as e:
        raise Exception(f"delete command execution failed: {str(e)}")

    result_set.affected_rows = len(result_set.rows)
    return result_set


def _handle_version(conn: pymemcache.Client, sql: str, cmd_args: List[str]):
    """
    Handle version command: version
    """

    result_set = ResultSet(full_sql=sql)
    version = conn.version()
    result_set.rows = [[version]]
    result_set.column_list = ["Version"]
    result_set.affected_rows = 1
    return result_set


def _handle_gets(conn: pymemcache.Client, sql: str, cmd_args: List[str]):
    """
    Handle gets command: gets <key1> <key2>
    """

    result_set = ResultSet(full_sql=sql)

    if len(cmd_args) < 1:
        raise Exception("Invalid gets command format")

    try:
        keys = [v.strip() for v in cmd_args]
        values = conn.gets_many(keys)
        result_set.column_list = ["Key", "Value", "CAS"]
        for key, (value, cas) in values.items():
            result_set.rows.append([key, value if value is not None else "None", cas])
    except Exception as e:
        raise Exception(f"gets command execution failed: {str(e)}")

    result_set.affected_rows = len(result_set.rows)
    return result_set


def _handle_incr(conn: pymemcache.Client, sql: str, cmd_args: List[str]):
    """
    Handle incr command: incr <key> [value]
    """

    result_set = ResultSet(full_sql=sql)

    if len(cmd_args) < 1:
        raise Exception("Invalid incr command format")
    try:
        key = cmd_args[0].strip()
        value = int(cmd_args[1].strip()) if len(cmd_args) > 1 else 1
        result = conn.incr(key, value)
        result_set.rows = [[str(result) if result is not None else "FAIL"]]
        result_set.column_list = ["Result"]
    except Exception as e:
        raise Exception(f"incr command execution failed: {str(e)}")

    result_set.affected_rows = 1
    return result_set


def _handle_decr(conn: pymemcache.Client, sql: str, cmd_args: List[str]):
    """
    Handle decr command: decr <key> [value]
    """
    result_set = ResultSet(full_sql=sql)

    if len(cmd_args) < 1:
        raise Exception("Invalid decr command format")
    try:
        key = cmd_args[0].strip()
        value = int(cmd_args[1].strip()) if len(cmd_args) > 1 else 1
        result = conn.decr(key, value)
        result_set.rows = [[str(result) if result is not None else "FAIL"]]
        result_set.column_list = ["Result"]
    except Exception as e:
        raise Exception(f"decr command execution failed: {str(e)}")

    result_set.affected_rows = 1
    return result_set


def _handle_touch(conn: pymemcache.Client, sql: str, cmd_args: List[str]):
    """
    Handle touch command: touch <key> <expiry>
    """

    result_set = ResultSet(full_sql=sql)

    if len(cmd_args) < 2:
        raise Exception("Invalid touch command format")

    try:
        key = cmd_args[0].strip()
        expiry = int(cmd_args[1].strip())
        ok = conn.touch(key, expire=expiry)
        result_set.rows = [["OK"] if ok else ["FAIL"]]
        result_set.column_list = ["Status"]
    except Exception as e:
        raise Exception(f"touch command execution failed: {str(e)}")

    result_set.affected_rows = 1
    return result_set


# Command handler mapping
cmd_handlers = {
    "get": _handle_get,
    "set": _handle_set,
    "delete": _handle_delete,
    "version": _handle_version,
    "gets": _handle_gets,
    "incr": _handle_incr,
    "decr": _handle_decr,
    "touch": _handle_touch,
}


def _parse_cmd_args(sql: str) -> Tuple[str, List[str]]:
    """
    Parse command arguments.
    """
    cmd = sql.split(" ")[0].strip().lower()
    cmd_args = sql.split(" ")[1:]
    return cmd, cmd_args


def _handle_cmd(conn: pymemcache.Client, sql: str):
    """
    Handle command.
    """

    # Basic command parsing.
    sql = sql.strip().lower()
    if not sql:
        raise Exception("Empty SQL statement")

    # Extract command name.
    parts = sql.split(" ")
    cmd = parts[0]
    cmd_args = parts[1:]

    if cmd not in cmd_handlers:
        raise Exception(f"Unsupported command: {cmd}")

    return cmd_handlers[cmd](conn, sql, cmd_args)
