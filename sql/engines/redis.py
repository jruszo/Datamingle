# -*- coding: UTF-8 -*-
"""
@author: hhyo、yyukai
@license: Apache Licence
@file: redis.py
@time: 2019/03/26
"""

import json
import re
import shlex

import redis
import logging
import traceback

from common.utils.timer import FuncTimer
from . import EngineBase
from .models import ResultSet, ReviewSet, ReviewResult

__author__ = "hhyo"

logger = logging.getLogger("default")


class RedisEngine(EngineBase):
    def get_connection(self, db_name=None):
        db_name = db_name or self.db_name
        if self.mode == "cluster":
            return redis.cluster.RedisCluster(
                host=self.host,
                port=self.port,
                username=self.user,
                password=self.password or None,
                encoding_errors="ignore",
                decode_responses=True,
                socket_connect_timeout=10,
                ssl=self.instance.is_ssl,
            )
        else:
            return redis.Redis(
                host=self.host,
                port=self.port,
                db=db_name,
                username=self.user,
                password=self.password or None,
                encoding_errors="ignore",
                decode_responses=True,
                socket_connect_timeout=10,
                ssl=self.instance.is_ssl,
            )

    name = "Redis"

    info = "Redis engine"

    def test_connection(self):
        return self.get_all_databases()

    def get_all_databases(self, **kwargs):
        """
        Get database list.
        :return:
        """
        result = ResultSet(full_sql="CONFIG GET databases")
        conn = self.get_connection()
        try:
            rows = conn.config_get("databases")["databases"]
        except Exception as e:
            """
            If fetching the "databases" config fails, this fallback infers the
            database count by parsing the output of the info command.
            Failure case 1: AWS ElastiCache (Redis) does not support some commands,
            for example: config get xx and some acl commands.
            Failure case 2: Redis user has no admin permission (-@admin), for example:
            "this user has no permissions to run the 'config' command or its subcommand".
            Steps:
            - Get keyspace info via info("Keyspace").
            - Extract db indexes (for example db0, db1).
            - Compute database count; at least 16 databases (0..15) are returned.
            """
            logger.warning(f"Redis CONFIG GET databases failed, exception: {e}")
            dbs = [
                int(i.split("db")[1])
                for i in conn.info("Keyspace").keys()
                if len(i.split("db")) == 2
            ]
            rows = max(dbs + [15]) + 1

        db_list = [str(x) for x in range(int(rows))]
        result.rows = db_list
        return result

    def get_all_tables(self, db_name, **kwargs):
        """Get key list. Redis keys are treated as tables for preview."""
        result = ResultSet(full_sql="")
        max_results = 100
        table_info_list = []
        try:
            conn = self.get_connection(db_name)
            scan_rows = conn.scan_iter(match=None, count=20)
            for idx, key in enumerate(scan_rows):
                if idx >= max_results:
                    break
                table_info_list.append(key)
        except Exception as e:
            logger.error(f"get_all_tables failed, exception: {e}")
            result.message = f"{e}"
        result.rows = table_info_list
        return result

    def query_check(self, db_name=None, sql="", limit_num=0):
        """Run checks before query submission."""
        result = {"msg": "", "bad_query": True, "filtered_sql": sql, "has_star": False}
        safe_cmd = [
            "scan",
            "exists",
            "ttl",
            "pttl",
            "type",
            "get",
            "mget",
            "strlen",
            "hgetall",
            "hlen",
            "hexists",
            "hget",
            "hmget",
            "hkeys",
            "hvals",
            "smembers",
            "scard",
            "sdiff",
            "sunion",
            "sismember",
            "llen",
            "lrange",
            "lindex",
            "zrange",
            "zrangebyscore",
            "zscore",
            "zcard",
            "zcount",
            "zrank",
            "info",
        ]
        # Command validation: only commands in safe_cmd are allowed.
        for cmd in safe_cmd:
            if re.match(rf"^{cmd}", sql.strip(), re.I):
                result["bad_query"] = False
                break
        if result["bad_query"]:
            result["msg"] = "This command is not allowed!"
        return result

    def processlist(self, command_type, **kwargs):
        """Get connection information."""
        sql = "client list"
        result_set = ResultSet(full_sql=sql)
        conn = self.get_connection(db_name=0)
        clients = conn.client_list()
        # Sort by idle time.
        sort_by = "idle"
        reverse = False
        clients = sorted(
            clients, key=lambda client: client.get(sort_by), reverse=reverse
        )
        result_set.rows = clients
        return result_set

    def query(self, db_name=None, sql="", limit_num=0, close_conn=True, **kwargs):
        """Return a ResultSet."""
        result_set = ResultSet(full_sql=sql)
        try:
            conn = self.get_connection(db_name=db_name)
            rows = conn.execute_command(*shlex.split(sql))
            result_set.column_list = ["Result"]
            if isinstance(rows, list) or isinstance(rows, tuple):
                if re.match(rf"^scan", sql.strip(), re.I):
                    keys = [[row] for row in rows[1]]
                    keys.insert(0, [rows[0]])
                    result_set.rows = tuple(keys)
                    result_set.affected_rows = len(rows[1])
                else:
                    result_set.rows = tuple([row] for row in rows)
                    result_set.affected_rows = len(rows)
            elif isinstance(rows, dict):
                result_set.column_list = ["field", "value"]
                # Convert dict/list values to JSON strings.
                pairs_list = []
                for k, v in rows.items():
                    if isinstance(v, dict):
                        processed_value = json.dumps(v)
                    elif isinstance(v, list):
                        processed_value = json.dumps(v)
                    else:
                        processed_value = v
                    # Append processed key-value pair.
                    pairs_list.append([k, processed_value])
                # Convert list to tuple and set to result_set.rows.
                result_set.rows = tuple(pairs_list)
                result_set.affected_rows = len(result_set.rows)
            else:
                result_set.rows = tuple([[rows]])
                result_set.affected_rows = 1 if rows else 0
            if limit_num > 0:
                result_set.rows = result_set.rows[0:limit_num]
        except Exception as e:
            logger.warning(
                f"Redis command execution failed, statement: {sql}, details: {traceback.format_exc()}"
            )
            result_set.error = str(e)
        return result_set

    def filter_sql(self, sql="", limit_num=0):
        return sql.strip()

    def query_masking(self, db_name=None, sql="", resultset=None):
        """Do not apply masking."""
        return resultset

    def execute_check(self, db_name=None, sql=""):
        """Run checks before workflow execution and return a ReviewSet."""
        check_result = ReviewSet(full_sql=sql)
        split_sql = [cmd.strip() for cmd in sql.split("\n") if cmd.strip()]
        line = 1
        for cmd in split_sql:
            result = ReviewResult(
                id=line,
                errlevel=0,
                stagestatus="Audit completed",
                errormessage="Displaying affected rows is not supported yet",
                sql=cmd,
                affected_rows=0,
                execute_time=0,
            )
            check_result.rows += [result]
            line += 1
        return check_result

    def execute_workflow(self, workflow):
        """Execute workflow and return a ReviewSet."""
        sql = workflow.sqlworkflowcontent.sql_content
        split_sql = [cmd.strip() for cmd in sql.split("\n") if cmd.strip()]
        execute_result = ReviewSet(full_sql=sql)
        line = 1
        cmd = None
        try:
            conn = self.get_connection(db_name=workflow.db_name)
            for cmd in split_sql:
                with FuncTimer() as t:
                    conn.execute_command(*shlex.split(cmd))
                execute_result.rows.append(
                    ReviewResult(
                        id=line,
                        errlevel=0,
                        stagestatus="Execute Successfully",
                        errormessage="Displaying affected rows is not supported yet",
                        sql=cmd,
                        affected_rows=0,
                        execute_time=t.cost,
                    )
                )
                line += 1
        except Exception as e:
            logger.warning(
                f"Redis command execution failed, statement: {cmd or sql}, details: {traceback.format_exc()}"
            )
            # Append current failed statement to execution result.
            execute_result.error = str(e)
            execute_result.rows.append(
                ReviewResult(
                    id=line,
                    errlevel=2,
                    stagestatus="Execute Failed",
                    errormessage=f"Exception: {e}",
                    sql=cmd,
                    affected_rows=0,
                    execute_time=0,
                )
            )
            line += 1
            # Append remaining statements as skipped because previous failed.
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
        return execute_result
