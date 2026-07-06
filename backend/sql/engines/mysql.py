# -*- coding: UTF-8 -*-
import json
import logging
import MySQLdb
import pymysql
import re
from enum import Enum
from pymysql.converters import escape_string as pymysql_escape_string

import schemaobject
import sqlparse
from MySQLdb.constants import FIELD_TYPE
from schemaobject.connection import build_database_url

from common.utils.timer import FuncTimer
from sql.utils.sql_utils import get_syntax_type, remove_comments
from . import EngineBase
from .models import ResultSet, ReviewResult, ReviewSet
from .mysql_ddl import MysqlDDLExecutorError, MysqlDDLExecutorService
from sql.utils.data_masking import simple_column_mask
from common.config import SysConfig

logger = logging.getLogger("default")

# https://github.com/mysql/mysql-connector-python/blob/master/lib/mysql/connector/constants.py#L168
column_types_map = {
    0: "DECIMAL",
    1: "TINY",
    2: "SHORT",
    3: "LONG",
    4: "FLOAT",
    5: "DOUBLE",
    6: "NULL",
    7: "TIMESTAMP",
    8: "LONGLONG",
    9: "INT24",
    10: "DATE",
    11: "TIME",
    12: "DATETIME",
    13: "YEAR",
    14: "NEWDATE",
    15: "VARCHAR",
    16: "BIT",
    245: "JSON",
    246: "NEWDECIMAL",
    247: "ENUM",
    248: "SET",
    249: "TINY_BLOB",
    250: "MEDIUM_BLOB",
    251: "LONG_BLOB",
    252: "BLOB",
    253: "VAR_STRING",
    254: "STRING",
    255: "GEOMETRY",
}


class MysqlForkType(Enum):
    """Define supported server fork types."""

    MYSQL = "mysql"
    MARIADB = "mariadb"
    PERCONA = "percona"


class MysqlEngine(EngineBase):
    name = "MySQL"
    info = "MySQL engine"
    test_query = "SELECT 1"
    _server_version = None
    _server_fork_type = None
    _server_info = None

    def __init__(self, instance=None):
        super().__init__(instance=instance)
        self.config = SysConfig()

    def get_connection(self, db_name=None):
        # https://stackoverflow.com/questions/19256155/python-mysqldb-returning-x01-for-bit-values
        conversions = MySQLdb.converters.conversions
        conversions[FIELD_TYPE.BIT] = lambda data: data == b"\x01"
        if self.conn:
            self.thread_id = self.conn.thread_id()
            return self.conn
        if db_name:
            self.conn = MySQLdb.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                passwd=self.password,
                db=db_name,
                charset=self.instance.charset or "utf8mb4",
                conv=conversions,
                connect_timeout=10,
            )
        else:
            self.conn = MySQLdb.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                passwd=self.password,
                charset=self.instance.charset or "utf8mb4",
                conv=conversions,
                connect_timeout=10,
            )
        self.thread_id = self.conn.thread_id()
        return self.conn

    def escape_string(self, value: str) -> str:
        """Escape string parameters."""
        return pymysql_escape_string(value)

    @property
    def auto_backup(self):
        """Whether backup is supported."""
        return False

    @property
    def seconds_behind_master(self):
        server_version = self.server_version
        # For non-MariaDB and version >= 8.4, use `show replica status`.
        if self.server_fork_type != MysqlForkType.MARIADB and server_version >= (8, 4):
            status_sql = "show replica status"
        else:
            status_sql = "show slave status"
        slave_status = self.query(
            sql=status_sql,
            close_conn=False,
            cursorclass=MySQLdb.cursors.DictCursor,
        )
        return (
            slave_status.rows[0].get("Seconds_Behind_Master")
            if slave_status.rows
            else None
        )

    @property
    def server_version(self):
        if self._server_version:
            return self._server_version

        def numeric_part(s):
            """Returns the leading numeric part of a string."""
            re_numeric_part = re.compile(r"^(\d+)")
            m = re_numeric_part.match(s)
            if m:
                return int(m.group(1))
            return None

        conn = self.get_connection()
        version = conn.get_server_info()
        self._server_version = tuple([numeric_part(n) for n in version.split(".")[:3]])
        return self._server_version

    def get_inventory_details(self):
        default_details = super().get_inventory_details()
        conn = self.get_connection()
        try:
            version = conn.get_server_info()
        except Exception as exc:
            logger.warning("Failed to fetch MySQL server version: %s", exc)
            return default_details

        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT @@hostname")
            row = cursor.fetchone()
        except Exception as exc:
            logger.warning("Failed to fetch MySQL inventory details: %s", exc)
            return default_details
        finally:
            if cursor is not None:
                cursor.close()

        hostname = row[0] if row else default_details["hostname"]
        details = dict(default_details)
        details["hostname"] = hostname or default_details["hostname"]
        details["version"] = version
        return details

    @property
    def server_info(self):
        if self._server_info:
            return self._server_info
        conn = self.get_connection()
        self._server_info = conn.get_server_info()
        return self._server_info

    @property
    def server_fork_type(self):
        """Determine server fork type: mysql, mariadb, or percona."""
        server_info = self.server_info
        for i in list(MysqlForkType):
            if i.value in server_info.lower():
                return i
        return MysqlForkType.MYSQL

    @property
    def schema_object(self):
        """Get schema object for current instance."""
        url = build_database_url(
            host=self.host, username=self.user, password=self.password, port=self.port
        )
        return schemaobject.SchemaObject(
            url, charset=self.instance.charset or "utf8mb4"
        )

    def kill_connection(self, thread_id):
        """Terminate database connection."""
        self.query(sql=f"kill {thread_id}")

    # Databases forbidden for querying.
    forbidden_databases = [
        "information_schema",
        "performance_schema",
        "mysql",
        "test",
        "sys",
    ]

    def get_all_databases(self):
        """Get database list, return a ResultSet."""
        sql = "show databases"
        result = self.query(sql=sql)
        db_list = [
            row[0] for row in result.rows if row[0] not in self.forbidden_databases
        ]
        result.rows = db_list
        return result

    forbidden_tables = ["test"]

    def get_all_tables(self, db_name, **kwargs):
        """Get table list, return a ResultSet."""
        sql = "show tables"
        result = self.query(db_name=db_name, sql=sql)
        tb_list = [row[0] for row in result.rows if row[0] not in self.forbidden_tables]
        result.rows = tb_list
        return result

    def get_group_tables_by_db(self, db_name):
        data = {}
        sql = f"""SELECT TABLE_NAME,
                            TABLE_COMMENT
                        FROM
                            information_schema.TABLES
                        WHERE
                            TABLE_SCHEMA=%(db_name)s;"""
        result = self.query(db_name=db_name, sql=sql, parameters={"db_name": db_name})
        for row in result.rows:
            table_name, table_cmt = row[0], row[1]
            if table_name[0] not in data:
                data[table_name[0]] = list()
            data[table_name[0]].append([table_name, table_cmt])
        return data

    def get_table_meta_data(self, db_name, tb_name, **kwargs):
        """Get table meta for data dictionary page.
        Returns dict: {"column_list": [], "rows": []}.
        """
        sql = f"""SELECT
                        TABLE_NAME as table_name,
                        ENGINE as engine,
                        ROW_FORMAT as row_format,
                        TABLE_ROWS as table_rows,
                        AVG_ROW_LENGTH as avg_row_length,
                        round(DATA_LENGTH/1024, 2) as data_length,
                        MAX_DATA_LENGTH as max_data_length,
                        round(INDEX_LENGTH/1024, 2) as index_length,
                        round((DATA_LENGTH + INDEX_LENGTH)/1024, 2) as data_total,
                        DATA_FREE as data_free,
                        AUTO_INCREMENT as auto_increment,
                        TABLE_COLLATION as table_collation,
                        CREATE_TIME as create_time,
                        CHECK_TIME as check_time,
                        UPDATE_TIME as update_time,
                        TABLE_COMMENT as table_comment
                    FROM
                        information_schema.TABLES
                    WHERE
                        TABLE_SCHEMA=%(db_name)s
                            AND TABLE_NAME=%(tb_name)s"""
        _meta_data = self.query(
            db_name, sql, parameters={"db_name": db_name, "tb_name": tb_name}
        )
        return {"column_list": _meta_data.column_list, "rows": _meta_data.rows[0]}

    def get_table_desc_data(self, db_name, tb_name, **kwargs):
        """Get table column metadata."""
        sql = f"""SELECT 
                        COLUMN_NAME as 'Column Name',
                        COLUMN_TYPE as 'Column Type',
                        CHARACTER_SET_NAME as 'Character Set',
                        IS_NULLABLE as 'Nullable',
                        COLUMN_KEY as 'Index Column',
                        COLUMN_DEFAULT as 'Default Value',
                        EXTRA as 'Extra',
                        COLUMN_COMMENT as 'Comment'
                    FROM
                        information_schema.COLUMNS
                    WHERE
                        TABLE_SCHEMA = %(db_name)s
                            AND TABLE_NAME = %(tb_name)s
                    ORDER BY ORDINAL_POSITION;"""
        _desc_data = self.query(
            db_name, sql, parameters={"db_name": db_name, "tb_name": tb_name}
        )
        return {"column_list": _desc_data.column_list, "rows": _desc_data.rows}

    def get_table_index_data(self, db_name, tb_name, **kwargs):
        """Get table index metadata."""
        sql = f"""SELECT
                        COLUMN_NAME as 'Column Name',
                        INDEX_NAME as 'Index Name',
                        NON_UNIQUE as 'Non Unique',
                        SEQ_IN_INDEX as 'Sequence In Index',
                        CARDINALITY as 'Cardinality',
                        NULLABLE as 'Nullable',
                        INDEX_TYPE as 'Index Type',
                        COMMENT as 'Comment'
                    FROM
                        information_schema.STATISTICS
                    WHERE
                        TABLE_SCHEMA = %(db_name)s
                    AND TABLE_NAME = %(tb_name)s;"""
        _index_data = self.query(
            db_name, sql, parameters={"db_name": db_name, "tb_name": tb_name}
        )
        return {"column_list": _index_data.column_list, "rows": _index_data.rows}

    def get_tables_metas_data(self, db_name, **kwargs):
        """Get all table metadata in a DB for dictionary export."""
        sql_tbs = f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=%(db_name)s ORDER BY TABLE_SCHEMA,TABLE_NAME;"
        tbs = self.query(
            sql=sql_tbs,
            cursorclass=MySQLdb.cursors.DictCursor,
            close_conn=False,
            parameters={"db_name": db_name},
        ).rows
        table_metas = []
        for tb in tbs:
            _meta = dict()
            engine_keys = [
                {"key": "COLUMN_NAME", "value": "Column Name"},
                {"key": "COLUMN_TYPE", "value": "Data Type"},
                {"key": "COLUMN_DEFAULT", "value": "Default Value"},
                {"key": "IS_NULLABLE", "value": "Nullable"},
                {"key": "EXTRA", "value": "Auto Increment"},
                {"key": "COLUMN_KEY", "value": "Primary Key"},
                {"key": "COLUMN_COMMENT", "value": "Comment"},
            ]
            _meta["ENGINE_KEYS"] = engine_keys
            _meta["TABLE_INFO"] = tb
            sql_cols = f"""SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                            WHERE TABLE_SCHEMA='{tb['TABLE_SCHEMA']}' AND TABLE_NAME='{tb['TABLE_NAME']}'
                            ORDER BY TABLE_SCHEMA,TABLE_NAME,ORDINAL_POSITION;"""
            _meta["COLUMNS"] = self.query(
                sql=sql_cols, cursorclass=MySQLdb.cursors.DictCursor, close_conn=False
            ).rows
            table_metas.append(_meta)
        return table_metas

    def get_bind_users(self, db_name: str):
        sql_get_bind_users = f"""select group_concat(distinct(GRANTEE)),TABLE_SCHEMA
                from information_schema.SCHEMA_PRIVILEGES
                where TABLE_SCHEMA=%(db_name)s
                group by TABLE_SCHEMA;"""
        return self.query(
            "information_schema",
            sql_get_bind_users,
            close_conn=False,
            parameters={"db_name": db_name},
        ).rows

    def get_all_databases_summary(self):
        """Instance DB management: get summary for all databases."""
        # Get all databases.
        sql_get_db = """SELECT SCHEMA_NAME,DEFAULT_CHARACTER_SET_NAME,DEFAULT_COLLATION_NAME 
        FROM information_schema.SCHEMATA
        WHERE SCHEMA_NAME NOT IN ('information_schema', 'performance_schema', 'mysql', 'test', 'sys');"""
        query_result = self.query("information_schema", sql_get_db, close_conn=False)
        if not query_result.error:
            dbs = query_result.rows
            # Get users bound to each database.
            rows = []
            for db in dbs:
                bind_users = self.get_bind_users(db_name=db[0])
                row = {
                    "db_name": db[0],
                    "charset": db[1],
                    "collation": db[2],
                    "grantees": bind_users[0][0].split(",") if bind_users else [],
                    "saved": False,
                }
                rows.append(row)
            query_result.rows = rows
        return query_result

    def get_instance_users_summary(self):
        """Instance account management: get summary for all users."""
        server_version = self.server_version
        sql_get_user_with_account_locked = "select concat('`', user, '`', '@', '`', host,'`') as query,user,host,account_locked from mysql.user;"
        sql_get_user_without_account_locked = "select concat('`', user, '`', '@', '`', host,'`') as query,user,host from mysql.user;"
        # MySQL >= 5.7.6 and MariaDB >= 10.4.2 support ACCOUNT LOCK.
        if (
            self.server_fork_type == MysqlForkType.MYSQL and server_version >= (5, 7, 6)
        ) or (
            self.server_fork_type == MysqlForkType.MARIADB
            and self.server_version >= (10, 4, 2)
        ):
            support_account_lock = True
            sql_get_user = sql_get_user_with_account_locked
        else:
            support_account_lock = False
            sql_get_user = sql_get_user_without_account_locked
        query_result = self.query("mysql", sql_get_user)
        if query_result.error and sql_get_user == sql_get_user_with_account_locked:
            # Query failed, fallback to SQL without lock info.
            query_result = self.query("mysql", sql_get_user_without_account_locked)
        if not query_result.error:
            db_users = query_result.rows
            # Get user privilege info.
            rows = []
            for db_user in db_users:
                user_host = db_user[0]
                user_priv = self.query(
                    "mysql", "show grants for {};".format(user_host), close_conn=False
                ).rows
                row = {
                    "user_host": user_host,
                    "user": db_user[1],
                    "host": db_user[2],
                    "privileges": user_priv,
                    "saved": False,
                    "is_locked": (
                        db_user[3]
                        if support_account_lock and len(db_user) == 4
                        else None
                    ),
                }
                rows.append(row)
            query_result.rows = rows
        return query_result

    def create_instance_user(self, **kwargs):
        """Instance account management: create account."""
        # escape
        user = self.escape_string(kwargs.get("user", ""))
        host = self.escape_string(kwargs.get("host", ""))
        password1 = self.escape_string(kwargs.get("password1", ""))
        remark = kwargs.get("remark", "")
        # Execute within one transaction.
        hosts = host.split("|")
        create_user_cmd = ""
        accounts = []
        for host in hosts:
            create_user_cmd += (
                f"create user '{user}'@'{host}' identified by '{password1}';"
            )
            accounts.append(
                {
                    "instance": self.instance,
                    "user": user,
                    "host": host,
                    "password": password1,
                    "remark": remark,
                }
            )
        exec_result = self.execute(db_name="mysql", sql=create_user_cmd)
        exec_result.rows = accounts
        return exec_result

    def drop_instance_user(self, user_host: str, **kwarg):
        """Instance account management: drop account."""
        # escape
        user_host = self.escape_string(user_host)
        return self.execute(db_name="mysql", sql=f"DROP USER {user_host};")

    def reset_instance_user_pwd(self, user_host: str, reset_pwd: str, **kwargs):
        """Instance account management: reset account password."""
        # escape
        user_host = self.escape_string(user_host)
        reset_pwd = self.escape_string(reset_pwd)
        return self.execute(
            db_name="mysql", sql=f"ALTER USER {user_host} IDENTIFIED BY '{reset_pwd}';"
        )

    def get_all_columns_by_tb(self, db_name, tb_name, **kwargs):
        """Get all columns, return a ResultSet."""
        db_name = self.escape_string(db_name)
        tb_name = self.escape_string(tb_name)
        sql = f"""SELECT
            COLUMN_NAME,
            COLUMN_TYPE,
            CHARACTER_SET_NAME,
            IS_NULLABLE,
            COLUMN_KEY,
            EXTRA,
            COLUMN_COMMENT
        FROM
            information_schema.COLUMNS
        WHERE
            TABLE_SCHEMA = '{db_name}'
                AND TABLE_NAME = '{tb_name}'
        ORDER BY ORDINAL_POSITION;"""
        result = self.query(
            db_name=db_name,
            sql=sql,
            parameters=({"db_name": db_name, "tb_name": tb_name}),
        )
        column_list = [row[0] for row in result.rows]
        result.rows = column_list
        return result

    def describe_table(self, db_name, tb_name, **kwargs):
        """Return ResultSet for `show create table`."""
        tb_name = self.escape_string(tb_name)
        sql = f"show create table `{tb_name}`;"
        result = self.query(db_name=db_name, sql=sql)
        return result

    @staticmethod
    def result_set_binary_as_hex(result_set):
        """Convert binary columns in ResultSet rows to hex strings."""
        new_rows, hex_column_index = [], []
        for idx, _type in enumerate(result_set.column_type):
            if _type in ["TINY_BLOB", "MEDIUM_BLOB", "LONG_BLOB", "BLOB"]:
                hex_column_index.append(idx)
        if hex_column_index:
            for row in result_set.rows:
                row = list(row)
                for index in hex_column_index:
                    row[index] = row[index].hex() if row[index] else row[index]
                new_rows.append(row)
        result_set.rows = tuple(new_rows)
        return result_set

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
        max_execution_time = kwargs.get("max_execution_time", 0)
        cursorclass = kwargs.get("cursorclass") or MySQLdb.cursors.Cursor
        try:
            conn = self.get_connection(db_name=db_name)
            conn.autocommit(True)
            cursor = conn.cursor(cursorclass)
            try:
                cursor.execute(f"set session max_execution_time={max_execution_time};")
            except MySQLdb.OperationalError:
                pass
            # This engine intentionally executes SQL that passed Archery's query checks.
            # codeql[py/sql-injection]
            effect_row = cursor.execute(sql, parameters)
            if int(limit_num) > 0:
                rows = cursor.fetchmany(size=int(limit_num))
            else:
                rows = cursor.fetchall()
            fields = cursor.description

            result_set.column_list = [i[0] for i in fields] if fields else []
            result_set.column_type = (
                [column_types_map.get(i[1], "") for i in fields] if fields else []
            )
            result_set.rows = rows
            result_set.affected_rows = effect_row
            if kwargs.get("binary_as_hex"):
                result_set = self.result_set_binary_as_hex(result_set)
        except Exception as e:
            logger.warning("%s statement execution failed", self.name, exc_info=True)
            result_set.error = str(e)
        finally:
            if close_conn:
                self.close()
        return result_set

    def query_check(self, db_name=None, sql=""):
        # Query checks: strip comments and split statements.
        result = {"msg": "", "bad_query": False, "filtered_sql": sql, "has_star": False}
        # Remove comments, validate syntax, and keep the first valid SQL.
        try:
            sql = sqlparse.format(sql, strip_comments=True)
            sql = sqlparse.split(sql)[0]
            result["filtered_sql"] = sql.strip()
        except IndexError:
            result["bad_query"] = True
            result["msg"] = "No valid SQL statement"
        if re.match(r"^select|^show|^explain", sql, re.I) is None:
            result["bad_query"] = True
            result["msg"] = "Unsupported query syntax type!"
        if "*" in sql:
            result["has_star"] = True
            result["msg"] = "SQL contains * "
        # For SELECT, run EXPLAIN first to validate syntax.
        if re.match(r"^select", sql, re.I):
            explain_result = self.query(db_name=db_name, sql=f"explain {sql}")
            if explain_result.error:
                result["bad_query"] = True
                result["msg"] = explain_result.error
        # Access to mysql.user should not be allowed.
        normalized_sql = sql.lower().replace("`", " ")
        normalized_sql = " ".join(normalized_sql.replace("\n", " ").split())
        normalized_sql = re.sub(r"\s*\.\s*", ".", normalized_sql)
        references_mysql_user = "mysql.user" in normalized_sql
        references_user_table = any(
            token.strip(";") == "user" for token in normalized_sql.split()
        )
        if references_mysql_user or (db_name == "mysql" and references_user_table):
            result["bad_query"] = True
            result["msg"] = "You do not have permission to view this table"

        return result

    def filter_sql(self, sql="", limit_num=0):
        # Enforce limit for query SQL; normalize limit styles.
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

    def query_masking(self, db_name=None, sql="", resultset=None):
        """Given SQL, DB name, and result set, return masked result set."""
        # Only mask SELECT statements.
        if re.match(r"^select", sql, re.I):
            mask_result = simple_column_mask(self.instance, resultset)
            mask_result.is_masked = True
        else:
            mask_result = resultset
        return mask_result

    def execute_check(self, db_name=None, sql=""):
        """Pre-check before workflow execution, return ReviewSet."""
        check_result = ReviewSet(full_sql=sql)
        critical_ddl_regex = self.config.get("critical_ddl_regex", "")
        ddl_dml_separation = self.config.get("ddl_dml_separation", False)
        p = re.compile(critical_ddl_regex) if critical_ddl_regex else None
        ddl_dml_flag = ""
        line = 1

        for statement in sqlparse.split(sql):
            statement = remove_comments(statement, db_type="mysql").strip()
            statement = statement.rstrip(";")
            if not statement:
                continue

            syntax_type = get_syntax_type(statement, parser=False, db_type="mysql")
            if re.match(r"^select", statement.lower()):
                row = ReviewResult(
                    id=line,
                    errlevel=2,
                    stagestatus="Rejected unsupported statement",
                    errormessage=(
                        "Only DML and DDL statements are supported. "
                        "Use SQL query feature for SELECT statements!"
                    ),
                    sql=statement,
                )
            elif critical_ddl_regex and p.match(statement.strip().lower()):
                row = ReviewResult(
                    id=line,
                    errlevel=2,
                    stagestatus="Rejected high-risk SQL",
                    errormessage=(
                        "Submitting statements matching "
                        + critical_ddl_regex
                        + " is prohibited!"
                    ),
                    sql=statement,
                )
            elif syntax_type not in ("DDL", "DML"):
                row = ReviewResult(
                    id=line,
                    errlevel=2,
                    stagestatus="Rejected unsupported statement",
                    errormessage=(
                        "Only DML and DDL statements are supported. "
                        "Use SQL query feature for SELECT statements!"
                    ),
                    sql=statement,
                )
            else:
                row = ReviewResult(
                    id=line,
                    errlevel=0,
                    stagestatus="Audit completed",
                    errormessage="None",
                    sql=statement,
                    affected_rows=0,
                    execute_time=0,
                )

                if ddl_dml_separation and syntax_type in ("DDL", "DML"):
                    if ddl_dml_flag == "":
                        ddl_dml_flag = syntax_type
                    elif ddl_dml_flag != syntax_type:
                        row.stagestatus = "Rejected unsupported statement"
                        row.errlevel = 2
                        row.errormessage = (
                            "DDL and DML statements cannot be executed together!"
                        )

            if syntax_type == "DDL":
                check_result.syntax_type = 1
            elif syntax_type == "DML" and check_result.syntax_type == 0:
                check_result.syntax_type = 2
            check_result.rows.append(row)
            line += 1

        if not check_result.rows:
            check_result.rows.append(
                ReviewResult(
                    id=1,
                    errlevel=2,
                    stagestatus="Rejected empty SQL",
                    errormessage="No executable SQL statements found.",
                    sql=sql,
                )
            )

        check_result.checked = True
        for row in check_result.rows:
            if row.errlevel == 1:
                check_result.warning_count += 1
            elif row.errlevel == 2:
                check_result.error_count += 1
        return check_result

    def execute_workflow(self, workflow, execution_options=None):
        """Execute workflow, return ReviewSet."""
        # Check whether instance is read-only.
        read_only = self.query(sql="SELECT @@global.read_only;").rows[0][0]
        if read_only in (1, "ON"):
            result = ReviewSet(
                full_sql=workflow.sqlworkflowcontent.sql_content,
                rows=[
                    ReviewResult(
                        id=1,
                        errlevel=2,
                        stagestatus="Execute Failed",
                        errormessage=(
                            "Instance read_only=1, executing change statements "
                            "is forbidden!"
                        ),
                        sql=workflow.sqlworkflowcontent.sql_content,
                    )
                ],
            )
            result.error = (
                "Instance read_only=1, executing change statements is forbidden!",
            )
            return result
        if workflow.syntax_type == 1 and self._workflow_contains_only_ddl(workflow):
            return self.execute_ddl_workflow(
                workflow=workflow, execution_options=execution_options or {}
            )
        return self.execute_direct_workflow(workflow=workflow, executor_id="direct")

    def get_ddl_executor_inspection(self, workflow):
        statements = self._ddl_executor_statements(workflow)
        service = MysqlDDLExecutorService(self)
        try:
            return service.inspect_workflow(workflow, statements)
        finally:
            self.close()

    def resolve_ddl_executor(self, workflow, executor_id=None):
        statements = self._ddl_executor_statements(workflow)
        service = MysqlDDLExecutorService(self)
        try:
            return service.resolve_executor(
                workflow=workflow,
                statements=statements,
                requested_executor=executor_id,
            )
        finally:
            self.close()

    def preflight_ddl_executor(self, workflow, executor_id=None):
        statements = self._ddl_executor_statements(workflow)
        service = MysqlDDLExecutorService(self)
        try:
            resolved = service.resolve_executor(
                workflow=workflow,
                statements=statements,
                requested_executor=executor_id,
            )
            service.preflight(
                workflow=workflow,
                statements=statements,
                resolved_executor=resolved,
            )
            return resolved
        finally:
            self.close()

    def execute_ddl_workflow(self, workflow, execution_options=None):
        executor_id = (execution_options or {}).get("executor")
        if self._should_execute_direct_ddl(workflow, executor_id):
            return self.execute_direct_workflow(
                workflow=workflow, executor_id=executor_id or "direct"
            )

        statements = self._ddl_executor_statements(workflow)
        service = MysqlDDLExecutorService(self)
        try:
            resolved = service.resolve_executor(
                workflow=workflow,
                statements=statements,
                requested_executor=executor_id,
            )
            if resolved.executor_id == "direct":
                return self.execute_direct_workflow(
                    workflow=workflow, executor_id=resolved.executor_id
                )
            return self.execute_external_ddl_workflow(
                workflow=workflow,
                statements=statements,
                service=service,
                resolved_executor=resolved,
            )
        except MysqlDDLExecutorError as e:
            logger.warning("MySQL DDL workflow execution failed", exc_info=True)
            execute_result = ReviewSet(full_sql=workflow.sqlworkflowcontent.sql_content)
            execute_result.error = "Execution failed"
            execute_result.rows.append(
                ReviewResult(
                    id=1,
                    errlevel=2,
                    stagestatus="Execute Failed",
                    errormessage="Execution failed",
                    sql=workflow.sqlworkflowcontent.sql_content,
                    executor=executor_id or "",
                )
            )
            return execute_result
        finally:
            self.close()

    def execute_external_ddl_workflow(
        self, workflow, statements, service, resolved_executor
    ):
        execute_result = ReviewSet(full_sql=workflow.sqlworkflowcontent.sql_content)
        try:
            execute_result.rows = service.execute(
                workflow=workflow,
                statements=statements,
                resolved_executor=resolved_executor,
            )
        except MysqlDDLExecutorError as e:
            logger.warning("External MySQL DDL execution failed", exc_info=True)
            execute_result.error = "Execution failed"
            execute_result.rows.append(
                ReviewResult(
                    id=1,
                    errlevel=2,
                    stagestatus="Execute Failed",
                    errormessage="Execution failed",
                    sql=workflow.sqlworkflowcontent.sql_content,
                    executor=resolved_executor.executor_id,
                )
            )
        return execute_result

    def execute_direct_workflow(self, workflow, executor_id="direct"):
        """Execute reviewed statements natively without OSC tooling."""
        sql = workflow.sqlworkflowcontent.sql_content
        execute_result = ReviewSet(full_sql=sql)
        statements = self._direct_workflow_statements(workflow)

        if not statements:
            execute_result.error = "No executable SQL statements found."
            execute_result.rows.append(
                ReviewResult(
                    id=1,
                    errlevel=2,
                    stagestatus="Execute Failed",
                    errormessage="No executable SQL statements found.",
                    sql=sql,
                )
            )
            return execute_result

        conn = None
        cursor = None
        current_statement = sql

        try:
            conn = self.get_connection(db_name=workflow.db_name)
            cursor = conn.cursor()
            try:
                conn.autocommit(False)
            except Exception:
                pass

            for line, statement in enumerate(statements, start=1):
                current_statement = statement
                with FuncTimer() as timer:
                    cursor.execute(statement)
                rowcount = getattr(cursor, "rowcount", 0)
                affected_rows = (
                    rowcount if isinstance(rowcount, int) and rowcount > 0 else 0
                )
                execute_result.rows.append(
                    ReviewResult(
                        id=line,
                        errlevel=0,
                        stagestatus="Execute Successfully",
                        errormessage="None",
                        sql=statement,
                        affected_rows=affected_rows,
                        execute_time=timer.cost,
                        executor=executor_id,
                    )
                )
            conn.commit()
        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            logger.warning("%s direct execution failed", self.name, exc_info=True)
            execute_result.error = "Execution failed"
            execute_result.rows.append(
                ReviewResult(
                    id=len(execute_result.rows) + 1,
                    errlevel=2,
                    stagestatus="Execute Failed",
                    errormessage="Execution failed",
                    sql=current_statement,
                    executor=executor_id,
                )
            )
        finally:
            try:
                if cursor:
                    cursor.close()
            except Exception:
                pass
            self.close()

        return execute_result

    def _direct_workflow_statements(self, workflow):
        """Prefer reviewed statements so direct execution matches the checked SQL."""
        review_content = workflow.sqlworkflowcontent.review_content or "[]"
        statements = []

        try:
            review_rows = json.loads(review_content)
        except (TypeError, json.JSONDecodeError):
            review_rows = []

        for row in review_rows:
            if isinstance(row, list):
                review_row = ReviewResult(inception_result=row)
            elif isinstance(row, dict):
                review_row = ReviewResult(**row)
            else:
                continue

            statement = remove_comments(review_row.sql, db_type="mysql").strip()
            if statement:
                statements.append(statement.rstrip(";"))

        if statements:
            return statements

        normalized_sql = sqlparse.format(
            workflow.sqlworkflowcontent.sql_content,
            strip_comments=True,
        )
        return [
            statement.strip().rstrip(";")
            for statement in sqlparse.split(normalized_sql)
            if statement.strip()
        ]

    def _ddl_executor_statements(self, workflow):
        """Drop DB context statements because OSC tools operate on explicit db/table flags."""
        return [
            statement
            for statement in self._direct_workflow_statements(workflow)
            if not re.match(r"^\s*use\b", statement, re.IGNORECASE)
        ]

    def _workflow_contains_only_ddl(self, workflow):
        statements = self._ddl_executor_statements(workflow)
        if not statements:
            return False
        return all(
            get_syntax_type(statement, parser=False, db_type="mysql") == "DDL"
            for statement in statements
        )

    def _has_configured_online_ddl_executors(self):
        return bool(
            self.config.get("gh_ost", "").strip()
            or self.config.get("pt_osc", "").strip()
        )

    def _should_execute_direct_ddl(self, workflow, executor_id=None):
        if executor_id == "direct":
            return True
        if executor_id:
            return False
        return (
            self._workflow_contains_only_ddl(workflow)
            and not self._has_configured_online_ddl_executors()
        )

    def execute(self, db_name=None, sql="", close_conn=True, parameters=None):
        """Execute statements natively."""
        result = ResultSet(full_sql=sql)
        conn = self.get_connection(db_name=db_name)
        try:
            cursor = conn.cursor()
            for statement in sqlparse.split(sql):
                cursor.execute(statement, parameters)
            conn.commit()
            cursor.close()
        except Exception as e:
            logger.warning("%s statement execution failed", self.name, exc_info=True)
            result.error = str(e)
        if close_conn:
            self.close()
        return result

    def get_variables(self, variables=None):
        """Get instance variables."""
        if variables:
            variables = (
                "','".join(variables)
                if isinstance(variables, list)
                else "','".join(list(variables))
            )
            db = (
                "performance_schema"
                if self.server_version > (5, 7)
                else "information_schema"
            )
            sql = f"""select * from {db}.global_variables where variable_name in ('{variables}');"""
        else:
            sql = "show global variables;"
        return self.query(sql=sql)

    def set_variable(self, variable_name, variable_value):
        """Set instance variable value."""
        sql = f"""set global {variable_name}={variable_value};"""
        return self.query(sql=sql)

    def processlist(
        self,
        command_type,
        base_sql="select id, user, host, db, command, time, state, ifnull(info,'') as info from information_schema.processlist",
        **kwargs,
    ):
        """Get process/connection info."""
        # escape
        command_type = self.escape_string(command_type)
        if not command_type:
            command_type = "Query"
        if command_type == "All":
            sql = base_sql + ";"
        elif command_type == "Not Sleep":
            sql = "{} where command<>'Sleep';".format(base_sql)
        else:
            sql = "{} where command= '{}';".format(base_sql, command_type)

        return self.query("information_schema", sql)

    def get_kill_command(self, thread_ids, thread_ids_check=True):
        """Generate kill command from thread ID list."""
        # Validate input.
        if thread_ids_check:
            if [i for i in thread_ids if not isinstance(i, int)]:
                return None
        sql = "select concat('kill ', id, ';') from information_schema.processlist where id in ({});".format(
            ",".join(str(tid) for tid in thread_ids)
        )
        all_kill_sql = self.query("information_schema", sql)
        kill_sql = ""
        for row in all_kill_sql.rows:
            kill_sql = kill_sql + row[0]

        return kill_sql

    def kill(self, thread_ids, thread_ids_check=True):
        """Kill threads."""
        # Validate input.
        if thread_ids_check:
            if [i for i in thread_ids if not isinstance(i, int)]:
                return ResultSet(full_sql="")
        sql = "select concat('kill ', id, ';') from information_schema.processlist where id in ({});".format(
            ",".join(str(tid) for tid in thread_ids)
        )
        all_kill_sql = self.query("information_schema", sql)
        kill_sql = ""
        for row in all_kill_sql.rows:
            kill_sql = kill_sql + row[0]
        return self.execute("information_schema", kill_sql)

    def tablespace(self, offset=0, row_count=14):
        """Get tablespace information."""
        sql = """
        SELECT
          table_schema AS table_schema,
          table_name AS table_name,
          engine AS engine,
          TRUNCATE((data_length+index_length+data_free)/1024/1024,2) AS total_size,
          table_rows AS table_rows,
          TRUNCATE(data_length/1024/1024,2) AS data_size,
          TRUNCATE(index_length/1024/1024,2) AS index_size,
          TRUNCATE(data_free/1024/1024,2) AS data_free,
          TRUNCATE(data_free/(data_length+index_length+data_free)*100,2) AS pct_free
        FROM information_schema.tables 
        WHERE table_schema NOT IN ('information_schema', 'performance_schema', 'mysql', 'test', 'sys')
          ORDER BY total_size DESC 
        LIMIT {},{};""".format(offset, row_count)
        return self.query("information_schema", sql)

    def tablespace_count(self):
        """Get tablespace count."""
        sql = """
        SELECT count(*)
        FROM information_schema.tables 
        WHERE table_schema NOT IN ('information_schema', 'performance_schema', 'mysql', 'test', 'sys')"""
        return self.query("information_schema", sql)

    def trxandlocks(self):
        """Get lock wait information."""
        server_version = self.server_version
        if server_version < (8, 0, 1):
            sql = """
                SELECT
                rtrx.`trx_state`                                                        AS "Waiting State",
                rtrx.`trx_started`                                                      AS "Waiting Transaction Start Time",
                rtrx.`trx_wait_started`                                                 AS "Waiting Transaction Wait Start Time",
                lw.`requesting_trx_id`                                                  AS "Waiting Transaction ID",
                rtrx.trx_mysql_thread_id                                                AS "Waiting Thread ID",
                rtrx.`trx_query`                                                        AS "Waiting Transaction SQL",
                CONCAT(rl.`lock_mode`, '-', rl.`lock_table`, '(', rl.`lock_index`, ')') AS "Waiting Table Info",
                rl.`lock_id`                                                            AS "Waiting Lock ID",
                lw.`blocking_trx_id`                                                    AS "Blocking Transaction ID",
                trx.trx_mysql_thread_id                                                 AS "Blocking Thread ID",
                CONCAT(l.`lock_mode`, '-', l.`lock_table`, '(', l.`lock_index`, ')')    AS "Blocking Table Info",
                l.lock_id                                                               AS "Blocking Lock ID",
                trx.`trx_state`                                                         AS "Blocking Transaction State",
                trx.`trx_started`                                                       AS "Blocking Transaction Start Time",
                trx.`trx_wait_started`                                                  AS "Blocking Transaction Wait Start Time",
                trx.`trx_query`                                                         AS "Blocking Transaction SQL"
                FROM information_schema.`INNODB_LOCKS` rl
                , information_schema.`INNODB_LOCKS` l
                , information_schema.`INNODB_LOCK_WAITS` lw
                , information_schema.`INNODB_TRX` rtrx
                , information_schema.`INNODB_TRX` trx
                WHERE rl.`lock_id` = lw.`requested_lock_id`
                    AND l.`lock_id` = lw.`blocking_lock_id`
                    AND lw.requesting_trx_id = rtrx.trx_id
                    AND lw.blocking_trx_id = trx.trx_id;"""

        else:
            sql = """
                SELECT
                rtrx.`trx_state`                                                           AS "Waiting State",
                rtrx.`trx_started`                                                         AS "Waiting Transaction Start Time",
                rtrx.`trx_wait_started`                                                    AS "Waiting Transaction Wait Start Time",
                lw.`REQUESTING_ENGINE_TRANSACTION_ID`                                      AS "Waiting Transaction ID",
                rtrx.trx_mysql_thread_id                                                   AS "Waiting Thread ID",
                rtrx.`trx_query`                                                           AS "Waiting Transaction SQL",
                CONCAT(rl.`lock_mode`, '-', rl.`OBJECT_SCHEMA`, '(', rl.`INDEX_NAME`, ')') AS "Waiting Table Info",
                rl.`ENGINE_LOCK_ID`                                                        AS "Waiting Lock ID",
                lw.`BLOCKING_ENGINE_TRANSACTION_ID`                                        AS "Blocking Transaction ID",
                trx.trx_mysql_thread_id                                                    AS "Blocking Thread ID",
                CONCAT(l.`lock_mode`, '-', l.`OBJECT_SCHEMA`, '(', l.`INDEX_NAME`, ')')    AS "Blocking Table Info",
                l.ENGINE_LOCK_ID                                                           AS "Blocking Lock ID",
                trx.`trx_state`                                                            AS "Blocking Transaction State",
                trx.`trx_started`                                                          AS "Blocking Transaction Start Time",
                trx.`trx_wait_started`                                                     AS "Blocking Transaction Wait Start Time",
                trx.`trx_query`                                                            AS "Blocking Transaction SQL"
                FROM performance_schema.`data_locks` rl
                , performance_schema.`data_locks` l
                , performance_schema.`data_lock_waits` lw
                , information_schema.`INNODB_TRX` rtrx
                , information_schema.`INNODB_TRX` trx
                WHERE rl.`ENGINE_LOCK_ID` = lw.`REQUESTING_ENGINE_LOCK_ID`
                    AND l.`ENGINE_LOCK_ID` = lw.`BLOCKING_ENGINE_LOCK_ID`
                    AND lw.REQUESTING_ENGINE_TRANSACTION_ID = rtrx.trx_id
                    AND lw.BLOCKING_ENGINE_TRANSACTION_ID = trx.trx_id;"""

        return self.query("information_schema", sql)

    def get_long_transaction(self, thread_time=3):
        """Get long-running transactions."""
        sql = """select trx.trx_started,
        trx.trx_state,
        trx.trx_operation_state,
        trx.trx_mysql_thread_id,
        trx.trx_tables_locked,
        trx.trx_rows_locked,
        trx.trx_rows_modified,
        trx.trx_is_read_only,
        trx.trx_isolation_level,
        p.user,
        p.host,
        p.db,
        TO_SECONDS(NOW()) - TO_SECONDS(trx.trx_started) trx_idle_time,
        p.time thread_time,
        IFNULL((SELECT
        GROUP_CONCAT(t1.sql_text order by t1.TIMER_START desc SEPARATOR ';
        ')
        FROM performance_schema.events_statements_history t1
        INNER JOIN performance_schema.threads t2
            ON t1.thread_id = t2.thread_id
        WHERE t2.PROCESSLIST_ID = p.id), '') info
        FROM information_schema.INNODB_TRX trx
        INNER JOIN information_schema.PROCESSLIST p
        ON trx.trx_mysql_thread_id = p.id
        WHERE trx.trx_state = 'RUNNING'
        AND p.COMMAND = 'Sleep'
        AND p.time > {}
        ORDER BY trx.trx_started ASC;""".format(thread_time)

        return self.query("information_schema", sql)

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
