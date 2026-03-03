# -*- coding: UTF-8 -*-
# https://stackoverflow.com/questions/7942520/relationship-between-catalog-schema-user-and-database-instance
import logging
import traceback
import re
import sqlparse
import MySQLdb
import simplejson as json
import threading
import pandas as pd
from common.config import SysConfig
from common.utils.timer import FuncTimer
from sql.utils.sql_utils import (
    get_syntax_type,
    get_full_sqlitem_list,
    get_exec_sqlitem_list,
)
from . import EngineBase
import cx_Oracle
from .models import ResultSet, ReviewSet, ReviewResult
from sql.utils.data_masking import simple_column_mask

logger = logging.getLogger("default")


class OracleEngine(EngineBase):
    test_query = "SELECT 1 FROM DUAL"

    def __init__(self, instance=None):
        super(OracleEngine, self).__init__(instance=instance)
        if instance:
            self.service_name = instance.service_name
            self.sid = instance.sid

    def get_connection(self, db_name=None):
        if self.conn:
            return self.conn
        if self.sid:
            dsn = cx_Oracle.makedsn(self.host, self.port, self.sid)
            self.conn = cx_Oracle.connect(
                self.user, self.password, dsn=dsn, encoding="UTF-8", nencoding="UTF-8"
            )
        elif self.service_name:
            dsn = cx_Oracle.makedsn(
                self.host, self.port, service_name=self.service_name
            )
            self.conn = cx_Oracle.connect(
                self.user, self.password, dsn=dsn, encoding="UTF-8", nencoding="UTF-8"
            )
        else:
            raise ValueError(
                "Both sid and dsn are missing. Please update instance config in admin page."
            )
        return self.conn

    name = "Oracle"

    info = "Oracle engine"

    @property
    def auto_backup(self):
        """Whether backup is supported."""
        return True

    @staticmethod
    def get_backup_connection():
        """Backup database connection."""
        archer_config = SysConfig()
        backup_host = archer_config.get("inception_remote_backup_host")
        backup_port = int(archer_config.get("inception_remote_backup_port", 3306))
        backup_user = archer_config.get("inception_remote_backup_user")
        backup_password = archer_config.get("inception_remote_backup_password")
        return MySQLdb.connect(
            host=backup_host,
            port=backup_port,
            user=backup_user,
            passwd=backup_password,
            charset="utf8mb4",
            autocommit=True,
        )

    @property
    def server_version(self):
        conn = self.get_connection()
        version = conn.version
        return tuple([n for n in version.split(".")[:3]])

    def get_all_databases(self):
        """Get database list for upper layer.
        Internally this returns Oracle schema list.
        """
        return self._get_all_schemas()

    def _get_all_databases(self):
        """Get database list, return a ResultSet."""
        sql = "select name from v$database"
        result = self.query(sql=sql)
        db_list = [row[0] for row in result.rows]
        result.rows = db_list
        return result

    def _get_all_instances(self):
        """Get instance list, return a ResultSet."""
        sql = "select instance_name from v$instance"
        result = self.query(sql=sql)
        instance_list = [row[0] for row in result.rows]
        result.rows = instance_list
        return result

    def _get_all_schemas(self):
        """
        Get schema list.
        """
        result = self.query(sql="SELECT username FROM all_users order by username")
        sysschema = (
            "AUD_SYS",
            "ANONYMOUS",
            "APEX_030200",
            "APEX_PUBLIC_USER",
            "APPQOSSYS",
            "BI USERS",
            "CTXSYS",
            "DBSNMP",
            "DIP USERS",
            "EXFSYS",
            "FLOWS_FILES",
            "HR USERS",
            "IX USERS",
            "MDDATA",
            "MDSYS",
            "MGMT_VIEW",
            "OE USERS",
            "OLAPSYS",
            "ORACLE_OCM",
            "ORDDATA",
            "ORDPLUGINS",
            "ORDSYS",
            "OUTLN",
            "OWBSYS",
            "OWBSYS_AUDIT",
            "PM USERS",
            "SCOTT",
            "SH USERS",
            "SI_INFORMTN_SCHEMA",
            "SPATIAL_CSW_ADMIN_USR",
            "SPATIAL_WFS_ADMIN_USR",
            "SYS",
            "SYSMAN",
            "SYSTEM",
            "WMSYS",
            "XDB",
            "XS$NULL",
            "DIP",
            "OJVMSYS",
            "LBACSYS",
            "AUDSYS",
            "DBSFWUSER",
            "DVF",
            "DVSYS",
            "GGSYS",
            "GSMADMIN_INTERNAL",
            "GSMCATUSER",
            "GSMUSER",
            "REMOTE_SCHEDULER_AGENT",
            "SYS$UMF",
            "SYSBACKUP",
            "SYSDG",
            "SYSKM",
            "SYSRAC",
        )
        schema_list = [row[0] for row in result.rows if row[0] not in sysschema]
        result.rows = schema_list
        return result

    def get_all_tables(self, db_name, **kwargs):
        """Get table list, return a ResultSet."""
        sql = f"""SELECT table_name 
        FROM all_tables 
        WHERE nvl(tablespace_name, 'no tablespace') NOT IN ('SYSTEM', 'SYSAUX') 
        AND OWNER = :db_name AND IOT_NAME IS NULL 
        AND DURATION IS NULL order by table_name"""
        result = self.query(db_name=db_name, sql=sql, parameters={"db_name": db_name})
        tb_list = [row[0] for row in result.rows if row[0] not in ["test"]]
        result.rows = tb_list
        return result

    def get_group_tables_by_db(self, db_name):
        data = {}
        table_list_sql = f"""SELECT table_name, comments FROM  dba_tab_comments  WHERE owner = :db_name"""
        result = self.query(
            db_name=db_name, sql=table_list_sql, parameters={"db_name": db_name}
        )
        for row in result.rows:
            table_name, table_cmt = row[0], row[1]
            if table_name[0] not in data:
                data[table_name[0]] = list()
            data[table_name[0]].append([table_name, table_cmt])
        return data

    def get_table_meta_data(self, db_name, tb_name, **kwargs):
        """Get table metadata for dictionary page.
        Returns dict: {"column_list": [], "rows": []}.
        """
        meta_data_sql = f"""select      tcs.TABLE_NAME, -- table name
                                        tcs.COMMENTS, -- table comment
                                        tcs.TABLE_TYPE,  -- table/view
                                        ss.SEGMENT_TYPE,  -- segment type
                                        ts.TABLESPACE_NAME, -- tablespace
                                        ts.COMPRESSION, -- compression
                                        bss.NUM_ROWS, -- row count
                                        bss.BLOCKS, -- data blocks
                                        bss.EMPTY_BLOCKS, -- empty blocks
                                        bss.AVG_SPACE, -- avg used space in blocks
                                        bss.CHAIN_CNT, -- row chaining/migration count
                                        bss.AVG_ROW_LEN, -- avg row length
                                        bss.LAST_ANALYZED  -- last analyzed time
                                    from dba_tab_comments tcs
                                    left join dba_segments ss
                                        on ss.owner = tcs.OWNER
                                        and ss.segment_name = tcs.TABLE_NAME
                                    left join dba_tables ts
                                        on ts.OWNER = tcs.OWNER
                                        and ts.TABLE_NAME = tcs.TABLE_NAME
                                    left join DBA_TAB_STATISTICS bss
                                        on bss.OWNER = tcs.owner
                                        and bss.TABLE_NAME = tcs.table_name
    
                                    WHERE
                                        tcs.OWNER=:db_name
                                        AND tcs.TABLE_NAME=:tb_name"""
        _meta_data = self.query(
            db_name=db_name,
            sql=meta_data_sql,
            parameters={"db_name": db_name, "tb_name": tb_name},
        )
        return {"column_list": _meta_data.column_list, "rows": _meta_data.rows[0]}

    def get_table_desc_data(self, db_name, tb_name, **kwargs):
        """Get table column metadata."""
        desc_sql = f"""SELECT bcs.COLUMN_NAME "Column Name",
                            ccs.comments "Column Comment" ,
                            bcs.data_type || case
                             when bcs.data_precision is not null and nvl(data_scale, 0) > 0 then
                              '(' || bcs.data_precision || ',' || data_scale || ')'
                             when bcs.data_precision is not null and nvl(data_scale, 0) = 0 then
                              '(' || bcs.data_precision || ')'
                             when bcs.data_precision is null and data_scale is not null then
                              '(*,' || data_scale || ')'
                             when bcs.char_length > 0 then
                              '(' || bcs.char_length || case char_used
                                when 'B' then
                                 ' Byte'
                                when 'C' then
                                 ' Char'
                                else
                                 null
                              end || ')'
                            end "Data Type",
                            bcs.DATA_DEFAULT "Default Value",
                            decode(nullable, 'N', ' NOT NULL') "Nullable",
                            ics.INDEX_NAME "Index Name",
                            acs.constraint_type "Constraint Type"
                        FROM  dba_tab_columns bcs
                        left  join dba_col_comments ccs
                            on  bcs.OWNER = ccs.owner
                            and  bcs.TABLE_NAME = ccs.table_name
                            and  bcs.COLUMN_NAME = ccs.column_name
                        left  join dba_ind_columns ics
                            on  bcs.OWNER = ics.TABLE_OWNER
                            and  bcs.TABLE_NAME = ics.table_name
                            and  bcs.COLUMN_NAME = ics.column_name
                        left join dba_constraints acs
                            on acs.owner = ics.TABLE_OWNER
                            and acs.table_name = ics.TABLE_NAME
                            and acs.index_name = ics.INDEX_NAME
                        WHERE
                            bcs.OWNER=:db_name
                            AND bcs.TABLE_NAME=:tb_name
                        ORDER BY bcs.COLUMN_ID"""
        _desc_data = self.query(
            db_name=db_name,
            sql=desc_sql,
            parameters={"db_name": db_name, "tb_name": tb_name},
        )
        return {"column_list": _desc_data.column_list, "rows": _desc_data.rows}

    def get_table_index_data(self, db_name, tb_name, **kwargs):
        """Get table index metadata."""
        index_sql = f""" SELECT ais.INDEX_NAME "Index Name",
                                ais.uniqueness "Uniqueness",
                                cols.column_names "Index Columns",
                                ais.index_type "Index Type",
                                ais.compression "Compression",
                                ais.tablespace_name "Tablespace",
                                ais.status "Status",
                                ais.partitioned "Partitioned",
                                pis.partitioning_type "Partition Type",
                                pis.locality "Is LOCAL Index",
                                pis.alignment "Leading Column Alignment"
                            FROM dba_indexes ais
                            left join DBA_PART_INDEXES pis
                                on ais.owner = pis.owner
                                and ais.index_name = pis.index_name
                            left JOIN (SELECT 
                                    ics.index_owner,
                                    ics.index_name,
                                    LISTAGG(ics.column_name, ', ') WITHIN GROUP (ORDER BY ics.column_position) AS column_names
                                FROM 
                                    dba_ind_columns ics
                                GROUP BY 
                                    ics.index_owner, ics.index_name
                                    UNION ALL
                                    select lobs.owner, lobs.index_name, lobs.column_name
                                      from dba_lobs lobs
                                    ) cols
                                ON ais.owner = cols.index_owner
                                AND ais.index_name = cols.index_name
                            WHERE
                                ais.owner = :db_name
                                AND ais.table_name = :tb_name"""
        _index_data = self.query(
            db_name, index_sql, parameters={"db_name": db_name, "tb_name": tb_name}
        )
        return {"column_list": _index_data.column_list, "rows": _index_data.rows}

    def get_tables_metas_data(self, db_name, **kwargs):
        """Get all table metadata in DB for dictionary export."""
        table_metas = []
        sql_cols = f""" SELECT bcs.TABLE_NAME TABLE_NAME,
                                   tcs.COMMENTS TABLE_COMMENTS,
                                   bcs.COLUMN_NAME COLUMN_NAME,
                                   bcs.data_type || case
                                     when bcs.data_precision is not null and nvl(data_scale, 0) > 0 then
                                      '(' || bcs.data_precision || ',' || data_scale || ')'
                                     when bcs.data_precision is not null and nvl(data_scale, 0) = 0 then
                                      '(' || bcs.data_precision || ')'
                                     when bcs.data_precision is null and data_scale is not null then
                                      '(*,' || data_scale || ')'
                                     when bcs.char_length > 0 then
                                      '(' || bcs.char_length || case char_used
                                        when 'B' then
                                         ' Byte'
                                        when 'C' then
                                         ' Char'
                                        else
                                         null
                                      end || ')'
                                   end data_type,
                                   bcs.DATA_DEFAULT,
                                   decode(nullable, 'N', ' NOT NULL') nullable,
                                   t1.index_name,
                                   lcs.comments comments
                              FROM dba_tab_columns bcs
                              left join dba_col_comments lcs
                                on bcs.OWNER = lcs.owner
                               and bcs.TABLE_NAME = lcs.table_name
                               and bcs.COLUMN_NAME = lcs.column_name
                              left join dba_tab_comments tcs
                                on bcs.OWNER = tcs.OWNER
                               and bcs.TABLE_NAME = tcs.TABLE_NAME
                              left join (select acs.OWNER,
                                                acs.TABLE_NAME,
                                                scs.column_name,
                                                acs.index_name
                                           from dba_cons_columns scs
                                           join dba_constraints acs
                                             on acs.constraint_name = scs.constraint_name
                                            and acs.owner = scs.OWNER
                                          where acs.constraint_type = 'P') t1
                                on t1.OWNER = bcs.OWNER
                               AND t1.TABLE_NAME = bcs.TABLE_NAME
                               AND t1.column_name = bcs.COLUMN_NAME
                             WHERE bcs.OWNER = :db_name
                             order by bcs.TABLE_NAME, comments"""
        cols_req = self.query(
            sql=sql_cols, close_conn=False, parameters={"db_name": db_name}
        ).rows

        # Define column names for query result.
        cols_df = pd.DataFrame(
            cols_req,
            columns=[
                "TABLE_NAME",
                "TABLE_COMMENTS",
                "COLUMN_NAME",
                "COLUMN_TYPE",
                "COLUMN_DEFAULT",
                "IS_NULLABLE",
                "COLUMN_KEY",
                "COLUMN_COMMENT",
            ],
        )

        # Get de-duplicated table names.
        col_list = cols_df.drop_duplicates("TABLE_NAME").to_dict("records")
        for cl in col_list:
            _meta = dict()
            engine_keys = [
                {"key": "COLUMN_NAME", "value": "Column Name"},
                {"key": "COLUMN_TYPE", "value": "Data Type"},
                {"key": "COLUMN_DEFAULT", "value": "Default Value"},
                {"key": "IS_NULLABLE", "value": "Nullable"},
                {"key": "COLUMN_KEY", "value": "Primary Key"},
                {"key": "COLUMN_COMMENT", "value": "Comment"},
            ]
            _meta["ENGINE_KEYS"] = engine_keys
            _meta["TABLE_INFO"] = {
                "TABLE_NAME": cl["TABLE_NAME"],
                "TABLE_COMMENTS": cl["TABLE_COMMENTS"],
            }
            table_name = cl["TABLE_NAME"]
            # Filter DataFrame rows by table name and convert to list.
            _meta["COLUMNS"] = cols_df.query("TABLE_NAME == @table_name").to_dict(
                "records"
            )

            table_metas.append(_meta)
        return table_metas

    def get_all_objects(self, db_name, **kwargs):
        """Get object_name list, return a ResultSet."""
        sql = f"""SELECT object_name FROM all_objects WHERE OWNER = :db_name """
        result = self.query(db_name=db_name, sql=sql, parameters={"db_name": db_name})
        tb_list = [row[0] for row in result.rows if row[0] not in ["test"]]
        result.rows = tb_list
        return result

    def get_all_columns_by_tb(self, db_name, tb_name, **kwargs):
        """Get all fields, return a ResultSet."""
        result = self.describe_table(db_name, tb_name)
        column_list = [row[0] for row in result.rows]
        result.rows = column_list
        return result

    def describe_table(self, db_name, tb_name, **kwargs):
        """return ResultSet"""
        # https://www.thepolyglotdeveloper.com/2015/01/find-tables-oracle-database-column-name/
        sql = f"""SELECT
        a.column_name,
        data_type,
        data_length,
        nullable,
        data_default,
        b.comments
        FROM all_tab_cols a, all_col_comments b
        WHERE a.table_name = b.table_name
        and a.owner = b.OWNER
        and a.COLUMN_NAME = b.COLUMN_NAME
        and a.table_name = :tb_name and a.owner = :db_name order by column_id
        """
        result = self.query(
            db_name=db_name,
            sql=sql,
            parameters={"db_name": db_name, "tb_name": tb_name},
        )
        return result

    def object_name_check(self, db_name=None, object_name=""):
        """Check object existence by name."""
        if "." in object_name:
            schema_name = object_name.split(".")[0]
            object_name = object_name.split(".")[1]
            if '"' in schema_name:
                schema_name = schema_name.replace('"', "")
                if '"' in object_name:
                    object_name = object_name.replace('"', "")
                else:
                    object_name = object_name.upper()
            else:
                schema_name = schema_name.upper()
                if '"' in object_name:
                    object_name = object_name.replace('"', "")
                else:
                    object_name = object_name.upper()
        else:
            schema_name = db_name
            if '"' in object_name:
                object_name = object_name.replace('"', "")
            else:
                object_name = object_name.upper()
        sql = f""" SELECT object_name FROM all_objects WHERE OWNER = :schema_name and OBJECT_NAME = :object_name """
        result = self.query(
            db_name=db_name,
            sql=sql,
            close_conn=False,
            parameters={"schema_name": schema_name, "object_name": object_name},
        )
        if result.affected_rows > 0:
            return True
        else:
            return False

    @staticmethod
    def get_sql_first_object_name(sql=""):
        """Get first object_name in SQL text."""
        object_name = ""
        # Match table/index/sequence.
        pattern = r"^(create|alter)\s+(table|index|unique\sindex|sequence)\s"
        groups = re.match(pattern, sql, re.M | re.IGNORECASE)

        if groups:
            object_name = (
                re.match(
                    r"^(create|alter)\s+(table|index|unique\sindex|sequence)\s+(.+?)(\s|\()",
                    sql,
                    re.M | re.IGNORECASE,
                )
                .group(3)
                .strip()
            )
            return object_name

        # Match create or replace SQL block.
        pattern = r"^create\s+(or\s+replace\s+)?(function|view|procedure|trigger|package\sbody|package|type\sbody|type)\s"
        groups = re.match(pattern, sql, re.M | re.IGNORECASE)

        if groups:
            object_name = (
                re.match(
                    r"^create\s+(or\s+replace\s+)?(function|view|procedure|trigger|package\sbody|package|type\sbody|type)\s+(.+?)(\s|\()",
                    sql,
                    re.M | re.IGNORECASE,
                )
                .group(3)
                .strip()
            )
            return object_name
        return object_name

    @staticmethod
    def check_create_index_table(sql="", object_name_list=None, db_name=""):
        schema_name = '"' + db_name + '"'
        object_name_list = object_name_list or set()
        if re.match(r"^create\s+index\s", sql):
            table_name = re.match(
                r"^create\s+index\s+.+\s+on\s(.+?)(\(|\s\()", sql, re.M
            ).group(1)
            if "." not in table_name:
                table_name = f"{schema_name}.{table_name}"
            table_name = table_name.upper()
            if table_name in object_name_list:
                return True
            else:
                return False
        elif re.match(r"^create\s+unique\s+index\s", sql):
            table_name = re.match(
                r"^create\s+unique\s+index\s+.+\s+on\s(.+?)(\(|\s\()", sql, re.M
            ).group(1)
            if "." not in table_name:
                table_name = f"{schema_name}.{table_name}"
            table_name = table_name.upper()
            if table_name in object_name_list:
                return True
            else:
                return False
        else:
            return False

    @staticmethod
    def get_dml_table(sql="", object_name_list=None, db_name=""):
        schema_name = '"' + db_name + '"'
        object_name_list = object_name_list or set()
        if re.match(r"^update", sql):
            table_name = re.match(r"^update\s(.+?)\s", sql, re.M).group(1)
            if "." not in table_name:
                table_name = f"{schema_name}.{table_name}"
            table_name = table_name.upper()
            if table_name in object_name_list:
                return True
            else:
                return False
        elif re.match(r"^delete", sql):
            table_name = re.match(r"^delete\s(.+?)\s", sql, re.M).group(1)
            if "." not in table_name:
                table_name = f"{schema_name}.{table_name}"
            table_name = table_name.upper()
            if table_name in object_name_list:
                return True
            else:
                return False
        elif re.match(r"^insert\s", sql):
            table_name = re.match(
                r"^insert\s+((into)|(all\s+into)|(all\s+when\s(.+?)into))\s+(.+?)(\(|\s)",
                sql,
                re.M,
            ).group(6)
            if "." not in table_name:
                table_name = f"{schema_name}.{table_name}"
            table_name = table_name.upper()
            if table_name in object_name_list:
                return True
            else:
                return False
        else:
            return False

    @staticmethod
    def where_check(sql=""):
        if re.match(r"^update((?!where).)*$|^delete((?!where).)*$", sql):
            return True
        else:
            parsed = sqlparse.parse(sql)[0]
            flattened = list(parsed.flatten())
            n_skip = 0
            flattened = flattened[: len(flattened) - n_skip]
            logical_operators = (
                "AND",
                "OR",
                "NOT",
                "BETWEEN",
                "ORDER BY",
                "GROUP BY",
                "HAVING",
            )
            for t in reversed(flattened):
                if t.is_keyword:
                    return True
            return False

    def explain_check(self, db_name=None, sql="", close_conn=False):
        # Use explain for SQL syntax check.
        # Keep connection alive to avoid excessive DB fork overhead.
        result = {"msg": "", "rows": 0}
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            if db_name:
                conn.current_schema = db_name
            if re.match(r"^explain", sql, re.I):
                sql = sql
            else:
                sql = f"explain plan for {sql}"
            sql = sql.rstrip(";")
            cursor.execute(sql)
            # Get affected row estimate.
            cursor.execute(
                "select CARDINALITY from (select CARDINALITY from PLAN_TABLE t where id = 0 order by t.timestamp desc) where rownum = 1"
            )
            rows = cursor.fetchone()
            conn.rollback()
            if not rows:
                result["rows"] = 0
            else:
                result["rows"] = rows[0]
        except Exception as e:
            logger.warning(
                f"Oracle statement execution failed, SQL: {sql}, "
                f"error: {traceback.format_exc()}"
            )
            result["msg"] = str(e)
        finally:
            if close_conn:
                self.close()
            return result

    def query_check(self, db_name=None, sql=""):
        # Query checks: strip comments and split statements.
        result = {"msg": "", "bad_query": False, "filtered_sql": sql, "has_star": False}
        keyword_warning = ""
        star_patter = r"(^|,|\s)\*(\s|\(|$)"
        # Remove comments, validate syntax, execute first valid SQL.
        try:
            sql = sqlparse.format(sql, strip_comments=True)
            sql = sqlparse.split(sql)[0]
            result["filtered_sql"] = re.sub(r";$", "", sql.strip())
            sql_lower = sql.lower()
        except IndexError:
            result["bad_query"] = True
            result["msg"] = "No valid SQL statement"
            return result
        if re.match(r"^select|^with|^explain", sql_lower) is None:
            result["bad_query"] = True
            result["msg"] = "Unsupported syntax!"
            return result
        if re.search(star_patter, sql_lower) is not None:
            keyword_warning += "Using * keyword is forbidden\n"
            result["has_star"] = True
        if result.get("bad_query") or result.get("has_star"):
            result["msg"] = keyword_warning
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
        """Return ResultSet."""
        result_set = ResultSet(full_sql=sql)
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            if db_name:
                conn.current_schema = db_name
            sql = sql.rstrip(";")
            # Support Oracle explain plan query.
            if re.match(r"^explain", sql, re.I):
                cursor.execute(sql)
                # Reset SQL to fetch explain plan output.
                sql = f"select PLAN_TABLE_OUTPUT from table(dbms_xplan.display)"
            cursor.execute(sql, parameters or [])
            fields = cursor.description
            if any(x[1] == cx_Oracle.CLOB for x in fields):
                rows = [
                    tuple([(c.read() if type(c) == cx_Oracle.LOB else c) for c in r])
                    for r in cursor
                ]
                if int(limit_num) > 0:
                    rows = rows[0 : int(limit_num)]
            else:
                if int(limit_num) > 0:
                    rows = cursor.fetchmany(int(limit_num))
                else:
                    rows = cursor.fetchall()
            result_set.column_list = [i[0] for i in fields] if fields else []
            result_set.rows = [tuple(x) for x in rows]
            result_set.affected_rows = len(result_set.rows)
        except Exception as e:
            logger.warning(
                f"Oracle statement execution failed, SQL: {sql}, "
                f"error: {traceback.format_exc()}"
            )
            result_set.error = str(e)
        finally:
            if close_conn:
                self.close()
        return result_set

    def query_masking(self, db_name=None, sql="", resultset=None):
        """Simple column masking, only effective for SELECT."""
        if re.match(r"^select", sql, re.I):
            filtered_result = simple_column_mask(self.instance, resultset)
            filtered_result.is_masked = True
        else:
            filtered_result = resultset
        return filtered_result

    def execute_check(self, db_name=None, sql="", close_conn=True):
        """
        Pre-check before workflow execution, return ReviewSet.
        update by Jan.song 20200302
        Use explain to estimate affected rows for data modifications.
        """
        config = SysConfig()
        check_result = ReviewSet(full_sql=sql)
        # Syntax supported by explain.
        explain_re = r"^merge|^update|^delete|^insert|^create\s+table|^create\s+index|^create\s+unique\s+index"
        # Unsupported/high-risk statement checks.
        line = 1
        # Track newly created objects in SQL.
        object_name_list = set()
        critical_ddl_regex = config.get("critical_ddl_regex", "")
        p = re.compile(critical_ddl_regex)
        check_result.syntax_type = 2  # TODO workflow type: 0 other, 1 DDL, 2 DML
        sqlitem = None
        try:
            sqlitemList = get_full_sqlitem_list(sql, db_name)
            for sqlitem in sqlitemList:
                sql_lower = sqlitem.statement.lower().rstrip(";")
                sql_nolower = sqlitem.statement.rstrip(";")
                object_name = self.get_sql_first_object_name(sql=sql_lower)
                if "." in object_name:
                    object_name = object_name
                else:
                    object_name = f"""{db_name}.{object_name}"""
                object_name_list.add(object_name)
                # Unsupported statements.
                if re.match(r"^select|^with|^explain", sql_lower):
                    result = ReviewResult(
                        id=line,
                        errlevel=2,
                        stagestatus="Rejected unsupported statement",
                        errormessage=(
                            "Only DML and DDL statements are supported. "
                            "Use SQL query feature for SELECT statements!"
                        ),
                        sql=sqlitem.statement,
                    )
                # High-risk statements.
                elif critical_ddl_regex and p.match(sql_lower.strip()):
                    result = ReviewResult(
                        id=line,
                        errlevel=2,
                        stagestatus="Rejected high-risk SQL",
                        errormessage="Submitting statements matching "
                        + critical_ddl_regex
                        + " is prohibited!",
                        sql=sqlitem.statement,
                    )
                # Reject update/delete without WHERE.
                elif re.match(
                    r"^update((?!where).)*$|^delete((?!where).)*$", sql_lower
                ):
                    result = ReviewResult(
                        id=line,
                        errlevel=2,
                        stagestatus="Rejected update/delete without WHERE",
                        errormessage="Data modifications must include WHERE clause!",
                        sql=sqlitem.statement,
                    )
                # Reject transaction/session control SQL.
                elif re.match(r"^set|^rollback|^exit", sql_lower):
                    result = ReviewResult(
                        id=line,
                        errlevel=2,
                        stagestatus="SQL cannot contain ^set|^rollback|^exit",
                        errormessage="SQL cannot contain ^set|^rollback|^exit",
                        sql=sqlitem.statement,
                    )

                # Use explain for syntax/semantic checks.
                elif re.match(explain_re, sql_lower) and sqlitem.stmt_type == "SQL":
                    if self.check_create_index_table(
                        db_name=db_name,
                        sql=sql_lower,
                        object_name_list=object_name_list,
                    ):
                        result = ReviewResult(
                            id=line,
                            errlevel=1,
                            stagestatus="WARNING: index on newly created table cannot be checked yet!",
                            errormessage="WARNING: index on newly created table cannot be checked yet!",
                            stmt_type=sqlitem.stmt_type,
                            object_owner=sqlitem.object_owner,
                            object_type=sqlitem.object_type,
                            object_name=sqlitem.object_name,
                            sql=sqlitem.statement,
                        )
                    elif len(object_name_list) > 0 and self.get_dml_table(
                        db_name=db_name,
                        sql=sql_lower,
                        object_name_list=object_name_list,
                    ):
                        result = ReviewResult(
                            id=line,
                            errlevel=1,
                            stagestatus="WARNING: DML on newly created table cannot be checked yet!",
                            errormessage="WARNING: DML on newly created table cannot be checked yet!",
                            stmt_type=sqlitem.stmt_type,
                            object_owner=sqlitem.object_owner,
                            object_type=sqlitem.object_type,
                            object_name=sqlitem.object_name,
                            sql=sqlitem.statement,
                        )
                    else:
                        result_set = self.explain_check(
                            db_name=db_name, sql=sqlitem.statement, close_conn=False
                        )
                        if result_set["msg"]:
                            result = ReviewResult(
                                id=line,
                                errlevel=2,
                                stagestatus="Explain syntax check failed!",
                                errormessage=result_set["msg"],
                                sql=sqlitem.statement,
                            )
                        else:
                            # Check object existence for create table/index statements.
                            if re.match(
                                r"^create\s+table|^create\s+index|^create\s+unique\s+index",
                                sql_lower,
                            ):
                                object_name = self.get_sql_first_object_name(
                                    sql=sql_nolower
                                )
                                # Save created object for existence checks in subsequent SQL.
                                if "." in object_name:
                                    schema_name = object_name.split(".")[0]
                                    object_name = object_name.split(".")[1]
                                    if '"' in schema_name:
                                        schema_name = schema_name
                                        if '"' not in object_name:
                                            object_name = object_name.upper()
                                    else:
                                        schema_name = schema_name.upper()
                                        if '"' not in object_name:
                                            object_name = object_name.upper()
                                else:
                                    schema_name = '"' + db_name + '"'
                                    if '"' not in object_name:
                                        object_name = object_name.upper()

                                object_name = f"""{schema_name}.{object_name}"""
                                if (
                                    self.object_name_check(
                                        db_name=db_name, object_name=object_name
                                    )
                                    or object_name in object_name_list
                                ):
                                    result = ReviewResult(
                                        id=line,
                                        errlevel=2,
                                        stagestatus=f"""{object_name} already exists!""",
                                        errormessage=f"""{object_name} already exists!""",
                                        sql=sqlitem.statement,
                                    )
                                else:
                                    object_name_list.add(object_name)
                                    if (
                                        result_set.get("rows", None)
                                        and result_set["rows"] > 1000
                                    ):
                                        result = ReviewResult(
                                            id=line,
                                            errlevel=1,
                                            stagestatus="Affected rows exceed 1000, please review",
                                            errormessage="Affected rows exceed 1000, please review",
                                            sql=sqlitem.statement,
                                            stmt_type=sqlitem.stmt_type,
                                            object_owner=sqlitem.object_owner,
                                            object_type=sqlitem.object_type,
                                            object_name=sqlitem.object_name,
                                            affected_rows=result_set["rows"],
                                            execute_time=0,
                                        )
                                    else:
                                        result = ReviewResult(
                                            id=line,
                                            errlevel=0,
                                            stagestatus="Audit completed",
                                            errormessage="None",
                                            sql=sqlitem.statement,
                                            stmt_type=sqlitem.stmt_type,
                                            object_owner=sqlitem.object_owner,
                                            object_type=sqlitem.object_type,
                                            object_name=sqlitem.object_name,
                                            affected_rows=result_set["rows"],
                                            execute_time=0,
                                        )
                            else:
                                if (
                                    result_set.get("rows", None)
                                    and result_set["rows"] > 1000
                                ):
                                    result = ReviewResult(
                                        id=line,
                                        errlevel=1,
                                        stagestatus="Affected rows exceed 1000, please review",
                                        errormessage="Affected rows exceed 1000, please review",
                                        sql=sqlitem.statement,
                                        stmt_type=sqlitem.stmt_type,
                                        object_owner=sqlitem.object_owner,
                                        object_type=sqlitem.object_type,
                                        object_name=sqlitem.object_name,
                                        affected_rows=result_set["rows"],
                                        execute_time=0,
                                    )
                                else:
                                    result = ReviewResult(
                                        id=line,
                                        errlevel=0,
                                        stagestatus="Audit completed",
                                        errormessage="None",
                                        sql=sqlitem.statement,
                                        stmt_type=sqlitem.stmt_type,
                                        object_owner=sqlitem.object_owner,
                                        object_type=sqlitem.object_type,
                                        object_name=sqlitem.object_name,
                                        affected_rows=result_set["rows"],
                                        execute_time=0,
                                    )
                # Other statements that cannot be checked by explain.
                else:
                    # Check object existence for alter table.
                    if re.match(r"^alter\s+table\s", sql_lower):
                        object_name = self.get_sql_first_object_name(sql=sql_nolower)
                        if "." in object_name:
                            schema_name = object_name.split(".")[0]
                            object_name = object_name.split(".")[1]
                            if '"' in schema_name:
                                schema_name = schema_name
                                if '"' not in object_name:
                                    object_name = object_name.upper()
                            else:
                                schema_name = schema_name.upper()
                                if '"' not in object_name:
                                    object_name = object_name.upper()
                        else:
                            schema_name = '"' + db_name + '"'
                            if '"' not in object_name:
                                object_name = object_name.upper()

                        object_name = f"""{schema_name}.{object_name}"""
                        if (
                            not self.object_name_check(
                                db_name=db_name, object_name=object_name
                            )
                            and object_name not in object_name_list
                        ):
                            result = ReviewResult(
                                id=line,
                                errlevel=2,
                                stagestatus=f"""{object_name} does not exist!""",
                                errormessage=f"""{object_name} does not exist!""",
                                sql=sqlitem.statement,
                            )
                        else:
                            result = ReviewResult(
                                id=line,
                                errlevel=1,
                                stagestatus="Current platform does not support auditing this syntax!",
                                errormessage="Current platform does not support auditing this syntax!",
                                sql=sqlitem.statement,
                                stmt_type=sqlitem.stmt_type,
                                object_owner=sqlitem.object_owner,
                                object_type=sqlitem.object_type,
                                object_name=sqlitem.object_name,
                                affected_rows=0,
                                execute_time=0,
                            )
                    # Check object existence for create statements.
                    elif re.match(r"^create", sql_lower):
                        object_name = self.get_sql_first_object_name(sql=sql_nolower)
                        if "." in object_name:
                            schema_name = object_name.split(".")[0]
                            object_name = object_name.split(".")[1]
                            if '"' in schema_name:
                                schema_name = schema_name
                                if '"' not in object_name:
                                    object_name = object_name.upper()
                            else:
                                schema_name = schema_name.upper()
                                if '"' not in object_name:
                                    object_name = object_name.upper()
                        else:
                            schema_name = '"' + db_name + '"'
                            if '"' not in object_name:
                                object_name = object_name.upper()

                        object_name = f"""{schema_name}.{object_name}"""
                        if re.match(r"^create\sor\sreplace", sql_lower) and (
                            self.object_name_check(
                                db_name=db_name, object_name=object_name
                            )
                            or object_name in object_name_list
                        ):
                            result = ReviewResult(
                                id=line,
                                errlevel=1,
                                stagestatus=f"""{object_name} already exists, please confirm replacement!""",
                                errormessage=f"""{object_name} already exists, please confirm replacement!""",
                                sql=sqlitem.statement,
                                stmt_type=sqlitem.stmt_type,
                                object_owner=sqlitem.object_owner,
                                object_type=sqlitem.object_type,
                                object_name=sqlitem.object_name,
                                affected_rows=0,
                                execute_time=0,
                            )
                        elif (
                            self.object_name_check(
                                db_name=db_name, object_name=object_name
                            )
                            or object_name in object_name_list
                        ):
                            result = ReviewResult(
                                id=line,
                                errlevel=2,
                                stagestatus=f"""{object_name} already exists!""",
                                errormessage=f"""{object_name} already exists!""",
                                sql=sqlitem.statement,
                            )
                        else:
                            object_name_list.add(object_name)
                            result = ReviewResult(
                                id=line,
                                errlevel=1,
                                stagestatus="Current platform does not support auditing this syntax!",
                                errormessage="Current platform does not support auditing this syntax!",
                                sql=sqlitem.statement,
                                stmt_type=sqlitem.stmt_type,
                                object_owner=sqlitem.object_owner,
                                object_type=sqlitem.object_type,
                                object_name=sqlitem.object_name,
                                affected_rows=0,
                                execute_time=0,
                            )
                    else:
                        result = ReviewResult(
                            id=line,
                            errlevel=1,
                            stagestatus="Current platform does not support auditing this syntax!",
                            errormessage="Current platform does not support auditing this syntax!",
                            sql=sqlitem.statement,
                            stmt_type=sqlitem.stmt_type,
                            object_owner=sqlitem.object_owner,
                            object_type=sqlitem.object_type,
                            object_name=sqlitem.object_name,
                            affected_rows=0,
                            execute_time=0,
                        )
                # Determine workflow type.
                if get_syntax_type(sql=sqlitem.statement, db_type="oracle") == "DDL":
                    check_result.syntax_type = 1
                check_result.rows += [result]
                line += 1
        except Exception as e:
            logger.warning(
                "Oracle statement execution failed, "
                f"SQL #{line}: {sqlitem.statement}, "
                f"error: {traceback.format_exc()}"
            )
            check_result.error = str(e)
        finally:
            if close_conn:
                self.close()
        # Count warnings and errors.
        for r in check_result.rows:
            if r.errlevel == 1:
                check_result.warning_count += 1
            if r.errlevel == 2:
                check_result.error_count += 1
        return check_result

    def execute_workflow(self, workflow, close_conn=True):
        """Execute workflow, return ReviewSet.
        Legacy logic split SQL from sql_content and executed sequentially.
        New logic executes SQL recorded in review results.
        For PLSQL object definitions, also verify compilation success.
        """
        review_content = workflow.sqlworkflowcontent.review_content
        review_result = json.loads(review_content)
        sqlitemList = get_exec_sqlitem_list(review_result, workflow.db_name)

        sql = workflow.sqlworkflowcontent.sql_content
        execute_result = ReviewSet(full_sql=sql)

        line = 1
        statement = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            conn.current_schema = workflow.db_name
            # Get workflow execution start time for backup log mining.
            cursor.execute(f"alter session set nls_date_format='yyyy-mm-dd hh24:mi:ss'")
            cursor.execute(f"select sysdate from dual")
            rows = cursor.fetchone()
            begin_time = rows[0]
            # Execute split statements one by one and append results.
            for sqlitem in sqlitemList:
                statement = sqlitem.statement
                if sqlitem.stmt_type == "SQL":
                    statement = statement.rstrip(";")
                # For DDL workflows, get original object definition and store it.
                # Requires: grant execute on dbms_metadata to execution user.
                if workflow.syntax_type == 1:
                    object_name = self.get_sql_first_object_name(statement)
                    back_obj_sql = f"""select dbms_metadata.get_ddl(object_type,object_name,owner)
                    from all_objects where (object_name=upper( '{object_name}' ) or OBJECT_NAME = '{sqlitem.object_name}')
                    and owner='{workflow.db_name}'
                                        """
                    cursor.execute(back_obj_sql)
                    metdata_back_flag = self.metdata_backup(workflow, cursor, statement)

                with FuncTimer() as t:
                    if statement != "":
                        cursor.execute(statement)
                        conn.commit()

                rowcount = cursor.rowcount
                stagestatus = "Execute Successfully"
                if (
                    sqlitem.stmt_type == "PLSQL"
                    and sqlitem.object_name
                    and sqlitem.object_name != "ANONYMOUS"
                    and sqlitem.object_name != ""
                ):
                    query_obj_sql = f"""SELECT OBJECT_NAME, STATUS, TO_CHAR(LAST_DDL_TIME, 'YYYY-MM-DD HH24:MI:SS') FROM ALL_OBJECTS
                                         WHERE OWNER = '{sqlitem.object_owner}'
                                         AND OBJECT_NAME = '{sqlitem.object_name}'
                                        """
                    cursor.execute(query_obj_sql)
                    row = cursor.fetchone()
                    if row:
                        status = row[1]
                        if status and status == "INVALID":
                            stagestatus = (
                                "Compile Failed. Object "
                                + sqlitem.object_owner
                                + "."
                                + sqlitem.object_name
                                + " is invalid."
                            )
                    else:
                        stagestatus = (
                            "Compile Failed. Object "
                            + sqlitem.object_owner
                            + "."
                            + sqlitem.object_name
                            + " doesn't exist."
                        )

                    if stagestatus != "Execute Successfully":
                        raise Exception(stagestatus)

                execute_result.rows.append(
                    ReviewResult(
                        id=line,
                        errlevel=0,
                        stagestatus=stagestatus,
                        errormessage="None",
                        sql=statement,
                        affected_rows=cursor.rowcount,
                        execute_time=t.cost,
                    )
                )
                line += 1
        except Exception as e:
            logger.warning(
                f"Oracle command execution failed, workflow id: {workflow.id}, "
                f"SQL: {statement or sql}, error: {traceback.format_exc()}"
            )
            execute_result.error = str(e)
            # conn.rollback()
            # Append failed statement info to execution result.
            execute_result.rows.append(
                ReviewResult(
                    id=line,
                    errlevel=2,
                    stagestatus="Execute Failed",
                    errormessage=f"Exception info: {e}",
                    sql=statement or sql,
                    affected_rows=0,
                    execute_time=0,
                )
            )
            line += 1
            # Mark remaining statements as audit completed but not executed.
            for sqlitem in sqlitemList[line - 1 :]:
                execute_result.rows.append(
                    ReviewResult(
                        id=line,
                        errlevel=0,
                        stagestatus="Audit completed",
                        errormessage="Previous statement failed, not executed",
                        sql=sqlitem.statement,
                        affected_rows=0,
                        execute_time=0,
                    )
                )
                line += 1
        finally:
            # Backup.
            if workflow.is_backup:
                try:
                    cursor.execute(f"select sysdate from dual")
                    rows = cursor.fetchone()
                    end_time = rows[0]
                    self.backup(
                        workflow,
                        cursor=cursor,
                        begin_time=begin_time,
                        end_time=end_time,
                    )
                except Exception as e:
                    logger.error(
                        f"Oracle workflow backup failed, workflow id: {workflow.id}, "
                        f"error: {traceback.format_exc()}"
                    )
            if close_conn:
                self.close()
        return execute_result

    def backup(self, workflow, cursor, begin_time, end_time):
        """
        :param workflow: Workflow object, linked in backup records.
        :param cursor: Current session cursor executing SQL.
        :param begin_time: SQL execution start time.
        :param end_time: SQL execution end time.
        :return:
        """
        # add Jan.song 2020402
        # Generate rollback SQL.
        # Requires `grant select any transaction` and
        # `grant execute on dbms_logmnr` to execution user.
        # DB must enable supplemental logging and archive mode.
        try:
            # Backup storage is shared with MySQL backup DB.
            # Create backup DB/table to store rollback SQL linked by workflow.id.
            workflow_id = workflow.id
            conn = self.get_backup_connection()
            backup_cursor = conn.cursor()
            backup_cursor.execute(f"""create database if not exists ora_backup;""")
            backup_cursor.execute(f"use ora_backup;")
            backup_cursor.execute(f"""CREATE TABLE if not exists `sql_rollback` (
                                       `id` bigint(20) NOT NULL AUTO_INCREMENT,
                                       `redo_sql` mediumtext,
                                       `undo_sql` mediumtext,
                                       `workflow_id` bigint(20) NOT NULL,
                                        PRIMARY KEY (`id`),
                                        key `idx_sql_rollback_01` (`workflow_id`)
                                     ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;""")
            # Use logminer to capture rollback SQL.
            logmnr_start_sql = f"""begin
                                        dbms_logmnr.start_logmnr(
                                        starttime=>to_date('{begin_time}','yyyy-mm-dd hh24:mi:ss'),
                                        endtime=>to_date('{end_time}','yyyy/mm/dd hh24:mi:ss'),
                                        options=>dbms_logmnr.dict_from_online_catalog + dbms_logmnr.continuous_mine);
                                    end;"""
            undo_sql = f"""select 
                           xmlagg(xmlparse(content sql_redo wellformed)  order by  scn,rs_id,ssn,rownum).getclobval() ,
                           xmlagg(xmlparse(content sql_undo wellformed)  order by  scn,rs_id,ssn,rownum).getclobval() 
                           from v$logmnr_contents
                           where  SEG_OWNER not in ('SYS')
                           and session# = (select sid from v$mystat where rownum = 1)
                           and serial# = (select serial# from v$session s where s.sid = (select sid from v$mystat where rownum = 1 ))  
                           group by  scn,rs_id,ssn  order by scn desc"""
            logmnr_end_sql = f"""begin
                                    dbms_logmnr.end_logmnr;
                                 end;"""
            cursor.execute(logmnr_start_sql)
            cursor.execute(undo_sql)
            rows = cursor.fetchall()
            cursor.execute(logmnr_end_sql)
            if len(rows) > 0:
                for row in rows:
                    redo_sql = f"{row[0]}"
                    redo_sql = redo_sql.replace("'", "\\'")
                    if row[1] is None:
                        undo_sql = f" "
                    else:
                        undo_sql = f"{row[1]}"
                    undo_sql = undo_sql.replace("'", "\\'")
                    # Persist rollback SQL.
                    sql = f"""insert into sql_rollback(redo_sql,undo_sql,workflow_id) values('{redo_sql}','{undo_sql}',{workflow_id});"""
                    backup_cursor.execute(sql)
        except Exception as e:
            logger.warning(f"Backup failed, error: {traceback.format_exc()}")
            return False
        finally:
            # Close connection.
            if conn:
                conn.close()
        return True

    def metdata_backup(self, workflow, cursor, redo_sql):
        """
        :param workflow: Workflow object, linked in backup records.
        :param cursor: Current session cursor, used to fetch metadata.
        :param redo_sql: Executed SQL.
        :return:
        """
        try:
            # Backup storage is shared with MySQL backup DB.
            # Create backup DB/table to store rollback SQL linked by workflow.id.
            workflow_id = workflow.id
            conn = self.get_backup_connection()
            backup_cursor = conn.cursor()
            backup_cursor.execute(f"""create database if not exists ora_backup;""")
            backup_cursor.execute(f"use ora_backup;")
            backup_cursor.execute(f"""CREATE TABLE if not exists `sql_rollback` (
                                       `id` bigint(20) NOT NULL AUTO_INCREMENT,
                                       `redo_sql` mediumtext,
                                       `undo_sql` mediumtext,
                                       `workflow_id` bigint(20) NOT NULL,
                                        PRIMARY KEY (`id`),
                                        key `idx_sql_rollback_01` (`workflow_id`)
                                     ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;""")
            rows = cursor.fetchall()
            if len(rows) > 0:
                for row in rows:
                    if row[0] is None:
                        undo_sql = f" "
                    else:
                        undo_sql = f"{row[0]}"
                    undo_sql = undo_sql.replace("'", "\\'")
                    # Persist rollback SQL.
                    sql = f"""insert into sql_rollback(redo_sql,undo_sql,workflow_id) values('{redo_sql}','{undo_sql}',{workflow_id});"""
                    backup_cursor.execute(sql)
        except Exception as e:
            logger.warning(f"Backup failed, error: {traceback.format_exc()}")
            return False
        finally:
            # Close connection.
            if conn:
                conn.close()
        return True

    def get_rollback(self, workflow):
        """
         add by Jan.song 20200402
        Get rollback SQL and return in reverse execution order:
        ['source SQL', 'rollback SQL'].
        """
        list_execute_result = json.loads(workflow.sqlworkflowcontent.execute_result)
        # Show rollback SQL in reverse order.
        list_execute_result.reverse()
        list_backup_sql = []
        try:
            # Create connection.
            conn = self.get_backup_connection()
            cur = conn.cursor()
            sql = f"""select redo_sql,undo_sql from sql_rollback where workflow_id = {workflow.id} order by id;"""
            cur.execute(f"use ora_backup;")
            cur.execute(sql)
            list_tables = cur.fetchall()
            for row in list_tables:
                redo_sql = row[0]
                undo_sql = row[1]
                # Build rollback SQL list: ['source SQL', 'rollback SQL'].
                list_backup_sql.append([redo_sql, undo_sql])
        except Exception as e:
            logger.error(f"Get rollback SQL failed, error: {traceback.format_exc()}")
            raise Exception(e)
        # Close connection.
        if conn:
            conn.close()
        return list_backup_sql

    def sqltuningadvisor(self, db_name=None, sql="", close_conn=True, **kwargs):
        """
        add by Jan.song 20200421
        SQL tuning support using DBMS_SQLTUNE package.
        Execution user must have advisor role.
        Return ResultSet.
        """
        result_set = ResultSet(full_sql=sql)
        task_name = "sqlaudit" + f"""{threading.currentThread().ident}"""
        task_begin = 0
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            sql = sql.rstrip(";")
            # Create tuning task.
            create_task_sql = f"""DECLARE
                                  my_task_name VARCHAR2(30);
                                  my_sqltext  CLOB;
                                  BEGIN
                                  my_sqltext := :sql;
                                  my_task_name := DBMS_SQLTUNE.CREATE_TUNING_TASK(
                                  sql_text    => my_sqltext,
                                  user_name   => :db_name,
                                  scope       => 'COMPREHENSIVE',
                                  time_limit  => 30,
                                  task_name   => :task_name,
                                  description => 'tuning');
                                  DBMS_SQLTUNE.EXECUTE_TUNING_TASK( task_name => :task_name);
                                  END;"""
            task_begin = 1
            cursor.execute(
                create_task_sql,
                {"sql": sql, "db_name": db_name, "task_name": task_name},
            )
            # Get tuning report.
            get_task_sql = (
                f"""select DBMS_SQLTUNE.REPORT_TUNING_TASK(:task_name) from dual"""
            )
            cursor.execute(get_task_sql, {"task_name": task_name})
            fields = cursor.description
            if any(x[1] == cx_Oracle.CLOB for x in fields):
                rows = [
                    tuple([(c.read() if type(c) == cx_Oracle.LOB else c) for c in r])
                    for r in cursor
                ]
            else:
                rows = cursor.fetchall()
            result_set.column_list = [i[0] for i in fields] if fields else []
            result_set.rows = [tuple(x) for x in rows]
            result_set.affected_rows = len(result_set.rows)
        except Exception as e:
            logger.warning(
                f"Oracle statement execution failed, SQL: {sql}, "
                f"error: {traceback.format_exc()}"
            )
            result_set.error = str(e)
        finally:
            # Drop tuning task.
            if task_begin == 1:
                end_sql = f"""DECLARE
                             begin
                             dbms_sqltune.drop_tuning_task('{task_name}');
                             end;"""
                cursor.execute(end_sql)
            if close_conn:
                self.close()
        return result_set

    def execute(self, db_name=None, sql="", close_conn=True, parameters=None):
        """Execute statement natively."""
        result = ResultSet(full_sql=sql)
        conn = self.get_connection(db_name=db_name)
        try:
            cursor = conn.cursor()
            for statement in sqlparse.split(sql):
                statement = statement.rstrip(";")
                cursor.execute(statement, parameters or [])
        except Exception as e:
            logger.warning(
                f"Oracle statement execution failed, SQL: {sql}, "
                f"error: {traceback.format_exc()}"
            )
            result.error = str(e)
        if close_conn:
            self.close()
        return result

    def processlist(self, command_type, **kwargs):
        """Get session information."""
        base_sql = """select 
                       s.sid,
                       s.serial#,
                       s.status,
                       s.username,
                       q.sql_text,
                       q.sql_fulltext,
                       s.machine,
                       s.sql_exec_start
                    from v$process p, v$session s, v$sqlarea q 
                    where p.addr = s.paddr  
                       and s.sql_hash_value = q.hash_value"""
        if not command_type:
            command_type = "Active"
        if command_type == "All":
            sql = base_sql + ";"
        elif command_type == "Active":
            sql = "{} and s.status = 'ACTIVE';".format(base_sql)
        elif command_type == "Others":
            sql = "{} and s.status != 'ACTIVE';".format(base_sql)
        else:
            sql = ""

        return self.query(sql=sql)

    def get_kill_command(self, thread_ids):
        """Generate kill command from sid+serial# list."""
        # Validate parameters, format: [[sid, serial#], [sid, serial#]].
        if [
            k
            for k in [[j for j in i if not isinstance(j, int)] for i in thread_ids]
            if k
        ]:
            return None
        sql = """select 'alter system kill session ' || '''' || s.sid || ',' || s.serial# || '''' || ' immediate' || ';'
                 from v$process p, v$session s, v$sqlarea q
                 where p.addr = s.paddr
                 and s.sql_hash_value = q.hash_value
                 and s.sid || ',' || s.serial# in ({});""".format(
            ",".join(f"'{str(tid[0])},{str(tid[1])}'" for tid in thread_ids)
        )
        all_kill_sql = self.query(sql=sql)
        kill_sql = ""
        for row in all_kill_sql.rows:
            kill_sql = kill_sql + row[0]

        return kill_sql

    def kill_session(self, thread_ids):
        """Kill sessions."""
        # Validate parameters, format: [[sid, serial#], [sid, serial#]].
        if [
            k
            for k in [[j for j in i if not isinstance(j, int)] for i in thread_ids]
            if k
        ]:
            return ResultSet(full_sql="")
        sql = """select 'alter system kill session ' || '''' || s.sid || ',' || s.serial# || '''' || ' immediate' || ';'
                         from v$process p, v$session s, v$sqlarea q
                         where p.addr = s.paddr
                         and s.sql_hash_value = q.hash_value
                         and s.sid || ',' || s.serial# in ({});""".format(
            ",".join(f"'{str(tid[0])},{str(tid[1])}'" for tid in thread_ids)
        )
        all_kill_sql = self.query(sql=sql)
        kill_sql = ""
        for row in all_kill_sql.rows:
            kill_sql = kill_sql + row[0]
        return self.execute(sql=kill_sql)

    def tablespace(self, offset=0, row_count=14):
        """Get tablespace information."""
        row_count = offset + row_count
        sql = """
        select f.* from (
            select rownum rownumber, e.* from (
                select a.tablespace_name,
                d.contents tablespace_type,
                d.status,
                round(a.bytes/1024/1024,2) total_space,
                round(b.bytes/1024/1024,2) used_space,
                round((b.bytes * 100) / a.bytes,2) pct_used
                from sys.sm$ts_avail a, sys.sm$ts_used b, sys.sm$ts_free c, dba_tablespaces d
                where a.tablespace_name = b.tablespace_name
                and a.tablespace_name = c.tablespace_name
                and a.tablespace_name = d.tablespace_name
                order by total_space desc ) e
                where rownum <=:row_count
        ) f where f.rownumber >=:offset;"""
        return self.query(
            sql=sql, parameters={"row_count": row_count, "offset": offset}
        )

    def tablespace_count(self):
        """Get tablespace count."""
        sql = """select count(*) from dba_tablespaces where contents != 'TEMPORARY'"""
        return self.query(sql=sql)

    def lock_info(self):
        """Get lock information."""
        sql = """
        select c.username,
               b.owner object_owner,
               a.object_id,
               b.object_name,
               a.locked_mode,
               c.sid related_sid,
               c.serial# related_serial#,
               c.machine,
               d.sql_text related_sql,
               d.sql_fulltext related_sql_full,
               c.sql_exec_start related_sql_exec_start
        from v$locked_object a,dba_objects b, v$session c, v$sqlarea d
        where b.object_id = a.object_id
        and a.session_id = c.sid
        and c.sql_hash_value = d.hash_value;"""

        return self.query(sql=sql)

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
