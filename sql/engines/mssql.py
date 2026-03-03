# -*- coding: UTF-8 -*-
import logging
import traceback
import re
import sqlparse

from . import EngineBase
import pyodbc
from .models import ResultSet, ReviewSet, ReviewResult
from sql.utils.data_masking import brute_mask
from common.utils.timer import FuncTimer

logger = logging.getLogger("default")


class MssqlEngine(EngineBase):
    test_query = "SELECT 1"

    name = "MsSQL"
    info = "MsSQL engine"

    def get_connection(self, db_name=None):
        if self.conn:
            return self.conn

        # Try to detect available ODBC drivers.
        available_drivers = []
        try:
            available_drivers = [driver for driver in pyodbc.drivers()]
        except Exception as e:
            logger.warning(f"Unable to fetch ODBC driver list: {e}")

        # Driver priority list.
        driver_priority = [
            "ODBC Driver 17 for SQL Server",
            "ODBC Driver 18 for SQL Server",
            "ODBC Driver 13 for SQL Server",
            "ODBC Driver 11 for SQL Server",
            "FreeTDS",
            "SQL Server",
        ]

        # Pick first available driver by priority.
        selected_driver = None
        if available_drivers:
            for driver in driver_priority:
                if driver in available_drivers:
                    selected_driver = driver
                    break

        # Fallback to default driver when nothing matched.
        if not selected_driver:
            selected_driver = "ODBC Driver 17 for SQL Server"
            if available_drivers:
                logger.warning(
                    "Recommended SQL Server ODBC driver not found, available drivers: "
                    f"{', '.join(available_drivers)}"
                )
                logger.warning(f"Falling back to default driver: {selected_driver}")
            else:
                logger.error(
                    "No available ODBC driver found, please install SQL Server ODBC driver"
                )

        # Build connection string (driver name must be wrapped with braces).
        # Use database default encoding; do not force UTF-8.
        if "ODBC Driver 17" in selected_driver or "ODBC Driver 18" in selected_driver:
            # ODBC Driver 17/18.
            connstr = """DRIVER={{{0}}};SERVER={1},{2};UID={3};PWD={4};
TrustServerCertificate=yes;connect timeout=10;""".format(
                selected_driver,
                self.host,
                self.port,
                self.user,
                self.password,
            )
        else:
            # Other drivers.
            connstr = """DRIVER={{{0}}};SERVER={1},{2};UID={3};PWD={4};
connect timeout=10;""".format(
                selected_driver,
                self.host,
                self.port,
                self.user,
                self.password,
            )

        # Add database name if provided.
        if db_name:
            connstr = f"{connstr};DATABASE={db_name}"

        try:
            self.conn = pyodbc.connect(connstr)
            # Let pyodbc use database default encoding.
            logger.info(f"Connected to SQL Server with driver '{selected_driver}'")
            return self.conn
        except pyodbc.Error as e:
            error_msg = str(e)
            if "Can't open lib" in error_msg or "file not found" in error_msg:
                # Provide more user-friendly error details.
                if available_drivers:
                    raise RuntimeError(
                        f"ODBC driver '{selected_driver}' is unavailable.\n"
                        f"Available drivers: {', '.join(available_drivers)}\n"
                        "Please install Microsoft ODBC Driver for SQL Server "
                        "or configure the correct driver.\n"
                        f"Original error: {error_msg}"
                    )
                else:
                    raise RuntimeError(
                        "No available ODBC driver found.\n"
                        "Please install Microsoft ODBC Driver for SQL Server.\n"
                        "Installation:\n"
                        f"  macOS: brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release && brew install msodbcsql17\n"
                        "  Linux: https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-odbc-driver-sql-server\n"
                        f"Original error: {error_msg}"
                    )
            else:
                # Raise other connection errors as-is.
                raise

    def get_all_databases(self):
        """Get database list, return a ResultSet."""
        sql = "SELECT name FROM master.sys.databases order by name"
        result = self.query(sql=sql)
        db_list = [
            row[0]
            for row in result.rows
            if row[0] not in ("master", "msdb", "tempdb", "model")
        ]
        result.rows = db_list
        return result

    def get_all_tables(self, db_name, **kwargs):
        """Get table list, return a ResultSet."""
        sql = """SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE' order by TABLE_NAME;"""
        result = self.query(db_name=db_name, sql=sql)
        tb_list = [row[0] for row in result.rows if row[0] not in ["test"]]
        result.rows = tb_list
        return result

    def get_group_tables_by_db(self, db_name):
        """
        Get tables and comments for a DB, grouped by first character.
        Example: {'a': ['account1', 'apply']}.
        """
        data = {}
        sql = f"""
        SELECT t.name AS table_name, 
            case when td.value is not null then convert(varchar(max),td.value) else '' end AS table_comment
        FROM    sysobjects t
        LEFT OUTER JOIN sys.extended_properties td
        ON      td.major_id = t.id
        AND     td.minor_id = 0
        AND     td.name = 'MS_Description'
        WHERE t.type = 'u' ORDER BY t.name;"""
        result = self.query(db_name=db_name, sql=sql)
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
        sql = """
            SELECT space.*,table_comment,index_length,IDENT_CURRENT(?) as auto_increment
            FROM (
            SELECT 
                t.NAME AS table_name,
                t.create_date as create_time,
                t.modify_date as update_time,
                p.rows AS table_rows,
                SUM(a.total_pages) * 8 AS data_total,
                SUM(a.used_pages) * 8 AS data_length,
                (SUM(a.total_pages) - SUM(a.used_pages)) * 8 AS data_free
            FROM 
                sys.tables t
            INNER JOIN      
                sys.indexes i ON t.OBJECT_ID = i.object_id
            INNER JOIN 
                sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
            INNER JOIN 
                sys.allocation_units a ON p.partition_id = a.container_id
            WHERE 
                t.NAME =?
                AND t.is_ms_shipped = 0
                AND i.OBJECT_ID > 255 
            GROUP BY 
                t.Name, t.create_date, t.modify_date, p.Rows) 
            AS space 
            INNER JOIN (
            SELECT      t.name AS table_name,
                        convert(varchar(max),td.value) AS table_comment
            FROM		sysobjects t
            LEFT OUTER JOIN sys.extended_properties td
                ON      td.major_id = t.id
                AND     td.minor_id = 0
                AND     td.name = 'MS_Description'
            WHERE t.type = 'u' and t.name = ?) AS comment 
            ON space.table_name = comment.table_name
            INNER JOIN (
            SELECT
                t.NAME				AS table_name,
                SUM(page_count * 8) AS index_length
            FROM sys.dm_db_index_physical_stats(
                db_id(), object_id(?), NULL, NULL, 'DETAILED') AS s
            JOIN sys.indexes AS i
            ON s.[object_id] = i.[object_id] AND s.index_id = i.index_id
            INNER JOIN      
                sys.tables t ON t.OBJECT_ID = i.object_id
            GROUP BY t.NAME
            ) AS index_size 
            ON index_size.table_name = space.table_name;
        """
        _meta_data = self.query(
            db_name,
            sql,
            parameters=(
                tb_name,
                tb_name,
                tb_name,
            ),
        )
        return {"column_list": _meta_data.column_list, "rows": _meta_data.rows[0]}

    def get_table_desc_data(self, db_name, tb_name, **kwargs):
        """Get table column metadata."""
        sql = """
            select COLUMN_NAME AS ColumnName, case when ISNUMERIC(CHARACTER_MAXIMUM_LENGTH)=1 
then DATA_TYPE + '(' + convert(varchar(max), CHARACTER_MAXIMUM_LENGTH) + ')' else DATA_TYPE end AS ColumnType,
                COLLATION_NAME AS CollationName,
                IS_NULLABLE AS IsNullable,
                COLUMN_DEFAULT AS DefaultValue
            from INFORMATION_SCHEMA.columns where TABLE_CATALOG=? and TABLE_NAME = ?;"""
        _desc_data = self.query(
            db_name,
            sql,
            parameters=(
                db_name,
                tb_name,
            ),
        )
        return {"column_list": _desc_data.column_list, "rows": _desc_data.rows}

    def get_table_index_data(self, db_name, tb_name, **kwargs):
        """Get table index metadata."""
        sql = """SELECT 
stuff((select ',' + COL_NAME(t.object_id,t.column_id) from sys.index_columns as t where i.object_id = t.object_id and 
i.index_id = t.index_id and t.is_included_column = 0 order by key_ordinal for xml path('')),1,1,'') as ColumnNames,
                i.name AS IndexName,
                is_unique as IsUnique,is_primary_key as IsPrimaryKey
            FROM sys.indexes AS i  
            WHERE i.object_id = OBJECT_ID(?)
            group by i.name,i.object_id,i.index_id,is_unique,is_primary_key;"""
        _index_data = self.query(db_name, sql, parameters=(tb_name,))
        return {"column_list": _index_data.column_list, "rows": _index_data.rows}

    def get_tables_metas_data(self, db_name, **kwargs):
        """Get all table metadata in DB for dictionary export."""
        sql = """SELECT t.name AS TABLE_NAME, 
            case when td.value is not null then convert(varchar(max),td.value) else '' end AS TABLE_COMMENT
        FROM    sysobjects t
        LEFT OUTER JOIN sys.extended_properties td
        ON      td.major_id = t.id
        AND     td.minor_id = 0
        AND     td.name = 'MS_Description'
        WHERE t.type = 'u' ORDER BY t.name;"""
        result = self.query(db_name=db_name, sql=sql)
        # query result to dict
        tbs = []
        for row in result.rows:
            tbs.append(dict(zip(result.column_list, row)))
        table_metas = []
        for tb in tbs:
            _meta = dict()
            engine_keys = [
                {"key": "COLUMN_NAME", "value": "Column Name"},
                {"key": "COLUMN_TYPE", "value": "Data Type"},
                {"key": "COLLATION_NAME", "value": "Collation"},
                {"key": "IS_NULLABLE", "value": "Nullable"},
                {"key": "COLUMN_DEFAULT", "value": "Default Value"},
            ]
            _meta["ENGINE_KEYS"] = engine_keys
            _meta["TABLE_INFO"] = tb
            sql_cols = """select COLUMN_NAME, case when ISNUMERIC(CHARACTER_MAXIMUM_LENGTH)=1 
then DATA_TYPE + '(' + convert(varchar(max), CHARACTER_MAXIMUM_LENGTH) + ')' else DATA_TYPE end COLUMN_TYPE,
                COLLATION_NAME,
                IS_NULLABLE,
                COLUMN_DEFAULT
            from INFORMATION_SCHEMA.columns where TABLE_CATALOG=? and TABLE_NAME = ?;"""
            query_result = self.query(
                db_name=db_name,
                sql=sql_cols,
                close_conn=False,
                parameters=(db_name, tb["TABLE_NAME"]),
            )

            columns = []
            # Convert query rows to dict.
            for row in query_result.rows:
                columns.append(dict(zip(query_result.column_list, row)))
            _meta["COLUMNS"] = tuple(columns)
            table_metas.append(_meta)
        return table_metas

    def get_all_columns_by_tb(self, db_name, tb_name, **kwargs):
        """Get all fields, return a ResultSet."""
        result = self.describe_table(db_name, tb_name)
        column_list = [row[0] for row in result.rows]
        result.rows = column_list
        return result

    def describe_table(self, db_name, tb_name, **kwargs):
        """return ResultSet"""
        sql = r"""select
        c.name ColumnName,
        t.name ColumnType,
        c.length  ColumnLength,
        c.scale   ColumnScale,
        c.isnullable ColumnNull,
            case when i.id is not null then 'Y' else 'N' end TablePk
        from (select name,id,uid from sysobjects where (xtype='U' or xtype='V') ) o 
        inner join syscolumns c on o.id=c.id 
        inner join systypes t on c.xtype=t.xusertype 
        left join sysusers u on u.uid=o.uid
        left join (select name,id,uid,parent_obj from sysobjects where xtype='PK' )  opk on opk.parent_obj=o.id 
        left join (select id,name,indid from sysindexes) ie on ie.id=o.id and ie.name=opk.name
        left join sysindexkeys i on i.id=o.id and i.colid=c.colid and i.indid=ie.indid
        WHERE O.name NOT LIKE 'MS%' AND O.name NOT LIKE 'SY%'
        and O.name=?
        order by o.name,c.colid"""
        result = self.query(db_name=db_name, sql=sql, parameters=(tb_name,))
        return result

    def query_check(self, db_name=None, sql=""):
        # Query checks: strip comments and split statements.
        result = {"msg": "", "bad_query": False, "filtered_sql": sql, "has_star": False}
        banned_keywords = [
            "ascii",
            "char",
            "charindex",
            "concat",
            "concat_ws",
            "difference",
            "format",
            "len",
            "nchar",
            "patindex",
            "quotename",
            "replace",
            "replicate",
            "reverse",
            "right",
            "soundex",
            "space",
            "str",
            "string_agg",
            "string_escape",
            "string_split",
            "stuff",
            "substring",
            "trim",
            "unicode",
        ]
        keyword_warning = ""
        star_patter = r"(^|,|\s)\*(\s|\(|$)"
        sql_whitelist = ["select", "sp_helptext"]
        # Build whitelist regex from whitelist list.
        whitelist_pattern = "^" + "|^".join(sql_whitelist)

        # Check whether this is SHOWPLAN query.
        sql_lower = sql.lower().strip()
        is_showplan = sql_lower.startswith("set showplan_all on")

        # Remove comments, validate syntax, and take first valid SQL.
        try:
            sql_cleaned = sqlparse.format(sql, strip_comments=True)
            sql_parts = sqlparse.split(sql_cleaned)

            if is_showplan:
                # SHOWPLAN query: extract actual SQL and validate it.
                actual_sql = None
                for part in sql_parts:
                    part_lower = part.strip().lower()
                    if not part_lower.startswith("set showplan_all"):
                        actual_sql = part.strip()
                        break

                if actual_sql:
                    # Check actual SQL against whitelist.
                    actual_sql_lower = actual_sql.lower()
                    if re.match(whitelist_pattern, actual_sql_lower) is None:
                        result["bad_query"] = True
                        result["msg"] = "Only {} syntax is supported!".format(
                            ",".join(sql_whitelist)
                        )
                        return result
                    # Return full SHOWPLAN SQL (including SET SHOWPLAN_ALL ON).
                    result["filtered_sql"] = sql.strip()
                    sql_lower = actual_sql_lower  # Used for subsequent checks.
                else:
                    # No actual SQL found in SHOWPLAN query.
                    result["bad_query"] = True
                    result["msg"] = "No valid SQL statement found in SHOWPLAN query"
                    return result
            else:
                # Normal query: use first SQL statement.
                sql = sql_parts[0] if sql_parts else sql
                result["filtered_sql"] = sql.strip()
                sql_lower = sql.lower()
        except IndexError:
            result["bad_query"] = True
            result["msg"] = "No valid SQL statement"
            return result

        # For normal queries, validate whitelist.
        if not is_showplan and re.match(whitelist_pattern, sql_lower) is None:
            result["bad_query"] = True
            result["msg"] = "Only {} syntax is supported!".format(
                ",".join(sql_whitelist)
            )
            return result
        if re.search(star_patter, sql_lower) is not None:
            keyword_warning += "Using * keyword is forbidden\n"
            result["has_star"] = True
        for keyword in banned_keywords:
            pattern = r"(^|,| |=){}( |\(|$)".format(keyword)
            if re.search(pattern, sql_lower) is not None:
                keyword_warning += "Using {} keyword is forbidden\n".format(keyword)
                result["bad_query"] = True
        if result.get("bad_query") or result.get("has_star"):
            result["msg"] = keyword_warning
        return result

    def filter_sql(self, sql="", limit_num=0):
        sql_lower = sql.lower()
        # Add row limit to query SQL.
        if re.match(r"^select", sql_lower):
            # If OFFSET ... FETCH NEXT already exists, do not add TOP.
            if re.search(r"\boffset\s+\d+\s+rows?\s+fetch\s+next", sql_lower, re.I):
                return sql.strip()
            # Do not add duplicate TOP.
            if sql_lower.find(" top ") == -1 and limit_num > 0:
                # For SELECT DISTINCT, place TOP after DISTINCT.
                distinct_match = re.match(r"^(select\s+distinct)(\s+.*)$", sql, re.I)
                if distinct_match:
                    return (
                        distinct_match.group(1)
                        + " top {}".format(limit_num)
                        + distinct_match.group(2)
                    )
                # Keep original case, replace first select only.
                return re.sub(
                    r"^select\s+",
                    "select top {} ".format(limit_num),
                    sql,
                    count=1,
                    flags=re.I,
                )
        return sql.strip()

    def query(
        self,
        db_name=None,
        sql="",
        limit_num=0,
        close_conn=True,
        parameters: tuple = None,
        **kwargs,
    ):
        """Return ResultSet."""
        result_set = ResultSet(full_sql=sql)
        try:
            conn = self.get_connection(db_name)
            cursor = conn.cursor()

            # Handle SHOWPLAN query (SET SHOWPLAN_ALL ON).
            sql_lower = sql.lower().strip()
            is_showplan = sql_lower.startswith("set showplan_all on")

            if is_showplan:
                # Parse SHOWPLAN query and extract actual SQL:
                # SET SHOWPLAN_ALL ON; <SQL>; SET SHOWPLAN_ALL OFF;
                sql_parts = sqlparse.split(sql)
                actual_sql = None
                for part in sql_parts:
                    part_lower = part.strip().lower()
                    if not part_lower.startswith("set showplan_all"):
                        actual_sql = part.strip()
                        break

                if actual_sql:
                    try:
                        # Enable SHOWPLAN.
                        cursor.execute("SET SHOWPLAN_ALL ON")
                        # Execute actual SQL.
                        cursor.execute(actual_sql)
                        # Fetch SHOWPLAN result.
                        rows = cursor.fetchall()
                        fields = cursor.description
                    finally:
                        # Ensure SHOWPLAN is disabled even on failure.
                        try:
                            cursor.execute("SET SHOWPLAN_ALL OFF")
                        except:
                            pass
                else:
                    # Fallback: execute full SQL if actual SQL not found.
                    cursor.execute(sql)
                    rows = cursor.fetchall()
                    fields = cursor.description
            else:
                # Normal query.
                # https://github.com/mkleehammer/pyodbc/wiki/Cursor#executesql-parameters
                if parameters:
                    cursor.execute(sql, *parameters)
                else:
                    cursor.execute(sql)
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
                f"MsSQL statement execution failed, SQL: {sql}, "
                f"error: {traceback.format_exc()}"
            )
            result_set.error = str(e)
        finally:
            if close_conn:
                self.close()
        return result_set

    def query_masking(self, db_name=None, sql="", resultset=None):
        """Given SQL, DB name and result set, return masked result set."""
        # Only mask SELECT statements.
        if re.match(r"^select", sql, re.I):
            filtered_result = brute_mask(self.instance, resultset)
            filtered_result.is_masked = True
        else:
            filtered_result = resultset
        return filtered_result

    def execute_check(self, db_name=None, sql=""):
        """Pre-check before workflow execution, return ReviewSet."""
        from common.config import SysConfig
        from sql.utils.sql_utils import get_syntax_type

        config = SysConfig()
        check_result = ReviewSet(full_sql=sql)
        # Unsupported/high-risk statement checks.
        line = 1
        critical_ddl_regex = config.get("critical_ddl_regex", "")
        p = re.compile(critical_ddl_regex) if critical_ddl_regex else None
        check_result.syntax_type = 2  # Default DML.

        # Split by GO first (MSSQL batch separator), keep original formatting.
        split_reg = re.compile(r"^\s*GO\s*$", re.I | re.M)
        sql_batches = re.split(split_reg, sql)

        # Get all SQL statements (split by GO, then split each batch by sqlparse).
        # Keep original formatting including line breaks for test expectations.
        all_statements = []
        for batch in sql_batches:
            if not batch.strip():
                continue
            # Split SQL inside each batch, while preserving original text display.
            batch_statements = sqlparse.split(batch)
            for stmt in batch_statements:
                # Keep original formatting without strip.
                if stmt.strip():
                    # If one statement only, keep original batch to preserve format.
                    # Otherwise use split statement.
                    if len(batch_statements) == 1:
                        all_statements.append(batch)
                    else:
                        all_statements.append(stmt)
        # Get DB connection for syntax check.
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            if db_name:
                cursor.execute(f"USE [{db_name}]")
        except Exception as e:
            logger.warning(f"MSSQL connection failed, error: {traceback.format_exc()}")
            # On connection failure, still perform basic rule checks.
            conn = None
            cursor = None

        # Check each SQL statement.
        for statement in all_statements:
            # Strip comments.
            statement_clean = sqlparse.format(statement, strip_comments=True).strip()
            if not statement_clean:
                continue

            # Unsupported statement check (SELECT query).
            if re.match(r"^select", statement_clean, re.I):
                result = ReviewResult(
                    id=line,
                    errlevel=2,
                    stagestatus="Rejected unsupported statement",
                    errormessage=(
                        "Only DML and DDL statements are supported. "
                        "Use SQL query feature for SELECT statements!"
                    ),
                    sql=statement,
                    affected_rows=0,
                    execute_time=0,
                )
            # High-risk statement check.
            elif critical_ddl_regex and p and p.match(statement_clean.lower()):
                result = ReviewResult(
                    id=line,
                    errlevel=2,
                    stagestatus="Rejected high-risk SQL",
                    errormessage=(
                        f"Submitting statements matching {critical_ddl_regex} "
                        f"is prohibited!"
                    ),
                    sql=statement,
                    affected_rows=0,
                    execute_time=0,
                )
            # Normal statement, perform syntax check.
            else:
                # Determine workflow syntax type.
                syntax_type = get_syntax_type(statement_clean, parser=True)
                if syntax_type == "DDL":
                    check_result.syntax_type = 1

                # Run syntax check when connection is available.
                if conn and cursor:
                    try:
                        # Use SET PARSEONLY ON for syntax check (parse only).
                        cursor.execute("SET PARSEONLY ON")
                        cursor.execute(statement_clean)
                        cursor.execute("SET PARSEONLY OFF")
                        # Syntax check passed.
                        result = ReviewResult(
                            id=line,
                            errlevel=0,
                            stagestatus="Audit completed",
                            errormessage="None",
                            sql=statement,
                            affected_rows=0,
                            execute_time=0,
                        )
                    except Exception as e:
                        # Syntax check failed.
                        error_msg = str(e)
                        result = ReviewResult(
                            id=line,
                            errlevel=2,
                            stagestatus="Syntax error",
                            errormessage=f"Syntax check failed: {error_msg}",
                            sql=statement,
                            affected_rows=0,
                            execute_time=0,
                        )
                        # Ensure PARSEONLY is reset.
                        try:
                            cursor.execute("SET PARSEONLY OFF")
                        except:
                            pass
                else:
                    # Without connection, pass by default (rule checks only).
                    result = ReviewResult(
                        id=line,
                        errlevel=0,
                        stagestatus="Audit completed",
                        errormessage="None",
                        sql=statement,
                        affected_rows=0,
                        execute_time=0,
                    )

            check_result.rows.append(result)
            line += 1

        # Close connection.
        if cursor:
            try:
                cursor.execute("SET PARSEONLY OFF")
            except:
                pass
            try:
                cursor.close()
            except:
                pass
        if conn and self.conn != conn:
            try:
                conn.close()
            except:
                pass

        # Count warnings and errors.
        for r in check_result.rows:
            if r.errlevel == 1:
                check_result.warning_count += 1
            if r.errlevel == 2:
                check_result.error_count += 1

        return check_result

    def execute_workflow(self, workflow):
        if workflow.is_backup:
            # TODO mssql backup is not implemented.
            pass
        return self.execute(
            db_name=workflow.db_name, sql=workflow.sqlworkflowcontent.sql_content
        )

    def execute(self, db_name=None, sql="", close_conn=True):
        """Execute SQL statements and return ReviewSet."""
        execute_result = ReviewSet(full_sql=sql)
        conn = self.get_connection(db_name=db_name)
        cursor = conn.cursor()

        # Split by GO first (MSSQL batch separator).
        split_reg = re.compile(r"^\s*GO\s*$", re.I | re.M)
        sql_batches = re.split(split_reg, sql)

        # Get all SQL statements (split by GO, then sqlparse each batch).
        all_statements = []
        for batch in sql_batches:
            batch = batch.strip()
            if not batch:
                continue
            # Use sqlparse split inside each batch.
            batch_statements = sqlparse.split(batch)
            for stmt in batch_statements:
                stmt = stmt.strip()
                if stmt:
                    all_statements.append(stmt)

        # Open transaction (MSSQL autocommit default; explicit transaction for rollback).
        conn.autocommit = False

        rowid = 1
        # Set DB context and record USE statement when db_name is provided.
        if db_name:
            use_sql = f"USE [{db_name}]"
            try:
                cursor.execute(use_sql)
                execute_result.rows.append(
                    ReviewResult(
                        id=rowid,
                        errlevel=0,
                        stagestatus="Execute Successfully",
                        errormessage="None",
                        sql=use_sql,
                        affected_rows=0,
                        execute_time=0,
                    )
                )
                rowid += 1
            except Exception as e:
                logger.warning(f"MSSQL USE statement failed: {traceback.format_exc()}")
                execute_result.error = str(e)
                execute_result.rows.append(
                    ReviewResult(
                        id=rowid,
                        errlevel=2,
                        stagestatus="Execute Failed",
                        errormessage=f"Exception info: {e}",
                        sql=use_sql,
                        affected_rows=0,
                        execute_time=0,
                    )
                )
                rowid += 1

        for idx, statement in enumerate(all_statements):
            try:
                # Use FuncTimer to track execution time.
                with FuncTimer() as t:
                    cursor.execute(statement)
                    # Commit immediately after each successful statement (MSSQL behavior).
                    conn.commit()
                execute_result.rows.append(
                    ReviewResult(
                        id=rowid,
                        errlevel=0,
                        stagestatus="Execute Successfully",
                        errormessage="None",
                        sql=statement,
                        affected_rows=cursor.rowcount,
                        execute_time=t.cost,
                    )
                )
            except Exception as e:
                logger.warning(
                    f"Mssql command execution failed, SQL: {statement}, "
                    f"error: {traceback.format_exc()}"
                )
                execute_result.error = str(e)
                # Append failed statement to execution results.
                execute_result.rows.append(
                    ReviewResult(
                        id=rowid,
                        errlevel=2,
                        stagestatus="Execute Failed",
                        errormessage=f"Exception info: {e}",
                        sql=statement,
                        affected_rows=0,
                        execute_time=0,
                    )
                )
                # On failure, roll back current transaction if needed.
                try:
                    conn.rollback()
                except:
                    pass
                # Mark following statements as audit passed but not executed.
                rowid += 1
                for remaining_statement in all_statements[idx + 1 :]:
                    remaining_statement = remaining_statement.strip()
                    if not remaining_statement:
                        continue
                    execute_result.rows.append(
                        ReviewResult(
                            id=rowid,
                            errlevel=0,
                            stagestatus="Audit completed",
                            errormessage="Previous statement failed, not executed",
                            sql=remaining_statement,
                            affected_rows=0,
                            execute_time=0,
                        )
                    )
                    rowid += 1
                # Stop execution and do not process remaining statements.
                break
            rowid += 1
        if close_conn:
            self.close()
        return execute_result

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
