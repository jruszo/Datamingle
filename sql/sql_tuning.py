# -*- coding: UTF-8 -*-

import time

from common.utils.const import SQLTuning
from sql.engines import get_engine
from sql.models import Instance
from sql.utils.sql_utils import extract_tables


class SqlTuning(object):
    def __init__(self, instance_name, db_name, sqltext):
        instance = Instance.objects.get(instance_name=instance_name)
        query_engine = get_engine(instance=instance)
        self.engine = query_engine
        self.db_name = self.engine.escape_string(db_name)
        self.sqltext = sqltext
        self.sql_variable = """
    select
      lower(variable_name),
      variable_value
    from performance_schema.global_variables
    where upper(variable_name) in ('%s')
    order by variable_name;""" % ("','".join(SQLTuning.SYS_PARM_FILTER))
        self.sql_optimizer_switch = """
    select variable_value
    from performance_schema.global_variables
    where upper(variable_name) = 'OPTIMIZER_SWITCH';
    """
        self.sql_table_info = """
    select
      table_name,
      engine,
      row_format                                           as format,
      table_rows,
      avg_row_length                                       as avg_row,
      round((data_length + index_length) / 1024 / 1024, 2) as total_mb,
      round((data_length) / 1024 / 1024, 2)                as data_mb,
      round((index_length) / 1024 / 1024, 2)               as index_mb
    from information_schema.tables
    where table_schema = '%s' and table_name = '%s'
    """
        self.sql_table_index = """
    select
      table_name,
      index_name,
      non_unique,
      seq_in_index,
      column_name,
      collation,
      cardinality,
      nullable,
      index_type
    from information_schema.statistics
    where table_schema = '%s' and table_name = '%s'
    order by 1, 3;    
    """

    def __extract_tables(self):
        """Get table names from SQL text."""
        return [i["name"].strip("`") for i in extract_tables(self.sqltext)]

    def basic_information(self):
        return self.engine.query(sql="select @@version", close_conn=False).to_sep_dict()

    def sys_parameter(self):
        # Get MySQL version information.
        server_version = self.engine.server_version
        if server_version < (5, 7, 0):
            sql = self.sql_variable.replace("performance_schema", "information_schema")
        else:
            sql = self.sql_variable
        return self.engine.query(sql=sql, close_conn=False).to_sep_dict()

    def optimizer_switch(self):
        # Get MySQL version information.
        server_version = self.engine.server_version
        if server_version < (5, 7, 0):
            sql = self.sql_optimizer_switch.replace(
                "performance_schema", "information_schema"
            )
        else:
            sql = self.sql_optimizer_switch
        return self.engine.query(sql=sql, close_conn=False).to_sep_dict()

    def sqlplan(self):
        plan = self.engine.query(
            self.db_name, "explain " + self.sqltext, close_conn=False
        ).to_sep_dict()
        optimizer_rewrite_sql = self.engine.query(
            sql="show warnings", close_conn=False
        ).to_sep_dict()
        return plan, optimizer_rewrite_sql

    # Related table extraction is limited and currently returns only one table.
    def object_statistics(self):
        object_statistics = []
        for index, table_name in enumerate(self.__extract_tables()):
            object_statistics.append(
                {
                    "structure": self.engine.query(
                        db_name=self.db_name,
                        sql=f"show create table `{table_name}`;",
                        close_conn=False,
                    ).to_sep_dict(),
                    "table_info": self.engine.query(
                        sql=self.sql_table_info % (self.db_name, table_name),
                        close_conn=False,
                    ).to_sep_dict(),
                    "index_info": self.engine.query(
                        sql=self.sql_table_index % (self.db_name, table_name),
                        close_conn=False,
                    ).to_sep_dict(),
                }
            )
        return object_statistics

    def exec_sql(self):
        result = {
            "EXECUTE_TIME": 0,
            "BEFORE_STATUS": {"column_list": [], "rows": []},
            "AFTER_STATUS": {"column_list": [], "rows": []},
            "SESSION_STATUS(DIFFERENT)": {
                "column_list": ["status_name", "before", "after", "diff"],
                "rows": [],
            },
            "PROFILING_DETAIL": {"column_list": [], "rows": []},
            "PROFILING_SUMMARY": {"column_list": [], "rows": []},
        }
        sql_profiling = """select concat(upper(left(variable_name,1)),
                            substring(lower(variable_name),
                            2,
                            (length(variable_name)-1))) var_name,
                            variable_value var_value 
                        from performance_schema.session_status order by 1"""

        # Get MySQL version information.
        server_version = self.engine.server_version
        if server_version < (5, 7, 0):
            sql = sql_profiling.replace("performance_schema", "information_schema")
        else:
            sql = sql_profiling
        self.engine.query(sql="set profiling=1", close_conn=False).to_sep_dict()
        records = self.engine.query(
            sql="select ifnull(max(query_id),0) from INFORMATION_SCHEMA.PROFILING",
            close_conn=False,
        ).to_sep_dict()
        query_id = records["rows"][0][0] + 3  # skip next sql
        # Get status before execution.
        result["BEFORE_STATUS"] = self.engine.query(
            sql=sql, close_conn=False
        ).to_sep_dict()

        # Execute query and measure execution time.
        t_start = time.time()
        self.engine.query(sql=self.sqltext, close_conn=False).to_sep_dict()
        t_end = time.time()
        cost_time = "%5s" % "{:.4f}".format(t_end - t_start)
        result["EXECUTE_TIME"] = cost_time

        # Get status after execution.
        result["AFTER_STATUS"] = self.engine.query(
            sql=sql, close_conn=False
        ).to_sep_dict()

        # Get profiling detail information.
        result["PROFILING_DETAIL"] = self.engine.query(
            sql="select STATE,DURATION,CPU_USER,CPU_SYSTEM,BLOCK_OPS_IN,BLOCK_OPS_OUT ,MESSAGES_SENT ,MESSAGES_RECEIVED ,PAGE_FAULTS_MAJOR ,PAGE_FAULTS_MINOR ,SWAPS from INFORMATION_SCHEMA.PROFILING where query_id="
            + str(query_id)
            + " order by seq",
            close_conn=False,
        ).to_sep_dict()
        result["PROFILING_SUMMARY"] = self.engine.query(
            sql="SELECT STATE,SUM(DURATION) AS Total_R,ROUND(100*SUM(DURATION)/(SELECT SUM(DURATION) FROM INFORMATION_SCHEMA.PROFILING WHERE QUERY_ID="
            + str(query_id)
            + "),2) AS Pct_R,COUNT(*) AS Calls,SUM(DURATION)/COUNT(*) AS R_Call FROM INFORMATION_SCHEMA.PROFILING WHERE QUERY_ID="
            + str(query_id)
            + " GROUP BY STATE ORDER BY Total_R DESC",
            close_conn=False,
        ).to_sep_dict()

        # Compare status before and after execution.
        before_status_rows = [list(item) for item in result["BEFORE_STATUS"]["rows"]]
        after_status_rows = [list(item) for item in result["AFTER_STATUS"]["rows"]]
        for index, item in enumerate(before_status_rows):
            if before_status_rows[index][1] != after_status_rows[index][1]:
                before_status_rows[index].append(after_status_rows[index][1])
                before_status_rows[index].append(
                    str(
                        float(after_status_rows[index][1])
                        - float(before_status_rows[index][1])
                    )
                )
        diff_rows = [item for item in before_status_rows if len(item) == 4]
        result["SESSION_STATUS(DIFFERENT)"]["rows"] = diff_rows
        return result
