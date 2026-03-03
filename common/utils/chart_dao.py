# -*- coding: UTF-8 -*-

from datetime import timedelta
from django.db import connection


class ChartDao(object):
    # Query data directly from Archery DB for dashboards
    @staticmethod
    def __query(sql):
        cursor = connection.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        fields = cursor.description
        column_list = []
        if fields:
            for i in fields:
                column_list.append(i[0])
        return {"column_list": column_list, "rows": rows}

    # Generate continuous date range
    @staticmethod
    def get_date_list(begin_date, end_date):
        dates = []
        this_day = begin_date
        while this_day <= end_date:
            dates += [this_day.strftime("%Y-%m-%d")]
            this_day += timedelta(days=1)
        return dates

    # SQL syntax type distribution
    def syntax_type(self, start_date, end_date):
        sql = """
        select
          case when syntax_type = 1
            then 'DDL'
          when syntax_type = 2
            then 'DML'
          else 'Other'
          end as syntax_type,
          count(*)
        from sql_workflow 
        where create_time >= '{}' and create_time <= '{}'
        group by syntax_type;""".format(start_date, end_date)
        return self.__query(sql)

    # Workflow volume by date
    def workflow_by_date(self, start_date, end_date):
        sql = """
        select
          date_format(create_time, '%Y-%m-%d'),
          count(*)
        from sql_workflow
        where create_time >= '{}' and create_time <= '{}'
        group by date_format(create_time, '%Y-%m-%d')
        order by 1 asc;""".format(start_date, end_date)
        return self.__query(sql)

    # Workflow count by group
    def workflow_by_group(self, start_date, end_date):
        sql = """
        select
          group_name,
          count(*)
        from sql_workflow
        where create_time >= '{}' and create_time <= '{}'
        group by group_id
        order by count(*) desc;""".format(start_date, end_date)
        return self.__query(sql)

    def workflow_by_user(self, start_date, end_date):
        """Workflow count by user."""
        # TODO select engineer ID and join user table for display name
        sql = """
        select
          engineer_display,
          count(*)
        from sql_workflow
        where create_time >= '{}' and create_time <= '{}'
        group by engineer_display
        order by count(*) desc;""".format(start_date, end_date)
        return self.__query(sql)

    # SQL query stats (daily scanned rows)
    def querylog_effect_row_by_date(self, start_date, end_date):
        sql = """
        select
          date_format(create_time, '%Y-%m-%d'),
          sum(effect_row)
        from query_log
        where create_time >= '{}' and create_time <= '{}'
        group by date_format(create_time, '%Y-%m-%d')
        order by sum(effect_row) desc;""".format(start_date, end_date)
        return self.__query(sql)

    # SQL query stats (daily query count)
    def querylog_count_by_date(self, start_date, end_date):
        sql = """
        select
          date_format(create_time, '%Y-%m-%d'),
          count(*)
        from query_log
        where create_time >= '{}' and create_time <= '{}'
        group by date_format(create_time, '%Y-%m-%d')
        order by count(*) desc;""".format(start_date, end_date)
        return self.__query(sql)

    # SQL query stats (scanned rows by user)
    def querylog_effect_row_by_user(self, start_date, end_date):
        sql = """
        select 
          user_display,
          sum(effect_row)
        from query_log
        where create_time >= '{}' and create_time <= '{}'
        group by user_display
        order by sum(effect_row) desc
        limit 20;""".format(start_date, end_date)
        return self.__query(sql)

    # SQL query stats (scanned rows by DB)
    def querylog_effect_row_by_db(self, start_date, end_date):
        sql = """
       select
          db_name,
          sum(effect_row)
        from query_log
        where create_time >= '{}' and create_time <= '{}'
        group by db_name
        order by sum(effect_row) desc
        limit 20;""".format(start_date, end_date)
        return self.__query(sql)

    # Slow log trend (by execution count)
    def slow_query_review_history_by_cnt(self, checksum):
        sql = f"""select sum(ts_cnt),date(date_add(ts_min, interval 8 HOUR))
from mysql_slow_query_review_history
where checksum = '{checksum}'
group by date(date_add(ts_min, interval 8 HOUR));"""
        return self.__query(sql)

    # Slow log trend (by duration)
    def slow_query_review_history_by_pct_95_time(self, checksum):
        sql = f"""select truncate(Query_time_pct_95,6),date(date_add(ts_min, interval 8 HOUR))
from mysql_slow_query_review_history
where checksum = '{checksum}'
group by date(date_add(ts_min, interval 8 HOUR));"""
        return self.__query(sql)

    # Slow log stats by db/user
    def slow_query_count_by_db_by_user(self, start_date, end_date):
        sql = """
        select
            concat(db_max,' user: ' ,user_max),
            sum(ts_cnt) 
        from mysql_slow_query_review_history 
        where ts_min >= '{}' and ts_min <= '{}'
        group by db_max,user_max order by sum(ts_cnt) desc limit 50;
        """.format(start_date, end_date)
        return self.__query(sql)

    # Slow log stats by db
    def slow_query_count_by_db(self, start_date, end_date):
        sql = """
        select
            db_max,
            sum(ts_cnt) 
        from mysql_slow_query_review_history 
        where ts_min >= '{}' and ts_min <= '{}'
        group by db_max order by sum(ts_cnt) desc limit 50;
        """.format(start_date, end_date)
        return self.__query(sql)

    # DB instance type distribution
    def instance_count_by_type(self):
        sql = """
        select db_type,count(1) as cn 
        from sql_instance 
        group by db_type 
        order by 2 desc;"""
        return self.__query(sql)

    def query_sql_prod_bill(self, start_date, end_date):
        sql = """
            SELECT
                CASE
                        a.STATUS 
                        WHEN 'workflow_finish' THEN
                        'Finished' 
                        WHEN 'workflow_autoreviewwrong' THEN
                        'Auto-review rejected' 
                        WHEN 'workflow_abort' THEN
                        'Manually aborted' 
                        WHEN 'workflow_exception' THEN
                        'Execution exception' 
                        WHEN 'workflow_review_pass' THEN
                        'Review passed' 
                        WHEN 'workflow_queuing' THEN
                        'Queued' 
                        WHEN 'workflow_executing' THEN
                        'Executing' 
                        WHEN 'workflow_manreviewing' THEN
                        'Waiting for reviewer' ELSE 'Unknown status' 
                    END AS status_desc,
                    COUNT( 1 ) AS count 
                FROM sql_workflow a
                    INNER JOIN sql_instance b ON ( a.instance_id = b.id ) 
                WHERE a.create_time >= '{}' and a.create_time <= '{}'
                GROUP BY a.STATUS
                ORDER BY 1;
          """.format(start_date, end_date)
        return self.__query(sql)

    def query_instance_env_info(self):
        sql = """
             SELECT
            db_type,
            type,
            COUNT(1) AS cn
        FROM
            sql_instance
        GROUP BY
            db_type,
            type
        ORDER BY
            1,
            2;
        """
        return self.__query(sql)
