# -*- coding: UTF-8 -*-
import traceback
import simplejson as json
import datetime

from common.utils.aliyun_sdk import Aliyun
from sql.models import AliyunRdsConfig
from sql.engines.mysql import MysqlEngine
from sql.engines.models import ResultSet


class AliyunRDS(MysqlEngine):
    def __init__(self, instance=None):
        super().__init__(instance=instance)
        self.instance_name = instance.instance_name

    # Functions migrated from sql/aliyun_rds.py.
    def processlist(self, command_type, **kwargs):
        if command_type is None or command_type == "":
            command_type = "Query"

        # Get related RDS instance ID by instance name.
        instance_info = AliyunRdsConfig.objects.get(
            instance__instance_name=self.instance_name
        )
        # Call Aliyun API to get process data.
        process_info = Aliyun(rds=instance_info).RequestServiceOfCloudDBA(
            "ShowProcessList", {"Language": "zh", "Command": command_type}
        )

        # Extract process list.
        process_list = json.loads(process_info)["AttrData"]
        process_list = json.loads(process_list)["ProcessList"]

        result_set = ResultSet(full_sql="show processlist")
        result_set.rows = process_list

        return result_set

    def get_kill_command(self, thread_ids):
        # Get related RDS instance ID by instance name.
        instance_info = AliyunRdsConfig.objects.get(
            instance__instance_name=self.instance_name
        )
        # Call Aliyun API to get process data.
        request_info = Aliyun(rds=instance_info).RequestServiceOfCloudDBA(
            "CreateKillSessionRequest", {"Language": "zh", "ThreadIDs": thread_ids}
        )

        request_list = json.loads(request_info)["AttrData"]
        kill_sql = str(request_list)

        return kill_sql

    def kill(self, thread_ids):
        kill_sql = ""
        for i in thread_ids:
            kill_sql = kill_sql + f"kill {i};"
        result = ResultSet(full_sql=kill_sql)
        try:
            # Get related RDS instance ID by instance name.
            instance_info = AliyunRdsConfig.objects.get(
                instance__instance_name=self.instance_name
            )
            # Call Aliyun API to terminate process.
            service_request_param = {"Language": "zh"}
            kill_result = Aliyun(rds=instance_info).RequestServiceOfCloudDBA(
                "ConfirmKillSessionRequest", service_request_param
            )

            # Get processed result.
            kill_result = json.loads(kill_result)["AttrData"]

            result.rows = kill_result
        except Exception as e:
            logger.warning(
                f"Aliyun RDS statement execution failed, sql: {kill_sql}, details: {traceback.format_exc()}"
            )
            result.error = str(e)
        return result

    def tablespace(self, offset, limit):
        # Get related RDS instance ID by instance name.
        instance_info = AliyunRdsConfig.objects.get(
            instance__instance_name=self.instance_name
        )
        # Call Aliyun API to get process data.
        space_info = Aliyun(rds=instance_info).RequestServiceOfCloudDBA(
            "GetSpaceStatForTables", {"Language": "zh", "OrderType": "Data"}
        )

        # Extract process list.
        space_list = json.loads(space_info)["ListData"]
        if space_list:
            space_list = json.loads(space_list)
        else:
            space_list = []

        result = ResultSet(full_sql="select * FROM information_schema.tables")
        result.rows = space_list

        return result

    # Get SQL slow log summary.
    def slowquery_review(self, start_time, end_time, db_name, limit, offset):
        # Calculate page number.
        page_number = (int(offset) + int(limit)) / int(limit)
        values = {"PageSize": int(limit), "PageNumber": int(page_number)}
        # DBName is optional.
        if db_name:
            values["DBName"] = db_name

        # Convert UTC time to format required by Aliyun.
        start_time = "%sZ" % start_time
        end_time = "%sZ" % end_time

        # Get related RDS instance ID by instance name.
        instance_info = AliyunRdsConfig.objects.get(
            instance__instance_name=self.instance_name
        )
        # Call Aliyun API to fetch SQL slow log summary.
        slowsql = Aliyun(rds=instance_info).DescribeSlowLogs(
            start_time, end_time, **values
        )

        # Handle table precision and format time.
        sql_slow_log = json.loads(slowsql)["Items"]["SQLSlowLog"]
        for SlowLog in sql_slow_log:
            SlowLog["SQLId"] = str(SlowLog["SQLHASH"])
            SlowLog["CreateTime"] = Aliyun.utc2local(
                SlowLog["CreateTime"], utc_format="%Y-%m-%dZ"
            )

        result = {
            "total": json.loads(slowsql)["TotalRecordCount"],
            "rows": sql_slow_log,
            "PageSize": json.loads(slowsql)["PageRecordCount"],
            "PageNumber": json.loads(slowsql)["PageNumber"],
        }
        # Return query result.
        return result

    # Get SQL slow log details.
    def slowquery_review_history(
        self, start_time, end_time, db_name, sql_id, limit, offset
    ):
        # Calculate page number.
        page_number = (int(offset) + int(limit)) / int(limit)
        values = {"PageSize": int(limit), "PageNumber": int(page_number)}
        # SQLId and DBName are optional.
        if sql_id:
            values["SQLHASH"] = sql_id
        if db_name:
            values["DBName"] = db_name

        # Convert UTC time to format required by Aliyun.
        start_time = datetime.datetime.strptime(
            start_time, "%Y-%m-%d"
        ).date() - datetime.timedelta(days=1)
        start_time = "%sT16:00Z" % start_time
        end_time = "%sT15:59Z" % end_time

        # Get related RDS instance ID by instance name.
        instance_info = AliyunRdsConfig.objects.get(
            instance__instance_name=self.instance_name
        )
        # Call Aliyun API to fetch SQL slow log summary.
        slowsql = Aliyun(rds=instance_info).DescribeSlowLogRecords(
            start_time, end_time, **values
        )

        # Format time and filter HostAddress.
        sql_slow_record = json.loads(slowsql)["Items"]["SQLSlowRecord"]
        for SlowRecord in sql_slow_record:
            SlowRecord["ExecutionStartTime"] = Aliyun.utc2local(
                SlowRecord["ExecutionStartTime"], utc_format="%Y-%m-%dT%H:%M:%SZ"
            )
            SlowRecord["HostAddress"] = SlowRecord["HostAddress"].split("[")[0]

        result = {
            "total": json.loads(slowsql)["TotalRecordCount"],
            "rows": sql_slow_record,
            "PageSize": json.loads(slowsql)["PageRecordCount"],
            "PageNumber": json.loads(slowsql)["PageNumber"],
        }

        # Return query result.
        return result
