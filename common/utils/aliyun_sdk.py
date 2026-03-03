# -*- coding: UTF-8 -*-
import datetime
import traceback

from aliyunsdkcore.client import AcsClient
from aliyunsdkrds.request.v20140815 import (
    DescribeSlowLogsRequest,
    DescribeSlowLogRecordsRequest,
    RequestServiceOfCloudDBARequest,
)
import simplejson as json
import logging

logger = logging.getLogger("default")


class Aliyun(object):
    def __init__(self, rds):
        try:
            self.DBInstanceId = rds.rds_dbinstanceid
            ak = rds.ak.raw_key_id
            secret = rds.ak.raw_key_secret
            self.clt = AcsClient(ak=ak, secret=secret)
        except Exception as m:
            raise Exception(
                f"Alibaba Cloud authentication failed: {m}{traceback.format_exc()}"
            )

    def request_api(self, request, *values):
        if values:
            for value in values:
                for k, v in value.items():
                    request.add_query_param(k, v)
        request.set_accept_format("json")
        result = self.clt.do_action_with_exception(request)
        return json.dumps(
            json.loads(result.decode("utf-8")),
            indent=4,
            sort_keys=False,
            ensure_ascii=False,
        )

    # Convert Alibaba Cloud UTC time to local time zone
    @staticmethod
    def utc2local(utc, utc_format):
        utc_time = datetime.datetime.strptime(utc, utc_format)
        local_tm = datetime.datetime.fromtimestamp(0)
        utc_tm = datetime.datetime.utcfromtimestamp(0)
        localtime = utc_time + (local_tm - utc_tm)
        return localtime

    def DescribeSlowLogs(self, StartTime, EndTime, **kwargs):
        """Get slow log list for an instance: DBName, SortKey, PageSize, PageNumber."""
        request = DescribeSlowLogsRequest.DescribeSlowLogsRequest()
        values = {
            "action_name": "DescribeSlowLogs",
            "DBInstanceId": self.DBInstanceId,
            "StartTime": StartTime,
            "EndTime": EndTime,
            "SortKey": "TotalExecutionCounts",
        }
        values = dict(values, **kwargs)
        result = self.request_api(request, values)
        return result

    def DescribeSlowLogRecords(self, StartTime, EndTime, **kwargs):
        """Get slow log details: SQLId, DBName, PageSize, PageNumber."""
        request = DescribeSlowLogRecordsRequest.DescribeSlowLogRecordsRequest()
        values = {
            "action_name": "DescribeSlowLogRecords",
            "DBInstanceId": self.DBInstanceId,
            "StartTime": StartTime,
            "EndTime": EndTime,
        }
        values = dict(values, **kwargs)
        result = self.request_api(request, values)
        return result

    def RequestServiceOfCloudDBA(
        self, ServiceRequestType, ServiceRequestParam, **kwargs
    ):
        """
        Get monitoring stats:
          'GetTimedMonData', {"Language":"zh","KeyGroup":"mem_cpu_usage","KeyName":"",
          "StartTime":"2018-01-15T04:03:26Z","EndTime":"2018-01-15T05:03:26Z"}
          key groups: mem_cpu_usage, iops_usage, detailed_disk_space
        Get process info:
          'ShowProcessList', {"Language":"zh","Command":"Query"}  -- Not Sleep, All
        Kill process:
          'ConfirmKillSessionRequest', {"Language":"zh","SQLRequestID":75865,
          "SQLStatement":"kill 34022786;"}
        Get table-space info:
          'GetSpaceStatForTables', {"Language":"zh", "OrderType":"Data"}
        Get resource usage info:
          'GetResourceUsage', {"Language":"zh"}
        """
        request = RequestServiceOfCloudDBARequest.RequestServiceOfCloudDBARequest()
        values = {
            "action_name": "RequestServiceOfCloudDBA",
            "DBInstanceId": self.DBInstanceId,
            "ServiceRequestType": ServiceRequestType,
            "ServiceRequestParam": ServiceRequestParam,
        }
        values = dict(values, **kwargs)
        result = self.request_api(request, values)
        return result
