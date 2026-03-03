# -*- coding: UTF-8 -*-
import MySQLdb
import simplejson as json
import datetime
import pymysql
from django.contrib.auth.decorators import permission_required
from django.db.models import F, Sum, Value as V, Max
from django.db.models.functions import Concat
from django.http import HttpResponse
from django.views.decorators.cache import cache_page
from pyecharts.charts import Line
from pyecharts import options as opts
from common.utils.chart_dao import ChartDao
from sql.engines import get_engine

from sql.utils.resource_group import user_instances
from common.utils.extend_json_encoder import ExtendJSONEncoder
from .models import Instance, SlowQuery, SlowQueryHistory, AliyunRdsConfig


import logging

logger = logging.getLogger("default")


# Get slow SQL summary.
@permission_required("sql.menu_slowquery", raise_exception=True)
def slowquery_review(request):
    instance_name = request.POST.get("instance_name")
    start_time = request.POST.get("StartTime")
    end_time = request.POST.get("EndTime")
    db_name = request.POST.get("db_name")
    limit = int(request.POST.get("limit"))
    offset = int(request.POST.get("offset"))
    # Server-side permission validation.
    try:
        user_instances(request.user, db_type=["mysql"]).get(instance_name=instance_name)
    except Exception:
        result = {
            "status": 1,
            "msg": "Your group is not associated with this instance",
            "data": [],
        }
        return HttpResponse(json.dumps(result), content_type="application/json")

    # Determine whether this is RDS or another instance type.
    instance_info = Instance.objects.get(instance_name=instance_name)
    if AliyunRdsConfig.objects.filter(instance=instance_info, is_enable=True).exists():
        # Call Aliyun slow-query API.
        query_engine = get_engine(instance=instance_info)
        result = query_engine.slowquery_review(
            start_time, end_time, db_name, limit, offset
        )
    else:
        limit = offset + limit
        search = request.POST.get("search")
        sortName = str(request.POST.get("sortName"))
        sortOrder = str(request.POST.get("sortOrder")).lower()

        # Time handling.
        end_time = datetime.datetime.strptime(
            end_time, "%Y-%m-%d"
        ) + datetime.timedelta(days=1)
        filter_kwargs = {"slowqueryhistory__db_max": db_name} if db_name else {}
        # Get slow-query summary data.
        slowsql_obj = (
            SlowQuery.objects.filter(
                slowqueryhistory__hostname_max=(
                    instance_info.host + ":" + str(instance_info.port)
                ),
                slowqueryhistory__ts_min__range=(start_time, end_time),
                fingerprint__icontains=search,
                **filter_kwargs
            )
            .annotate(SQLText=Max("fingerprint"), SQLId=F("checksum"))
            .values("SQLText", "SQLId")
            .annotate(
                CreateTime=Max("slowqueryhistory__ts_max"),
                DBName=Max("slowqueryhistory__db_max"),  # Database
                QueryTimeAvg=Sum("slowqueryhistory__query_time_sum")
                / Sum("slowqueryhistory__ts_cnt"),  # Average execution time
                MySQLTotalExecutionCounts=Sum(
                    "slowqueryhistory__ts_cnt"
                ),  # Total execution count
                MySQLTotalExecutionTimes=Sum(
                    "slowqueryhistory__query_time_sum"
                ),  # Total execution time
                ParseTotalRowCounts=Sum(
                    "slowqueryhistory__rows_examined_sum"
                ),  # Total scanned rows
                ReturnTotalRowCounts=Sum(
                    "slowqueryhistory__rows_sent_sum"
                ),  # Total returned rows
                ParseRowAvg=Sum("slowqueryhistory__rows_examined_sum")
                / Sum("slowqueryhistory__ts_cnt"),  # Average scanned rows
                ReturnRowAvg=Sum("slowqueryhistory__rows_sent_sum")
                / Sum("slowqueryhistory__ts_cnt"),  # Average returned rows
            )
        )
        slow_sql_count = slowsql_obj.count()
        # Default sort: total execution count descending.
        slow_sql_list = slowsql_obj.order_by(
            "-" + sortName if "desc".__eq__(sortOrder) else sortName
        )[offset:limit]

        # Serialize QuerySet.
        sql_slow_log = []
        for SlowLog in slow_sql_list:
            SlowLog["QueryTimeAvg"] = round(SlowLog["QueryTimeAvg"], 6)
            SlowLog["MySQLTotalExecutionTimes"] = round(
                SlowLog["MySQLTotalExecutionTimes"], 6
            )
            SlowLog["ParseRowAvg"] = int(SlowLog["ParseRowAvg"])
            SlowLog["ReturnRowAvg"] = int(SlowLog["ReturnRowAvg"])
            sql_slow_log.append(SlowLog)
        result = {"total": slow_sql_count, "rows": sql_slow_log}

    # Return query result.
    return HttpResponse(
        json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
        content_type="application/json",
    )


# Get slow SQL details.
@permission_required("sql.menu_slowquery", raise_exception=True)
def slowquery_review_history(request):
    instance_name = request.POST.get("instance_name")
    start_time = request.POST.get("StartTime")
    end_time = request.POST.get("EndTime")
    db_name = request.POST.get("db_name")
    sql_id = request.POST.get("SQLId")
    limit = int(request.POST.get("limit"))
    offset = int(request.POST.get("offset"))
    # Server-side permission validation.
    try:
        user_instances(request.user, db_type=["mysql"]).get(instance_name=instance_name)
    except Exception:
        result = {
            "status": 1,
            "msg": "Your group is not associated with this instance",
            "data": [],
        }
        return HttpResponse(json.dumps(result), content_type="application/json")

    # Determine whether this is RDS or another instance type.
    instance_info = Instance.objects.get(instance_name=instance_name)
    if AliyunRdsConfig.objects.filter(instance=instance_info, is_enable=True).exists():
        # Call Aliyun slow-query API.
        query_engine = get_engine(instance=instance_info)
        result = query_engine.slowquery_review_history(
            start_time, end_time, db_name, sql_id, limit, offset
        )
    else:
        search = request.POST.get("search")
        sortName = str(request.POST.get("sortName"))
        sortOrder = str(request.POST.get("sortOrder")).lower()

        # Time handling.
        end_time = datetime.datetime.strptime(
            end_time, "%Y-%m-%d"
        ) + datetime.timedelta(days=1)
        limit = offset + limit
        filter_kwargs = {}
        filter_kwargs.update({"checksum": sql_id}) if sql_id else None
        filter_kwargs.update({"db_max": db_name}) if db_name else None
        # SQLId and DBName are optional.
        # Get slow-query detail data.
        slow_sql_record_obj = SlowQueryHistory.objects.filter(
            hostname_max=(instance_info.host + ":" + str(instance_info.port)),
            ts_min__range=(start_time, end_time),
            sample__icontains=search,
            **filter_kwargs
        ).annotate(
            ExecutionStartTime=F(
                "ts_min"
            ),  # Earliest time in this 5-minute aggregation window
            DBName=F("db_max"),  # Database name
            HostAddress=Concat(
                V("'"), "user_max", V("'"), V("@"), V("'"), "client_max", V("'")
            ),  # User name
            SQLText=F("sample"),  # SQL text
            TotalExecutionCounts=F("ts_cnt"),  # Count in this aggregation window
            QueryTimePct95=F("query_time_pct_95"),  # 95th percentile query time
            QueryTimes=F("query_time_sum"),  # Total query time (seconds)
            LockTimes=F("lock_time_sum"),  # Total lock time (seconds)
            ParseRowCounts=F("rows_examined_sum"),  # Total rows examined
            ReturnRowCounts=F("rows_sent_sum"),  # Total rows returned
        )

        slow_sql_record_count = slow_sql_record_obj.count()
        slow_sql_record_list = slow_sql_record_obj.order_by(
            "-" + sortName if "desc".__eq__(sortOrder) else sortName
        )[offset:limit].values(
            "ExecutionStartTime",
            "DBName",
            "HostAddress",
            "SQLText",
            "TotalExecutionCounts",
            "QueryTimePct95",
            "QueryTimes",
            "LockTimes",
            "ParseRowCounts",
            "ReturnRowCounts",
        )

        # Serialize QuerySet.
        sql_slow_record = []
        for SlowRecord in slow_sql_record_list:
            SlowRecord["QueryTimePct95"] = round(SlowRecord["QueryTimePct95"], 6)
            SlowRecord["QueryTimes"] = round(SlowRecord["QueryTimes"], 6)
            SlowRecord["LockTimes"] = round(SlowRecord["LockTimes"], 6)
            sql_slow_record.append(SlowRecord)
        result = {"total": slow_sql_record_count, "rows": sql_slow_record}

        # Return query result.
    return HttpResponse(
        json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
        content_type="application/json",
    )


@cache_page(60 * 10)
def report(request):
    """Return slow SQL trend history."""
    checksum = request.GET.get("checksum")
    checksum = pymysql.escape_string(checksum)
    cnt_data = ChartDao().slow_query_review_history_by_cnt(checksum)
    pct_data = ChartDao().slow_query_review_history_by_pct_95_time(checksum)
    cnt_x_data = [row[1] for row in cnt_data["rows"]]
    cnt_y_data = [int(row[0]) for row in cnt_data["rows"]]
    pct_y_data = [str(row[0]) for row in pct_data["rows"]]
    line = Line(init_opts=opts.InitOpts(width="800", height="380px"))
    line.add_xaxis(cnt_x_data)
    line.add_yaxis(
        "Slow Query Count",
        cnt_y_data,
        is_smooth=True,
        markline_opts=opts.MarkLineOpts(
            data=[
                opts.MarkLineItem(type_="max", name="Max"),
                opts.MarkLineItem(type_="average", name="Average"),
            ]
        ),
    )
    line.add_yaxis(
        "Slow Query Duration (95%)",
        pct_y_data,
        is_smooth=True,
        is_symbol_show=False,
    )
    line.set_series_opts(
        areastyle_opts=opts.AreaStyleOpts(
            opacity=0.5,
        )
    )
    line.set_global_opts(
        title_opts=opts.TitleOpts(title="SQL Trend History"),
        legend_opts=opts.LegendOpts(selected_mode="single"),
        xaxis_opts=opts.AxisOpts(
            axistick_opts=opts.AxisTickOpts(is_align_with_label=True),
            is_scale=False,
            boundary_gap=False,
        ),
    )

    result = {"status": 0, "msg": "", "data": line.render_embed()}
    return HttpResponse(json.dumps(result), content_type="application/json")
