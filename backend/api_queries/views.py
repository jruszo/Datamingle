import datetime
import logging
import re
import time
import traceback
import uuid

import sqlparse
from sqlparse import tokens as sqlparse_tokens
from sqlparse.sql import Function, Identifier, IdentifierList, Parenthesis, TokenList
from django.contrib.auth.decorators import permission_required
from django.db import close_old_connections, connection, transaction
from django.db.models import Q
from django.utils.decorators import method_decorator
from common.task_queue import async_task
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiTypes
from rest_framework import generics, permissions, serializers, status, views
from rest_framework.exceptions import PermissionDenied
from rest_framework.response import Response

from common.config import SysConfig
from common.utils.const import WorkflowAction, WorkflowStatus
from common.utils.timer import FuncTimer
from sql.engines import get_engine
from sql.utils.data_masking import simple_column_mask
from sql.models import (
    Instance,
    QueryLog,
    QueryPrivileges,
    QueryPrivilegesApply,
    TeamPermissionGroup,
    Team,
)
from sql.notify import notify_for_audit
from sql.query_privileges import (
    _db_priv,
    _query_apply_audit_call_back,
    _tb_priv,
    query_priv_check,
)
from sql.utils.team import (
    teams_for_role,
    user_groups,
    user_instances,
    user_member_groups,
)
from sql.utils.team import (
    has_any_active_instance_grant,
    temp_instance_access_level,
    READ_ACCESS_LEVELS,
)
from sql.utils.tasks import add_kill_conn_schedule, del_schedule
from sql.utils.workflow_audit import AuditException, get_auditor

from api_core.pagination import CustomizedPagination
from api_core.response import success_response
from api_agents.models import AgentCommandType
from api_agents.services import (
    AgentCommandDispatchError,
    AgentCommandExecutionError,
    filter_agent_runnable_instances,
    result_set_from_agent_result,
    run_agent_command_sync,
)
from api_queries.serializers import (
    QueryDescribeResponseSerializer,
    QueryDescribeSerializer,
    QueryExecuteResponseSerializer,
    QueryExecuteSerializer,
    QueryFavoriteListSerializer,
    QueryFavoriteSerializer,
    QueryInstanceSerializer,
    QueryLogSerializer,
    QueryPrivilegesApplyCreateSerializer,
    QueryPrivilegesApplyListSerializer,
    QueryPrivilegesAuditSerializer,
    QueryPrivilegesListSerializer,
    QueryPrivilegesModifySerializer,
)

logger = logging.getLogger("default")


def _require_permission(request, permission):
    if request.user.is_superuser or request.user.has_perm(permission):
        return
    raise PermissionDenied(f"Missing required permission: {permission}")


def _require_any_permission(request, *perm_list):
    if request.user.is_superuser:
        return
    if any(request.user.has_perm(perm) for perm in perm_list):
        return
    raise PermissionDenied(
        f"Missing required permission. Need one of: {', '.join(perm_list)}"
    )


def _require_query_page_access(request):
    if has_any_active_instance_grant(request.user):
        return
    if teams_for_role(request.user, TeamPermissionGroup.QUERY).exists():
        return
    _require_any_permission(
        request, "sql.menu_query", "sql.menu_sqlquery", "sql.query_submit"
    )


def _normalize_result_set(result_set):
    result = result_set.__dict__.copy()
    result["rows"] = result_set.to_dict()
    return result


def _is_describe_ddl(result_data):
    column_list = [str(value).lower() for value in result_data.get("column_list", [])]
    return len(column_list) == 2 and "create table" in column_list[1]


def _first_meaningful_token(statement):
    return statement.token_first(skip_cm=True, skip_ws=True)


def _token_is_wildcard_projection(token, inside_function=False):
    if isinstance(token, Function):
        return False
    if token.ttype is sqlparse_tokens.Wildcard and not inside_function:
        return True
    if isinstance(token, Parenthesis):
        return any(
            _token_is_wildcard_projection(child, inside_function=inside_function)
            for child in token.tokens
        )
    if isinstance(token, TokenList):
        return any(
            _token_is_wildcard_projection(child, inside_function=inside_function)
            for child in token.tokens
        )
    return False


def _statement_has_wildcard_projection(statement):
    for token in statement.tokens:
        if token.is_whitespace or token.ttype in sqlparse_tokens.Comment:
            continue
        if token.normalized.upper() == "FROM":
            return False
        if _token_is_wildcard_projection(token):
            return True
    return False


def _identifier_table_parts(identifier):
    return (
        (identifier.get_parent_name() or "").strip("`").lower(),
        (identifier.get_real_name() or identifier.get_name() or "").strip("`").lower(),
    )


def _append_table_identifier(tables, token):
    if isinstance(token, IdentifierList):
        for identifier in token.get_identifiers():
            _append_table_identifier(tables, identifier)
    elif isinstance(token, Identifier):
        tables.append(_identifier_table_parts(token))
    elif token.ttype in sqlparse_tokens.Name or token.ttype in sqlparse_tokens.Keyword:
        tables.append(("", token.value.strip("`").lower()))


def _referenced_table_identifiers(token_list):
    tables = []
    expect_table = False
    for token in token_list.tokens:
        if token.is_whitespace or token.ttype in sqlparse_tokens.Comment:
            continue
        normalized = token.normalized.upper()
        if expect_table:
            if isinstance(token, Parenthesis):
                tables.extend(_referenced_table_identifiers(token))
            else:
                _append_table_identifier(tables, token)
            expect_table = False
            continue
        if isinstance(token, TokenList) and not isinstance(token, Function):
            tables.extend(_referenced_table_identifiers(token))
        if normalized in {"FROM", "JOIN", "DESCRIBE", "DESC"} or normalized.endswith(
            " JOIN"
        ):
            expect_table = True
    return tables


def _static_mysql_query_check(db_name, sql):
    result = {"msg": "", "bad_query": False, "filtered_sql": sql, "has_star": False}
    try:
        stripped = sqlparse.format(sql, strip_comments=True)
        statements = sqlparse.split(stripped)
        if not statements:
            raise IndexError
        sql = statements[0].strip()
        parsed = sqlparse.parse(sql)
        if not parsed:
            raise IndexError
        statement = parsed[0]
        result["filtered_sql"] = sql
    except IndexError:
        result["bad_query"] = True
        result["msg"] = "No valid SQL statement"
        return result

    first_token = _first_meaningful_token(statement)
    first_keyword = first_token.normalized.upper() if first_token else ""
    if first_keyword not in {"SELECT", "SHOW", "EXPLAIN", "DESCRIBE", "DESC"}:
        result["bad_query"] = True
        result["msg"] = "Unsupported query syntax type!"
    if _statement_has_wildcard_projection(statement):
        result["has_star"] = True
        result["msg"] = "SQL contains * "

    db_name = (db_name or "").strip("`").lower()
    references_mysql_user = any(
        (schema == "mysql" and table == "user")
        or (db_name == "mysql" and not schema and table == "user")
        for schema, table in _referenced_table_identifiers(statement)
    )
    if references_mysql_user:
        result["bad_query"] = True
        result["msg"] = "You do not have permission to view this table"
    return result


def _mysql_quote_identifier(value):
    value = str(value or "").strip()
    if not value or "\x00" in value:
        raise serializers.ValidationError({"errors": "Invalid MySQL identifier."})
    return f"`{value.replace('`', '``')}`"


class QueryExecute(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="Execute Online Query",
        request=QueryExecuteSerializer,
        responses={200: QueryExecuteResponseSerializer},
        description="Execute SQL query on an instance and return result rows.",
    )
    def post(self, request):
        serializer = QueryExecuteSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data
        user = request.user

        instance_name = data["instance_name"]
        sql_content = data["sql_content"]
        db_name = data["db_name"]
        tb_name = data.get("tb_name") or None
        schema_name = data.get("schema_name") or None
        limit_num = data["limit_num"]

        try:
            instance = user_instances(user).get(instance_name=instance_name)
        except Instance.DoesNotExist:
            raise serializers.ValidationError(
                {"errors": "Your group is not associated with this instance."}
            )

        if not (
            user.is_superuser
            or user.has_perm("sql.query_submit")
            or user_instances(user, tag_codes=["can_read"])
            .filter(pk=instance.pk)
            .exists()
            or temp_instance_access_level(user, instance) in READ_ACCESS_LEVELS
        ):
            raise PermissionDenied("You do not have permission to query this instance.")

        config = SysConfig()
        priv_check = True
        error_message = None
        seconds_behind_master = ""

        try:
            query_engine = get_engine(instance=instance)
            if instance.db_type == "mysql":
                query_check_info = _static_mysql_query_check(db_name, sql_content)
            else:
                query_check_info = {
                    "bad_query": False,
                    "filtered_sql": sql_content,
                    "has_star": False,
                }
            if query_check_info.get("bad_query"):
                raise serializers.ValidationError(
                    {"errors": query_check_info.get("msg", "Blocked SQL statement.")}
                )
            if query_check_info.get("has_star") and config.get("disable_star") is True:
                raise serializers.ValidationError(
                    {"errors": query_check_info.get("msg", "SELECT * is not allowed.")}
                )
            sql_content = query_check_info["filtered_sql"]

            priv_check_info = query_priv_check(
                user, instance, db_name, sql_content, limit_num
            )
            if priv_check_info["status"] != 0:
                raise serializers.ValidationError({"errors": priv_check_info["msg"]})

            limit_num = priv_check_info["data"]["limit_num"]
            priv_check = priv_check_info["data"]["priv_check"]

            if re.match(r"^explain", sql_content.lower()):
                limit_num = 0

            sql_content = query_engine.filter_sql(sql=sql_content, limit_num=limit_num)
            max_execution_time = int(config.get("max_execution_time", 60))
            command = run_agent_command_sync(
                instance=instance,
                command_type=AgentCommandType.QUERY_EXECUTE,
                workflow_type="query",
                workflow_id=f"{user.username}:{uuid.uuid4().hex}",
                payload={
                    "db_name": db_name,
                    "schema_name": schema_name,
                    "tb_name": tb_name,
                    "sql": sql_content,
                    "limit": limit_num,
                    "max_execution_time_ms": max_execution_time * 1000,
                    "submitted_by": user.username,
                },
                timeout_seconds=max_execution_time,
            )
            query_result = result_set_from_agent_result(sql_content, command.result)
            seconds_behind_master = command.result.get("seconds_behind_master", "")

            if config.get("data_masking"):
                try:
                    started_at = time.monotonic()
                    masking_result = simple_column_mask(instance, query_result)
                    masking_result.mask_time = round(time.monotonic() - started_at, 3)
                    if masking_result.error:
                        if config.get("query_check"):
                            error_message = (
                                f"Data masking error: {masking_result.error}"
                            )
                        else:
                            logger.warning(
                                "Data masking error, allowed by config. SQL: %s error: %s",
                                sql_content,
                                masking_result.error,
                            )
                            query_result.error = None
                            result_data = query_result.__dict__.copy()
                            result_data["rows"] = query_result.to_dict()
                    else:
                        result_data = masking_result.__dict__.copy()
                        result_data["rows"] = masking_result.to_dict()
                except Exception as exc:
                    logger.exception("Data masking failed")
                    if config.get("query_check"):
                        error_message = "Data masking error, contact admin."
                    else:
                        logger.warning(
                            "Data masking error, allowed by config. SQL: %s error: %s",
                            sql_content,
                            str(exc),
                        )
                        query_result.error = None
                        result_data = query_result.__dict__.copy()
                        result_data["rows"] = query_result.to_dict()
            else:
                result_data = query_result.__dict__.copy()
                result_data["rows"] = query_result.to_dict()
        except serializers.ValidationError:
            raise
        except (AgentCommandDispatchError, AgentCommandExecutionError) as exc:
            logger.warning("Agent query execution failed: %s", exc)
            raise serializers.ValidationError({"errors": str(exc)})
        except Exception:
            logger.exception("Query error while executing user SQL")
            raise serializers.ValidationError({"errors": "Query error, contact admin."})

        if error_message:
            effect_rows = 0
        else:
            result_data["seconds_behind_master"] = seconds_behind_master
            if int(limit_num) == 0:
                effect_rows = int(query_result.affected_rows)
            else:
                effect_rows = min(int(limit_num), int(query_result.affected_rows))

        QueryLog.objects.create(
            username=user.username,
            user_display=user.display,
            db_name=db_name,
            instance_name=instance.instance_name,
            sqllog=sql_content,
            effect_row=effect_rows,
            cost_time=query_result.query_time,
            priv_check=priv_check,
            hit_rule=query_result.mask_rule_hit,
            masking=query_result.is_masked,
        )
        if error_message:
            raise serializers.ValidationError({"errors": error_message})

        return success_response(data=result_data)


class QueryInstanceList(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="Queryable Instances",
        responses={200: QueryInstanceSerializer(many=True)},
        parameters=[
            OpenApiParameter(
                name="type",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY,
                description="Optional instance type filter.",
            ),
            OpenApiParameter(
                name="db_type",
                type={"type": "array", "items": {"type": "string"}},
                location=OpenApiParameter.QUERY,
                description="Optional database-type filter.",
            ),
        ],
        description="List instances the current user can query, filtered to can_read resources.",
    )
    def get(self, request):
        _require_query_page_access(request)

        instance_type = request.query_params.get("type")
        db_type = request.query_params.getlist("db_type")
        if not db_type:
            db_type = request.query_params.getlist("db_type[]")

        queryset = user_instances(
            request.user,
            type=instance_type,
            db_type=db_type or None,
            tag_codes=["can_read"],
        )
        queryset = filter_agent_runnable_instances(queryset).order_by("instance_name")
        serializer = QueryInstanceSerializer(queryset, many=True)
        return success_response(data=serializer.data)


class QueryDescribe(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="Describe Table",
        request=QueryDescribeSerializer,
        responses={200: QueryDescribeResponseSerializer},
        description="Get normalized table-structure data for the selected instance and table.",
    )
    def post(self, request):
        _require_query_page_access(request)

        serializer = QueryDescribeSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data

        try:
            instance = user_instances(request.user, tag_codes=["can_read"]).get(
                pk=data["instance_id"]
            )
        except Instance.DoesNotExist:
            raise serializers.ValidationError(
                {"errors": "The instance is not associated with your group."}
            )

        try:
            db_name = data["db_name"]
            schema_name = data.get("schema_name", "")
            tb_name = data["tb_name"]
            command = run_agent_command_sync(
                instance=instance,
                command_type=AgentCommandType.QUERY_EXECUTE,
                workflow_type="query.describe",
                workflow_id=f"{request.user.username}:{uuid.uuid4().hex}",
                payload={
                    "db_name": db_name,
                    "schema_name": schema_name,
                    "tb_name": tb_name,
                    "sql": f"show create table {_mysql_quote_identifier(tb_name)};",
                    "limit": 1,
                    "max_execution_time_ms": int(
                        SysConfig().get("max_execution_time", 60)
                    )
                    * 1000,
                    "submitted_by": request.user.username,
                },
                timeout_seconds=int(SysConfig().get("max_execution_time", 60)),
            )
            query_result = result_set_from_agent_result(
                command.payload["sql"], command.result
            )
        except serializers.ValidationError:
            raise
        except (AgentCommandDispatchError, AgentCommandExecutionError) as exc:
            raise serializers.ValidationError({"errors": str(exc)})
        except Exception:
            raise serializers.ValidationError({"errors": "Operation failed."})

        if query_result.error:
            raise serializers.ValidationError({"errors": query_result.error})

        result_data = _normalize_result_set(query_result)
        result_data["display_mode"] = (
            "ddl" if _is_describe_ddl(result_data) else "table"
        )
        return success_response(data=result_data)


class QueryLogBase(generics.ListAPIView):
    permission_classes = [permissions.IsAuthenticated]
    pagination_class = CustomizedPagination
    serializer_class = QueryLogSerializer
    queryset = QueryLog.objects.all().order_by("-id")

    audit_only = False

    def _check_permission(self, request):
        if self.audit_only:
            _require_permission(request, "sql.audit_user")
        else:
            _require_permission(request, "sql.menu_sqlquery")

    def get_queryset(self):
        user = self.request.user
        query_log_id = self.request.query_params.get("query_log_id")
        search = self.request.query_params.get("search", "")
        start_date = self.request.query_params.get("start_date", "")
        end_date = self.request.query_params.get("end_date", "")
        star = self.request.query_params.get("star")

        queryset = self.queryset
        if not self.audit_only:
            queryset = queryset.filter(username=user.username)
        if star == "true":
            queryset = queryset.filter(favorite=True)
        elif star == "false":
            queryset = queryset.filter(favorite=False)
        if query_log_id:
            queryset = queryset.filter(id=query_log_id)
        if self.audit_only and not (
            user.is_superuser or user.has_perm("sql.audit_user")
        ):
            queryset = queryset.filter(username=user.username)
        if start_date and end_date:
            try:
                end_date_obj = datetime.datetime.strptime(
                    end_date, "%Y-%m-%d"
                ) + datetime.timedelta(days=1)
            except ValueError as exc:
                raise serializers.ValidationError(
                    {"errors": "Invalid end_date format."}
                )
            queryset = queryset.filter(create_time__range=(start_date, end_date_obj))
        if search:
            queryset = queryset.filter(
                Q(sqllog__icontains=search)
                | Q(user_display__icontains=search)
                | Q(alias__icontains=search)
                | Q(db_name__icontains=search)
                | Q(instance_name__icontains=search)
            )
        return queryset

    @extend_schema(
        summary="Query Logs",
        responses={200: QueryLogSerializer},
        parameters=[
            OpenApiParameter(
                name="query_log_id",
                type=OpenApiTypes.INT,
                location=OpenApiParameter.QUERY,
                description="Query log ID.",
            ),
            OpenApiParameter(
                name="search",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY,
                description="Search SQL/user/alias.",
            ),
            OpenApiParameter(
                name="star",
                type=OpenApiTypes.BOOL,
                location=OpenApiParameter.QUERY,
                description="Only favorites if true.",
            ),
            OpenApiParameter(
                name="start_date",
                type=OpenApiTypes.DATE,
                location=OpenApiParameter.QUERY,
                description="Start date (YYYY-MM-DD).",
            ),
            OpenApiParameter(
                name="end_date",
                type=OpenApiTypes.DATE,
                location=OpenApiParameter.QUERY,
                description="End date (YYYY-MM-DD).",
            ),
            OpenApiParameter(
                name="page",
                type=OpenApiTypes.INT,
                location=OpenApiParameter.QUERY,
            ),
            OpenApiParameter(
                name="size",
                type=OpenApiTypes.INT,
                location=OpenApiParameter.QUERY,
            ),
        ],
        description="List online query logs.",
    )
    def get(self, request):
        self._check_permission(request)
        queryset = self.get_queryset()
        page_obj = self.paginate_queryset(queryset=queryset)
        serializer = self.get_serializer(page_obj, many=True)
        return self.get_paginated_response(serializer.data)


class QueryLogList(QueryLogBase):
    audit_only = False


class QueryLogAuditList(QueryLogBase):
    audit_only = True


class QueryFavorite(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="Favorite Query Logs",
        responses={200: QueryFavoriteListSerializer(many=True)},
        description="List the current user's favorite query logs for common-query selection.",
    )
    @method_decorator(permission_required("sql.menu_sqlquery", raise_exception=True))
    def get(self, request):
        queryset = QueryLog.objects.filter(
            username=request.user.username, favorite=True
        ).order_by("-id")
        serializer = QueryFavoriteListSerializer(queryset, many=True)
        return success_response(data=serializer.data)

    @extend_schema(
        summary="Favorite Query Log",
        request=QueryFavoriteSerializer,
        description="Favorite/unfavorite a query log and update alias.",
    )
    @method_decorator(permission_required("sql.menu_sqlquery", raise_exception=True))
    def post(self, request):
        serializer = QueryFavoriteSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data

        try:
            query_log = QueryLog.objects.get(
                id=data["query_log_id"], username=request.user.username
            )
        except QueryLog.DoesNotExist:
            raise serializers.ValidationError({"errors": "Query log does not exist."})

        query_log.favorite = data["star"]
        query_log.alias = data["alias"]
        query_log.save(update_fields=["favorite", "alias"])
        return success_response()


class QueryPrivilegesApplyListCreate(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    pagination_class = CustomizedPagination

    @extend_schema(
        summary="List Query Privilege Applications",
        responses={200: QueryPrivilegesApplyListSerializer},
        parameters=[
            OpenApiParameter(
                name="search",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY,
                description="Search title/user display.",
            ),
            OpenApiParameter(
                name="page",
                type=OpenApiTypes.INT,
                location=OpenApiParameter.QUERY,
            ),
            OpenApiParameter(
                name="size",
                type=OpenApiTypes.INT,
                location=OpenApiParameter.QUERY,
            ),
        ],
        description="List query privilege applications.",
    )
    def get(self, request):
        _require_permission(request, "sql.menu_queryapplylist")

        user = request.user
        search = request.query_params.get("search", "")
        queryset = QueryPrivilegesApply.objects.all()
        if search:
            queryset = queryset.filter(
                Q(title__icontains=search) | Q(user_display__icontains=search)
            )
        if user.is_superuser:
            pass
        elif user.has_perm("sql.query_review"):
            group_ids = [group.team_id for group in user_member_groups(user)]
            queryset = queryset.filter(team_id__in=group_ids)
        else:
            queryset = queryset.filter(user_name=user.username)
        queryset = queryset.order_by("-apply_id")

        paginator = self.pagination_class()
        page_obj = paginator.paginate_queryset(queryset, request, view=self)
        serializer = QueryPrivilegesApplyListSerializer(page_obj, many=True)
        return paginator.get_paginated_response(serializer.data)

    @extend_schema(
        summary="Create Query Privilege Application",
        request=QueryPrivilegesApplyCreateSerializer,
        description="Submit a query privilege application workflow.",
    )
    def post(self, request):
        _require_permission(request, "sql.query_applypriv")
        serializer = QueryPrivilegesApplyCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data
        user = request.user

        try:
            instance = user_instances(user, tag_codes=["can_read"]).get(
                instance_name=data["instance_name"]
            )
        except Instance.DoesNotExist:
            raise serializers.ValidationError(
                {"errors": "Your group is not associated with this instance!"}
            )

        try:
            group = Team.objects.get(team_name=data["team_name"])
        except Team.DoesNotExist:
            raise serializers.ValidationError({"errors": "Team does not exist."})

        priv_type = data["priv_type"]
        if priv_type == 1:
            for db_name in data["db_list"]:
                if _db_priv(user, instance, db_name):
                    raise serializers.ValidationError(
                        {
                            "errors": (
                                f"You already have database permission for {db_name} "
                                f"on instance {data['instance_name']}; duplicate request is not allowed."
                            )
                        }
                    )
        else:
            db_name = data["db_name"]
            if _db_priv(user, instance, db_name):
                raise serializers.ValidationError(
                    {
                        "errors": (
                            f"You already have full database permission for {db_name} "
                            f"on instance {data['instance_name']}; duplicate request is not allowed."
                        )
                    }
                )
            for tb_name in data["table_list"]:
                if _tb_priv(user, instance, db_name, tb_name):
                    raise serializers.ValidationError(
                        {
                            "errors": (
                                f"You already have query permission on {db_name}.{tb_name} "
                                f"for instance {data['instance_name']}; duplicate request is not allowed."
                            )
                        }
                    )

        apply_info = QueryPrivilegesApply(
            title=data["title"],
            team_id=group.team_id,
            team_name=group.team_name,
            audit_auth_groups="",
            user_name=user.username,
            user_display=user.display,
            instance=instance,
            valid_date=data["valid_date"],
            status=WorkflowStatus.WAITING,
            limit_num=data["limit_num"],
            priv_type=priv_type,
        )
        if priv_type == 1:
            apply_info.db_list = ",".join(data["db_list"])
            apply_info.table_list = ""
        else:
            apply_info.db_list = data["db_name"]
            apply_info.table_list = ",".join(data["table_list"])

        audit_handler = get_auditor(workflow=apply_info)
        try:
            with transaction.atomic():
                audit_handler.create_audit()
        except AuditException:
            logger.error("Failed to create query privilege audit flow.")
            raise serializers.ValidationError(
                {"errors": "Failed to create approval flow, please contact admin."}
            )

        _query_apply_audit_call_back(
            audit_handler.workflow.apply_id, audit_handler.audit.current_status
        )
        async_task(
            notify_for_audit,
            workflow_audit=audit_handler.audit,
            timeout=60,
            task_name=f"query-priv-apply-{audit_handler.workflow.apply_id}",
        )
        return success_response(
            data={"apply_id": audit_handler.workflow.apply_id},
            status_code=status.HTTP_201_CREATED,
        )


class QueryPrivilegesList(generics.ListAPIView):
    permission_classes = [permissions.IsAuthenticated]

    pagination_class = CustomizedPagination
    serializer_class = QueryPrivilegesListSerializer
    queryset = QueryPrivileges.objects.all()

    @extend_schema(
        summary="List User Query Privileges",
        responses={200: QueryPrivilegesListSerializer},
        parameters=[
            OpenApiParameter(
                name="user_display",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY,
                description="Filter by user display name, or 'all'.",
            ),
            OpenApiParameter(
                name="search",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY,
                description="Search user/db/table.",
            ),
            OpenApiParameter(
                name="page",
                type=OpenApiTypes.INT,
                location=OpenApiParameter.QUERY,
            ),
            OpenApiParameter(
                name="size",
                type=OpenApiTypes.INT,
                location=OpenApiParameter.QUERY,
            ),
        ],
        description="List current query privileges.",
    )
    def get(self, request):
        _require_permission(request, "sql.menu_queryapplylist")
        user = request.user
        user_display = request.query_params.get("user_display", "all")
        search = request.query_params.get("search", "")

        queryset = self.queryset.filter(
            is_deleted=0, valid_date__gte=datetime.datetime.now()
        )
        if search:
            queryset = queryset.filter(
                Q(user_display__icontains=search)
                | Q(db_name__icontains=search)
                | Q(table_name__icontains=search)
            )
        if user_display != "all":
            queryset = queryset.filter(user_display=user_display)

        if user.is_superuser:
            pass
        elif user.has_perm("sql.query_mgtpriv"):
            group_ids = [group.team_id for group in user_member_groups(user)]
            queryset = queryset.filter(
                instance__queryprivilegesapply__team_id__in=group_ids
            )
        else:
            queryset = queryset.filter(user_name=user.username)

        queryset = queryset.distinct().order_by("-privilege_id")
        page_obj = self.paginate_queryset(queryset=queryset)
        serializer = self.get_serializer(page_obj, many=True)
        return self.get_paginated_response(serializer.data)


class QueryPrivilegeDetail(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="Update Query Privilege",
        request=QueryPrivilegesModifySerializer,
        description="Update a query privilege record.",
    )
    def patch(self, request, privilege_id):
        _require_permission(request, "sql.query_mgtpriv")
        payload = request.data.copy()
        payload["privilege_id"] = privilege_id
        payload["type"] = 2
        serializer = QueryPrivilegesModifySerializer(data=payload)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data

        try:
            privilege = QueryPrivileges.objects.get(privilege_id=data["privilege_id"])
        except QueryPrivileges.DoesNotExist:
            raise serializers.ValidationError(
                {"errors": "Target privilege does not exist."}
            )

        privilege.valid_date = data["valid_date"]
        privilege.limit_num = data["limit_num"]
        privilege.save(update_fields=["valid_date", "limit_num"])
        return success_response()

    @extend_schema(
        summary="Delete Query Privilege",
        description="Soft-delete a query privilege record.",
    )
    def delete(self, request, privilege_id):
        _require_permission(request, "sql.query_mgtpriv")
        try:
            privilege = QueryPrivileges.objects.get(privilege_id=privilege_id)
        except QueryPrivileges.DoesNotExist:
            raise serializers.ValidationError(
                {"errors": "Target privilege does not exist."}
            )

        privilege.is_deleted = 1
        privilege.save(update_fields=["is_deleted"])
        return success_response()


class QueryPrivilegeApplicationReviewCreate(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="Create Query Privilege Review",
        request=QueryPrivilegesAuditSerializer,
        description="Create a review decision for a query privilege application.",
    )
    def post(self, request, apply_id):
        _require_permission(request, "sql.query_review")
        payload = request.data.copy()
        payload["apply_id"] = apply_id
        serializer = QueryPrivilegesAuditSerializer(data=payload)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data

        try:
            audit_status = WorkflowAction(int(data["audit_status"]))
        except ValueError as exc:
            raise serializers.ValidationError(
                {"errors": "Invalid audit_status parameter."}
            )

        try:
            query_apply = QueryPrivilegesApply.objects.get(apply_id=data["apply_id"])
        except QueryPrivilegesApply.DoesNotExist:
            raise serializers.ValidationError({"errors": "Workflow does not exist."})

        auditor = get_auditor(workflow=query_apply)
        with transaction.atomic():
            try:
                workflow_audit_detail = auditor.operate(
                    audit_status, request.user, data.get("audit_remark", "")
                )
            except AuditException as exc:
                raise serializers.ValidationError({"errors": "Audit failed."})
            _query_apply_audit_call_back(
                auditor.audit.workflow_id, auditor.audit.current_status
            )

        async_task(
            notify_for_audit,
            workflow_audit=auditor.audit,
            workflow_audit_detail=workflow_audit_detail,
            timeout=60,
            task_name=f"query-priv-audit-{data['apply_id']}",
        )
        return success_response()
