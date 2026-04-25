import logging
import os
import re
from urllib.parse import quote

from django.conf import settings
from django.contrib.auth.decorators import permission_required
from django.contrib.auth.password_validation import validate_password
from django.core.exceptions import ValidationError as DjangoValidationError
from django.db.models import Q
from django.http import FileResponse, Http404
from django.template import loader
from django.utils import timezone
from django.utils.decorators import method_decorator
from django_redis import get_redis_connection
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiTypes
from rest_framework import generics, serializers, status, views
from rest_framework.exceptions import PermissionDenied
from rest_framework.response import Response

from sql.engines import ResultSet, engine_map, get_engine
from sql.models import (
    Instance,
    InstanceAccount,
    InstanceDatabase,
    InstanceTag,
    ParamHistory,
    ParamTemplate,
    ResourceGroup,
    Users,
)
from sql.utils.instance_management import (
    SUPPORTED_MANAGEMENT_DB_TYPE,
    get_instanceaccount_unique_key,
    get_instanceaccount_unique_value,
)
from sql.utils.resource_group import user_instances
from sql.utils.sql_utils import filter_db_list

from api_core.pagination import CustomizedPagination
from api_core.response import success_response
from api_instances.serializers import (
    DataDictionaryDatabaseListSerializer,
    DataDictionaryInstanceSerializer,
    DataDictionaryTableDetailSerializer,
    DataDictionaryTableGroupListSerializer,
    InstanceAccountDeleteSerializer,
    InstanceAccountGrantSerializer,
    InstanceAccountListSerializer,
    InstanceAccountLockSerializer,
    InstanceAccountPasswordSerializer,
    InstanceAccountPayloadSerializer,
    InstanceAccountRecordSerializer,
    InstanceDatabaseListSerializer,
    InstanceDatabaseMetadataSerializer,
    InstanceDatabasePayloadSerializer,
    InstanceConnectionTestResultSerializer,
    InstanceConnectionTestRequestSerializer,
    InstanceCreateSerializer,
    InstanceDetailSerializer,
    InstanceDiagnosticKillPreviewSerializer,
    InstanceDiagnosticKillResultSerializer,
    InstanceDiagnosticListSerializer,
    InstanceEditorSerializer,
    InstanceListSerializer,
    InstanceMetadataSerializer,
    InstanceParamEditSerializer,
    InstanceParamHistoryListSerializer,
    InstanceParamListSerializer,
    InstanceResourceListSerializer,
    InstanceResourceSerializer,
    InstanceTagCreateSerializer,
    InstanceTagManagementSerializer,
    InstanceTagUpdateSerializer,
)

logger = logging.getLogger("default")

DATA_DICTIONARY_DB_TYPES = ["mysql", "mssql", "oracle"]
INSTANCE_OPERATION_DB_TYPES = ["mysql", "mongo"]
INSTANCE_PARAMETER_DB_TYPES = ["mysql", "goinception"]
ALLOWED_PROCESSLIST_PARAMS = set()

MYSQL_ACCOUNT_RE = re.compile(r"^`((?:``|[^`])*)`@`((?:``|[^`])*)`$")
MYSQL_ALLOWED_PRIVILEGES = {
    "ALTER",
    "ALTER ROUTINE",
    "ALL",
    "ALL PRIVILEGES",
    "CREATE",
    "CREATE ROUTINE",
    "CREATE TABLESPACE",
    "CREATE TEMPORARY TABLES",
    "CREATE USER",
    "CREATE VIEW",
    "DELETE",
    "DROP",
    "EVENT",
    "EXECUTE",
    "FILE",
    "GRANT OPTION",
    "INDEX",
    "INSERT",
    "LOCK TABLES",
    "PROCESS",
    "REFERENCES",
    "RELOAD",
    "REPLICATION CLIENT",
    "REPLICATION SLAVE",
    "SELECT",
    "SHOW DATABASES",
    "SHOW VIEW",
    "SHUTDOWN",
    "SUPER",
    "TRIGGER",
    "UPDATE",
    "USAGE",
}


def _require_any_permission(request, *perm_list):
    if request.user.is_superuser:
        return
    if any(request.user.has_perm(perm) for perm in perm_list):
        return
    raise PermissionDenied(
        f"Missing required permission. Need one of: {', '.join(perm_list)}"
    )


def _require_permission(request, permission):
    if request.user.is_superuser or request.user.has_perm(permission):
        return
    raise PermissionDenied(f"Missing required permission: {permission}")


def _data_dictionary_queryset(user):
    return user_instances(user, db_type=DATA_DICTIONARY_DB_TYPES).order_by(
        "instance_name", "id"
    )


def _data_dictionary_instance(user, instance_id):
    try:
        return _data_dictionary_queryset(user).get(pk=instance_id)
    except Instance.DoesNotExist as exc:
        raise serializers.ValidationError(
            {"errors": "The instance is not associated with your group."}
        ) from exc


def _safe_dictionary_export_path(base_dir, instance_name, db_name):
    base_dir = os.path.normpath(base_dir)
    full_path = os.path.normpath(
        os.path.join(base_dir, f"{instance_name}_{db_name}.html")
    )
    if os.path.commonpath([base_dir, full_path]) != base_dir:
        return ""
    return full_path


def _safe_positive_int(value, default, maximum):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return min(max(parsed, 1), maximum)


def _required_int(value, field_name):
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise serializers.ValidationError(
            {field_name: "A valid integer is required."}
        ) from exc


def _mysql_quote_identifier(value):
    value = str(value or "").strip()
    if not value or "\x00" in value:
        raise serializers.ValidationError({"errors": "Invalid MySQL identifier."})
    return f"`{value.replace('`', '``')}`"


def _mysql_account_identifier(user, host):
    return f"{_mysql_quote_identifier(user)}@{_mysql_quote_identifier(host)}"


def _parse_mysql_account_identifier(user_host):
    value = str(user_host or "").strip()
    match = MYSQL_ACCOUNT_RE.match(value)
    if match:
        user, host = match.groups()
        return _mysql_account_identifier(
            user.replace("``", "`"), host.replace("``", "`")
        )
    if "@" in value and "`" not in value:
        user, host = value.split("@", 1)
        return _mysql_account_identifier(user, host)
    raise serializers.ValidationError(
        {"user_host": "Expected MySQL account in `user`@`host` format."}
    )


def _mysql_privilege_token(privilege):
    normalized = str(privilege or "").strip().upper().replace("_", " ")
    if normalized == "GRANT":
        normalized = "GRANT OPTION"
    if normalized not in MYSQL_ALLOWED_PRIVILEGES:
        raise serializers.ValidationError(
            {"privs": f"Unsupported MySQL privilege: {privilege}"}
        )
    return normalized


def _table_groups_to_list(grouped_tables):
    return [
        {"group": group, "tables": tables}
        for group, tables in sorted(grouped_tables.items(), key=lambda item: item[0])
    ]


def _operation_database_instance(user, instance_id):
    try:
        return _operation_database_queryset(user).get(pk=instance_id)
    except Instance.DoesNotExist as exc:
        raise serializers.ValidationError(
            {"errors": "The instance is not associated with your group."}
        ) from exc


def _operation_database_queryset(user):
    return user_instances(user, db_type=INSTANCE_OPERATION_DB_TYPES).order_by(
        "instance_name", "id"
    )


def _operation_account_queryset(user):
    return user_instances(user, db_type=SUPPORTED_MANAGEMENT_DB_TYPE).order_by(
        "instance_name", "id"
    )


def _operation_account_instance(user, instance_id, db_type=None):
    try:
        queryset = _operation_account_queryset(user)
        if db_type:
            queryset = queryset.filter(db_type__in=db_type)
        return queryset.get(pk=instance_id)
    except Instance.DoesNotExist as exc:
        raise serializers.ValidationError(
            {"errors": "The instance is not associated with your group."}
        ) from exc


def _operation_param_queryset(user):
    return user_instances(user, db_type=INSTANCE_PARAMETER_DB_TYPES).order_by(
        "instance_name", "id"
    )


def _operation_param_instance(user, instance_id):
    try:
        return _operation_param_queryset(user).get(pk=instance_id)
    except Instance.DoesNotExist as exc:
        raise serializers.ValidationError(
            {"errors": "The instance is not associated with your group."}
        ) from exc


def _operation_diagnostic_queryset(user):
    return user_instances(user).order_by("instance_name", "id")


def _operation_diagnostic_instance(user, instance_id):
    try:
        return _operation_diagnostic_queryset(user).get(pk=instance_id)
    except Instance.DoesNotExist as exc:
        raise serializers.ValidationError(
            {"errors": "The instance is not associated with your group."}
        ) from exc


def _owner_display(owner):
    if not owner:
        return ""
    try:
        return Users.objects.get(username=owner).display
    except Users.DoesNotExist as exc:
        raise serializers.ValidationError({"owner": "Owner does not exist."}) from exc


def _clear_instance_resource_cache():
    try:
        redis_connection = get_redis_connection("default")
        for key in redis_connection.scan_iter(match="*insRes*", count=2000):
            redis_connection.delete(key)
    except Exception:
        logger.warning("Failed to clear instance resource cache", exc_info=True)


def _validate_account_password(password):
    try:
        validate_password(password, user=None, password_validators=None)
    except DjangoValidationError as exc:
        raise serializers.ValidationError({"password": list(exc.messages)})


def _mysql_user_host(user, host, user_host=""):
    if user_host:
        return _parse_mysql_account_identifier(user_host)
    return _mysql_account_identifier(user, host)


def _mongo_db_name_user(db_name, user, db_name_user=""):
    if db_name_user:
        return db_name_user
    return f"{db_name}.{user}"


def _validate_account_identity(instance, data, require_password=False):
    password = data.get("password", "")
    if instance.db_type == "mysql":
        if not all([data.get("user"), data.get("host")]):
            raise serializers.ValidationError(
                {"errors": "MySQL account operations require user and host."}
            )
    elif instance.db_type == "mongo":
        if not all([data.get("db_name"), data.get("user")]):
            raise serializers.ValidationError(
                {"errors": "Mongo account operations require database and user."}
            )
    else:
        raise serializers.ValidationError(
            {"errors": f"Unsupported instance type: {instance.db_type}"}
        )

    if require_password and not password:
        raise serializers.ValidationError({"password": "This field is required."})
    if password:
        _validate_account_password(password)


def _account_metadata_fields(instance, data):
    user = data.get("user", "")
    host = data.get("host", "")
    db_name = data.get("db_name", "")
    if instance.db_type == "mysql" and not host:
        user_host = data.get("user_host", "")
        if user_host and "@`" in user_host:
            try:
                host = user_host.rsplit("@`", 1)[1].rstrip("`")
            except IndexError:
                host = ""
    elif instance.db_type == "mongo" and not db_name:
        db_name_user = data.get("db_name_user", "")
        if "." in db_name_user:
            db_name, user = db_name_user.split(".", 1)
    return user, host, db_name


def _account_result(result, success_detail, data=None, status_code=status.HTTP_200_OK):
    if result.error:
        raise serializers.ValidationError({"errors": result.error})
    return success_response(
        data=data or {},
        detail=success_detail,
        status_code=status_code,
    )


def _grant_option_name(privilege):
    return "GRANT OPTION" if privilege == "GRANT" else privilege


def _privilege_list(privs, key):
    values = []
    if isinstance(privs, dict):
        values = privs.get(key) or privs.get("privileges") or []
    elif isinstance(privs, list):
        values = privs
    return [_mysql_privilege_token(_grant_option_name(item)) for item in values if item]


def _mysql_account_grant_sql(data, escaped_user_host):
    op_type = _required_int(data.get("op_type", 0), "op_type")
    priv_type = _required_int(data.get("priv_type", 0), "priv_type")
    privs = data.get("privs") or {}
    verb = "GRANT" if op_type == 0 else "REVOKE"
    direction = "TO" if op_type == 0 else "FROM"
    statements = []

    if priv_type == 0:
        global_privs = _privilege_list(privs, "global_privs")
        if not global_privs:
            raise serializers.ValidationError(
                {"errors": "Global privileges are required."}
            )
        statements.append(
            f"{verb} {','.join(global_privs)} ON *.* {direction} {escaped_user_host};"
        )
    elif priv_type == 1:
        db_privs = _privilege_list(privs, "db_privs")
        db_names = data.get("db_names") or [data.get("db_name")]
        db_names = [db_name for db_name in db_names if db_name]
        if not db_privs or not db_names:
            raise serializers.ValidationError(
                {"errors": "Database privileges and databases are required."}
            )
        statements.extend(
            f"{verb} {','.join(db_privs)} ON {_mysql_quote_identifier(db_name)}.* {direction} {escaped_user_host};"
            for db_name in db_names
        )
    elif priv_type == 2:
        table_privs = _privilege_list(privs, "tb_privs")
        db_name = data.get("db_name")
        table_names = data.get("tb_names") or [data.get("tb_name")]
        table_names = [table_name for table_name in table_names if table_name]
        if not table_privs or not db_name or not table_names:
            raise serializers.ValidationError(
                {"errors": "Table privileges, database, and tables are required."}
            )
        statements.extend(
            f"{verb} {','.join(table_privs)} ON {_mysql_quote_identifier(db_name)}.{_mysql_quote_identifier(table_name)} {direction} {escaped_user_host};"
            for table_name in table_names
        )
    elif priv_type == 3:
        column_privs = _privilege_list(privs, "col_privs")
        db_name = data.get("db_name")
        table_name = data.get("tb_name")
        column_names = data.get("col_names") or []
        if not column_privs or not db_name or not table_name or not column_names:
            raise serializers.ValidationError(
                {
                    "errors": "Column privileges, database, table, and columns are required."
                }
            )
        columns = ",".join(_mysql_quote_identifier(column) for column in column_names)
        statements.extend(
            f"{verb} {privilege}({columns}) ON {_mysql_quote_identifier(db_name)}.{_mysql_quote_identifier(table_name)} {direction} {escaped_user_host};"
            for privilege in column_privs
        )
    else:
        raise serializers.ValidationError({"priv_type": "Unsupported privilege scope."})

    return "".join(statements)


class InstanceList(generics.ListAPIView):
    """
    List all instances or create a new instance configuration.
    """

    pagination_class = CustomizedPagination
    serializer_class = InstanceListSerializer
    queryset = Instance.objects.all().order_by("id")

    def get_queryset(self):
        queryset = (
            super().get_queryset().prefetch_related("instance_tag", "resource_group")
        )
        search = self.request.query_params.get("search", "").strip()
        instance_type = self.request.query_params.get("type", "").strip()
        db_type = self.request.query_params.get("db_type", "").strip()
        ordering = self.request.query_params.get("ordering", "").strip()

        raw_tags = self.request.query_params.getlist("tags")
        if not raw_tags:
            raw_tags = self.request.query_params.getlist("tags[]")
        if not raw_tags:
            raw_tags = self.request.query_params.get("tags", "").split(",")
        tag_ids = [tag.strip() for tag in raw_tags if str(tag).strip()]

        if search:
            search_filter = (
                Q(instance_name__icontains=search)
                | Q(host__icontains=search)
                | Q(user__icontains=search)
            )
            if search.isdigit():
                search_filter |= Q(id=int(search))
            queryset = queryset.filter(search_filter)

        if instance_type:
            queryset = queryset.filter(type=instance_type)

        if db_type:
            queryset = queryset.filter(db_type=db_type)

        for tag_id in tag_ids:
            queryset = queryset.filter(instance_tag=tag_id, instance_tag__active=True)

        queryset = queryset.distinct()

        allowed_ordering = {
            "id",
            "-id",
            "instance_name",
            "-instance_name",
            "db_type",
            "-db_type",
            "host",
            "-host",
            "port",
            "-port",
            "user",
            "-user",
            "type",
            "-type",
        }
        if ordering in allowed_ordering:
            queryset = queryset.order_by(ordering, "id")

        return queryset

    @extend_schema(
        summary="Instance List",
        responses={200: InstanceListSerializer},
        parameters=[
            OpenApiParameter(
                name="search",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY,
                description="Match instance ID, name, host, or user.",
            ),
            OpenApiParameter(
                name="type",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY,
                description="Instance type: master or slave.",
            ),
            OpenApiParameter(
                name="db_type",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY,
                description="Database engine type.",
            ),
            OpenApiParameter(
                name="tags",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY,
                description="Filter by active instance-tag IDs. Repeat the parameter to apply AND semantics.",
            ),
            OpenApiParameter(
                name="ordering",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY,
                description="Ordering key, e.g. instance_name or -host.",
            ),
        ],
        description="List all instances with pagination, search, and legacy inventory filters.",
    )
    @method_decorator(
        permission_required("sql.menu_instance_list", raise_exception=True)
    )
    def get(self, request):
        instances = self.filter_queryset(self.get_queryset())
        page_ins = self.paginate_queryset(queryset=instances)
        serializer_obj = self.get_serializer(page_ins, many=True)
        return self.get_paginated_response(serializer_obj.data)

    @extend_schema(
        summary="Create Instance",
        request=InstanceCreateSerializer,
        responses={201: InstanceListSerializer},
        description="Create an instance configuration for the SPA inventory flow.",
    )
    @method_decorator(permission_required("sql.menu_instance", raise_exception=True))
    def post(self, request):
        serializer = InstanceCreateSerializer(data=request.data)
        if serializer.is_valid():
            instance = serializer.save()
            return success_response(
                data=InstanceListSerializer(instance).data,
                detail="Instance created successfully.",
                status_code=status.HTTP_201_CREATED,
            )
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class InstanceDetail(views.APIView):
    """
    Instance operations.
    """

    serializer_class = InstanceEditorSerializer

    def get_object(self, pk):
        try:
            return Instance.objects.prefetch_related(
                "resource_group", "instance_tag"
            ).get(pk=pk)
        except Instance.DoesNotExist:
            raise Http404

    @extend_schema(
        summary="Instance Detail",
        responses={200: InstanceEditorSerializer},
        description="Get a single instance configuration for editing.",
    )
    @method_decorator(permission_required("sql.menu_instance", raise_exception=True))
    def get(self, request, pk):
        instance = self.get_object(pk)
        return success_response(data=InstanceEditorSerializer(instance).data)

    @extend_schema(
        summary="Update Instance",
        request=InstanceDetailSerializer,
        responses={200: InstanceEditorSerializer},
        description="Update an instance configuration.",
    )
    @method_decorator(permission_required("sql.menu_instance", raise_exception=True))
    def put(self, request, pk):
        instance = self.get_object(pk)
        serializer = InstanceDetailSerializer(instance, data=request.data)
        if serializer.is_valid():
            serializer.save()
            return success_response(data=InstanceEditorSerializer(instance).data)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    @extend_schema(
        summary="Delete Instance", description="Delete an instance configuration."
    )
    @method_decorator(permission_required("sql.menu_instance", raise_exception=True))
    def delete(self, request, pk):
        instance = self.get_object(pk)
        instance.delete()
        return success_response()


class InstanceMetadata(views.APIView):
    """Lookup data used by the SPA inventory list and create form."""

    @extend_schema(
        summary="Instance Inventory Metadata",
        responses={200: InstanceMetadataSerializer},
        description="List available instance types, enabled database types, active tags, and resource groups.",
    )
    def get(self, request):
        _require_any_permission(request, "sql.menu_instance", "sql.menu_instance_list")

        instance_types = [
            {"value": "master", "label": "MASTER"},
            {"value": "slave", "label": "SLAVE"},
        ]
        db_types = []
        for db_type in settings.ENABLED_ENGINES:
            engine = engine_map.get(db_type)
            if not engine:
                continue
            db_types.append({"value": db_type, "label": engine.name})

        payload = {
            "instance_types": instance_types,
            "db_types": db_types,
            "tags": InstanceTag.objects.filter(active=True).order_by("tag_name", "id"),
            "resource_groups": ResourceGroup.objects.filter(is_deleted=0).order_by(
                "group_name", "group_id"
            ),
        }
        serializer = InstanceMetadataSerializer(payload)
        return success_response(data=serializer.data)


class DataDictionaryInstanceList(views.APIView):
    """List instances that support Data Dictionary browsing."""

    @extend_schema(
        summary="Data Dictionary Instances",
        responses={200: DataDictionaryInstanceSerializer(many=True)},
        description="List user-visible MySQL, MSSQL, and Oracle instances for the SPA Data Dictionary.",
    )
    @method_decorator(
        permission_required("sql.menu_data_dictionary", raise_exception=True)
    )
    def get(self, request):
        instances = _data_dictionary_queryset(request.user)
        return success_response(
            data=DataDictionaryInstanceSerializer(instances, many=True).data
        )


class DataDictionaryDatabaseList(views.APIView):
    """List databases for one Data Dictionary instance."""

    @extend_schema(
        summary="Data Dictionary Databases",
        responses={200: DataDictionaryDatabaseListSerializer},
        parameters=[
            OpenApiParameter(
                name="instance_id",
                type=OpenApiTypes.INT,
                location=OpenApiParameter.QUERY,
                required=True,
                description="Instance ID.",
            ),
        ],
        description="List visible databases for a Data Dictionary instance.",
    )
    @method_decorator(
        permission_required("sql.menu_data_dictionary", raise_exception=True)
    )
    def get(self, request):
        instance_id = request.query_params.get("instance_id")
        if not instance_id:
            raise serializers.ValidationError(
                {"instance_id": "This field is required."}
            )

        instance = _data_dictionary_instance(request.user, instance_id)

        try:
            query_engine = get_engine(instance=instance)
            databases = query_engine.get_all_databases()
            databases.rows = filter_db_list(
                db_list=databases.rows,
                db_name_regex=query_engine.instance.show_db_name_regex,
                is_match_regex=True,
            )
            databases.rows = filter_db_list(
                db_list=databases.rows,
                db_name_regex=query_engine.instance.denied_db_name_regex,
                is_match_regex=False,
            )
        except Exception as exc:
            logger.exception(
                "Failed to list data dictionary databases for instance_id=%s",
                instance_id,
            )
            raise serializers.ValidationError({"errors": str(exc)})
        finally:
            if "query_engine" in locals():
                query_engine.close()

        if databases.error:
            raise serializers.ValidationError({"errors": databases.error})

        payload = {"count": len(databases.rows), "result": databases.rows}
        return success_response(data=DataDictionaryDatabaseListSerializer(payload).data)


class DataDictionaryTableList(views.APIView):
    """List tables grouped by initial letter for one database."""

    @extend_schema(
        summary="Data Dictionary Tables",
        responses={200: DataDictionaryTableGroupListSerializer},
        parameters=[
            OpenApiParameter(
                name="instance_id",
                type=OpenApiTypes.INT,
                location=OpenApiParameter.QUERY,
                required=True,
                description="Instance ID.",
            ),
            OpenApiParameter(
                name="db_name",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY,
                required=True,
                description="Database name.",
            ),
        ],
        description="List grouped tables and table comments for a Data Dictionary database.",
    )
    @method_decorator(
        permission_required("sql.menu_data_dictionary", raise_exception=True)
    )
    def get(self, request):
        instance_id = request.query_params.get("instance_id")
        db_name = request.query_params.get("db_name", "").strip()
        if not instance_id:
            raise serializers.ValidationError(
                {"instance_id": "This field is required."}
            )
        if not db_name:
            raise serializers.ValidationError({"db_name": "This field is required."})

        instance = _data_dictionary_instance(request.user, instance_id)

        try:
            query_engine = get_engine(instance=instance)
            escaped_db_name = query_engine.escape_string(db_name)
            grouped_tables = query_engine.get_group_tables_by_db(
                db_name=escaped_db_name
            )
        except Exception as exc:
            logger.exception(
                "Failed to list data dictionary tables for instance_id=%s db_name=%s",
                instance_id,
                db_name,
            )
            raise serializers.ValidationError({"errors": str(exc)})
        finally:
            if "query_engine" in locals():
                query_engine.close()

        table_groups = _table_groups_to_list(grouped_tables)
        payload = {
            "count": sum(len(item["tables"]) for item in table_groups),
            "result": table_groups,
        }
        return success_response(
            data=DataDictionaryTableGroupListSerializer(payload).data
        )


class DataDictionaryTableDetail(views.APIView):
    """Get table metadata, columns, indexes, and optional CREATE SQL."""

    @extend_schema(
        summary="Data Dictionary Table Detail",
        responses={200: DataDictionaryTableDetailSerializer},
        parameters=[
            OpenApiParameter(
                name="instance_id",
                type=OpenApiTypes.INT,
                location=OpenApiParameter.QUERY,
                required=True,
                description="Instance ID.",
            ),
            OpenApiParameter(
                name="db_name",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY,
                required=True,
                description="Database name.",
            ),
            OpenApiParameter(
                name="table_name",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY,
                required=True,
                description="Table name.",
            ),
        ],
        description="Get table metadata for the SPA Data Dictionary detail panel.",
    )
    @method_decorator(
        permission_required("sql.menu_data_dictionary", raise_exception=True)
    )
    def get(self, request):
        instance_id = request.query_params.get("instance_id")
        db_name = request.query_params.get("db_name", "").strip()
        table_name = request.query_params.get("table_name", "").strip()
        if not instance_id:
            raise serializers.ValidationError(
                {"instance_id": "This field is required."}
            )
        if not db_name:
            raise serializers.ValidationError({"db_name": "This field is required."})
        if not table_name:
            raise serializers.ValidationError({"table_name": "This field is required."})

        instance = _data_dictionary_instance(request.user, instance_id)

        try:
            query_engine = get_engine(instance=instance)
            escaped_db_name = query_engine.escape_string(db_name)
            escaped_table_name = query_engine.escape_string(table_name)
            data = {
                "meta_data": query_engine.get_table_meta_data(
                    db_name=escaped_db_name, tb_name=escaped_table_name
                ),
                "desc": query_engine.get_table_desc_data(
                    db_name=escaped_db_name, tb_name=escaped_table_name
                ),
                "index": query_engine.get_table_index_data(
                    db_name=escaped_db_name, tb_name=escaped_table_name
                ),
            }
            if instance.db_type == "mysql":
                quoted_table_name = _mysql_quote_identifier(table_name)
                create_sql = query_engine.query(
                    escaped_db_name, f"show create table {quoted_table_name};"
                )
                if create_sql.error:
                    raise serializers.ValidationError({"errors": create_sql.error})
                data["create_sql"] = create_sql.rows
        except serializers.ValidationError:
            raise
        except Exception as exc:
            logger.exception(
                "Failed to load data dictionary table detail for instance_id=%s db_name=%s table_name=%s",
                instance_id,
                db_name,
                table_name,
            )
            raise serializers.ValidationError({"errors": str(exc)})
        finally:
            if "query_engine" in locals():
                query_engine.close()

        return success_response(data=DataDictionaryTableDetailSerializer(data).data)


class DataDictionaryExport(views.APIView):
    """Export Data Dictionary HTML for a database or full instance."""

    @extend_schema(
        summary="Export Data Dictionary",
        parameters=[
            OpenApiParameter(
                name="instance_id",
                type=OpenApiTypes.INT,
                location=OpenApiParameter.QUERY,
                required=True,
                description="Instance ID.",
            ),
            OpenApiParameter(
                name="db_name",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY,
                description="Database name. Required for non-superusers.",
            ),
        ],
        description="Export Data Dictionary HTML. Non-superusers must export a single database.",
    )
    @method_decorator(
        permission_required("sql.data_dictionary_export", raise_exception=True)
    )
    def get(self, request):
        instance_id = request.query_params.get("instance_id")
        db_name = request.query_params.get("db_name", "").strip()
        if not instance_id:
            raise serializers.ValidationError(
                {"instance_id": "This field is required."}
            )

        instance = _data_dictionary_instance(request.user, instance_id)

        try:
            query_engine = get_engine(instance=instance)
            if db_name:
                databases = [query_engine.escape_string(db_name)]
            elif request.user.is_superuser:
                databases = query_engine.get_all_databases().rows
            else:
                raise PermissionDenied(
                    "Only admins can export dictionary data for a full instance."
                )

            export_dir = os.path.join(settings.BASE_DIR, "downloads", "dictionary")
            os.makedirs(export_dir, exist_ok=True)

            for database in databases:
                table_metas = query_engine.get_tables_metas_data(db_name=database)
                context = {
                    "db_name": database,
                    "tables": table_metas,
                    "export_time": timezone.now(),
                }
                data = loader.render_to_string(
                    template_name="dictionaryexport.html",
                    context=context,
                    request=request,
                )
                full_path = _safe_dictionary_export_path(
                    export_dir, instance.instance_name, database
                )
                if not full_path:
                    raise serializers.ValidationError(
                        {"errors": "Invalid instance name or database name."}
                    )
                with open(full_path, "w", encoding="utf-8") as export_file:
                    export_file.write(data)
        except PermissionDenied:
            raise
        except serializers.ValidationError:
            raise
        except Exception as exc:
            logger.exception(
                "Failed to export data dictionary for instance_id=%s db_name=%s",
                instance_id,
                db_name,
            )
            raise serializers.ValidationError({"errors": str(exc)})
        finally:
            if "query_engine" in locals():
                query_engine.close()

        if db_name:
            full_path = _safe_dictionary_export_path(
                export_dir, instance.instance_name, db_name
            )
            if not full_path:
                raise serializers.ValidationError(
                    {"errors": "Invalid instance name or database name."}
                )
            response = FileResponse(open(full_path, "rb"))
            response["Content-Type"] = "application/octet-stream"
            response["Content-Disposition"] = (
                f'attachment;filename="{quote(instance.instance_name)}_{quote(db_name)}.html"'
            )
            return response

        return success_response(
            detail=(
                f"Data dictionary export for instance {instance.instance_name} succeeded. "
                "Please download it from the downloads directory."
            )
        )


class InstanceOperationDatabaseInstanceList(views.APIView):
    """List instances available to database-management operators."""

    @extend_schema(
        summary="Instance Operation Database Instances",
        responses={200: DataDictionaryInstanceSerializer(many=True)},
        description="List MySQL and Mongo instances visible to the current database operator.",
    )
    @method_decorator(permission_required("sql.menu_database", raise_exception=True))
    def get(self, request):
        instances = _operation_database_queryset(request.user)
        return success_response(
            data=DataDictionaryInstanceSerializer(instances, many=True).data
        )


class InstanceOperationDatabaseListCreate(views.APIView):
    """List or create databases for the Instance Operations workspace."""

    @extend_schema(
        summary="Instance Operation Database List",
        responses={200: InstanceDatabaseListSerializer},
        parameters=[
            OpenApiParameter(
                name="instance_id",
                type=OpenApiTypes.INT,
                location=OpenApiParameter.QUERY,
                required=True,
                description="Instance ID.",
            ),
            OpenApiParameter(
                name="saved",
                type=OpenApiTypes.BOOL,
                location=OpenApiParameter.QUERY,
                description="Only show databases registered in Datamingle metadata.",
            ),
        ],
        description="List live databases merged with saved owner metadata.",
    )
    @method_decorator(permission_required("sql.menu_database", raise_exception=True))
    def get(self, request):
        instance_id = request.query_params.get("instance_id")
        saved_only = request.query_params.get("saved") == "true"
        if not instance_id:
            return success_response(data={"count": 0, "results": []})

        instance = _operation_database_instance(request.user, instance_id)
        configured_databases = {
            item["db_name"]: {**item, "saved": True}
            for item in InstanceDatabase.objects.filter(instance=instance).values(
                "id", "db_name", "owner", "owner_display", "remark", "sys_time"
            )
        }

        try:
            query_engine = get_engine(instance=instance)
            query_result = query_engine.get_all_databases_summary()
        except Exception as exc:
            logger.exception(
                "Failed to list instance-operation databases for instance_id=%s",
                instance_id,
            )
            raise serializers.ValidationError({"errors": str(exc)})
        finally:
            if "query_engine" in locals():
                query_engine.close()

        if query_result.error:
            raise serializers.ValidationError({"errors": query_result.error})

        rows = []
        for row in query_result.rows:
            database_name = row.get("db_name")
            merged_row = {**row, "saved": False}
            if database_name in configured_databases:
                merged_row.update(configured_databases[database_name])
            if not saved_only or merged_row["saved"]:
                rows.append(merged_row)

        payload = {"count": len(rows), "results": rows}
        return success_response(data=InstanceDatabaseListSerializer(payload).data)

    @extend_schema(
        summary="Create Instance Database",
        request=InstanceDatabasePayloadSerializer,
        responses={201: InstanceDatabaseMetadataSerializer},
        description="Create a database when supported, then register owner metadata.",
    )
    @method_decorator(permission_required("sql.menu_database", raise_exception=True))
    def post(self, request):
        serializer = InstanceDatabasePayloadSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data
        instance = _operation_database_instance(request.user, data["instance_id"])
        owner = data.get("owner", "")
        owner_display = _owner_display(owner)
        db_name = data["db_name"]
        remark = data.get("remark", "")

        try:
            engine = get_engine(instance=instance)
            if instance.db_type == "mysql":
                quoted_db_name = _mysql_quote_identifier(db_name)
                exec_result = engine.execute(
                    db_name="information_schema",
                    sql=f"create database {quoted_db_name};",
                )
            elif instance.db_type == "mongo":
                exec_result = ResultSet()
                conn = engine.get_connection()
                database = conn[db_name]
                database.create_collection(name=f"archery-{db_name}")
            else:
                raise serializers.ValidationError(
                    {"errors": f"Unsupported instance type: {instance.db_type}"}
                )
        except serializers.ValidationError:
            raise
        except Exception as exc:
            logger.exception(
                "Failed to create database for instance_id=%s db_name=%s",
                instance.id,
                db_name,
            )
            raise serializers.ValidationError({"errors": str(exc)})
        finally:
            if "engine" in locals():
                engine.close()

        if exec_result.error:
            raise serializers.ValidationError({"errors": exec_result.error})

        database_record = InstanceDatabase.objects.create(
            instance=instance,
            db_name=db_name,
            owner=owner,
            owner_display=owner_display,
            remark=remark,
        )
        _clear_instance_resource_cache()
        return success_response(
            data=InstanceDatabaseMetadataSerializer(database_record).data,
            detail="Database created successfully.",
            status_code=status.HTTP_201_CREATED,
        )


class InstanceOperationDatabaseDetail(views.APIView):
    """Update saved owner metadata for a database."""

    @extend_schema(
        summary="Update Instance Database Metadata",
        request=InstanceDatabasePayloadSerializer,
        responses={200: InstanceDatabaseMetadataSerializer},
        description="Register or update database owner metadata.",
    )
    @method_decorator(permission_required("sql.menu_database", raise_exception=True))
    def put(self, request):
        serializer = InstanceDatabasePayloadSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data
        instance = _operation_database_instance(request.user, data["instance_id"])
        owner = data.get("owner", "")
        owner_display = _owner_display(owner)

        database_record, _ = InstanceDatabase.objects.update_or_create(
            instance=instance,
            db_name=data["db_name"],
            defaults={
                "owner": owner,
                "owner_display": owner_display,
                "remark": data.get("remark", ""),
            },
        )
        return success_response(
            data=InstanceDatabaseMetadataSerializer(database_record).data,
            detail="Database metadata updated successfully.",
        )


class InstanceOperationAccountInstanceList(views.APIView):
    """List instances available to account-management operators."""

    @extend_schema(
        summary="Instance Operation Account Instances",
        responses={200: DataDictionaryInstanceSerializer(many=True)},
        description="List MySQL and Mongo instances visible to the current account operator.",
    )
    @method_decorator(
        permission_required("sql.menu_instance_account", raise_exception=True)
    )
    def get(self, request):
        instances = _operation_account_queryset(request.user)
        return success_response(
            data=DataDictionaryInstanceSerializer(instances, many=True).data
        )


class InstanceOperationAccountListCreate(views.APIView):
    """List or create accounts for the Instance Operations workspace."""

    @extend_schema(
        summary="Instance Operation Account List",
        responses={200: InstanceAccountListSerializer},
        parameters=[
            OpenApiParameter(
                name="instance_id",
                type=OpenApiTypes.INT,
                location=OpenApiParameter.QUERY,
                required=True,
                description="Instance ID.",
            ),
            OpenApiParameter(
                name="saved",
                type=OpenApiTypes.BOOL,
                location=OpenApiParameter.QUERY,
                description="Only show accounts registered in Datamingle metadata.",
            ),
        ],
        description="List live database accounts merged with saved metadata.",
    )
    @method_decorator(
        permission_required("sql.menu_instance_account", raise_exception=True)
    )
    def get(self, request):
        instance_id = request.query_params.get("instance_id")
        saved_only = request.query_params.get("saved") == "true"
        if not instance_id:
            return success_response(data={"count": 0, "results": []})

        instance = _operation_account_instance(request.user, instance_id)
        configured_accounts = {}
        for account in InstanceAccount.objects.filter(instance=instance).values(
            "id", "user", "host", "db_name", "remark", "sys_time"
        ):
            account["saved"] = True
            configured_accounts[
                get_instanceaccount_unique_value(instance.db_type, account)
            ] = account

        try:
            query_engine = get_engine(instance=instance)
            query_result = query_engine.get_instance_users_summary()
        except Exception as exc:
            logger.exception(
                "Failed to list instance-operation accounts for instance_id=%s",
                instance_id,
            )
            raise serializers.ValidationError({"errors": str(exc)})
        finally:
            if "query_engine" in locals():
                query_engine.close()

        if query_result.error:
            raise serializers.ValidationError({"errors": query_result.error})

        key = get_instanceaccount_unique_key(instance.db_type)
        rows = []
        for row in query_result.rows:
            merged_row = {**row, "saved": False}
            if row.get(key) in configured_accounts:
                merged_row.update(configured_accounts[row[key]])
            if not saved_only or merged_row["saved"]:
                rows.append(merged_row)

        payload = {"count": len(rows), "results": rows}
        return success_response(data=InstanceAccountListSerializer(payload).data)

    @extend_schema(
        summary="Create Instance Account",
        request=InstanceAccountPayloadSerializer,
        responses={201: InstanceAccountRecordSerializer},
        description="Create a database account on the instance, then save account metadata.",
    )
    @method_decorator(
        permission_required("sql.instance_account_manage", raise_exception=True)
    )
    def post(self, request):
        serializer = InstanceAccountPayloadSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data
        instance = _operation_account_instance(request.user, data["instance_id"])
        _validate_account_identity(instance, data, require_password=True)

        try:
            engine = get_engine(instance=instance)
            exec_result = engine.create_instance_user(
                db_name=data.get("db_name", ""),
                user=data["user"],
                host=data.get("host", ""),
                password1=data["password"],
                remark=data.get("remark", ""),
            )
        except Exception as exc:
            logger.exception(
                "Failed to create account for instance_id=%s user=%s",
                instance.id,
                data["user"],
            )
            raise serializers.ValidationError({"errors": str(exc)})
        finally:
            if "engine" in locals():
                engine.close()

        if exec_result.error:
            raise serializers.ValidationError({"errors": exec_result.error})

        accounts = []
        for row in exec_result.rows:
            account_data = dict(row)
            account_data["password"] = ""
            accounts.append(InstanceAccount(**account_data))
        InstanceAccount.objects.bulk_create(accounts, ignore_conflicts=True)
        created_account = None
        if accounts:
            first_account = accounts[0]
            created_account = InstanceAccount.objects.filter(
                instance=first_account.instance,
                user=first_account.user,
                host=first_account.host,
                db_name=first_account.db_name,
            ).first()
        return success_response(
            data=(
                InstanceAccountRecordSerializer(created_account).data
                if created_account
                else {}
            ),
            detail="Account created successfully.",
            status_code=status.HTTP_201_CREATED,
        )


class InstanceOperationAccountMetadata(views.APIView):
    """Update saved account metadata without changing live account privileges."""

    @extend_schema(
        summary="Update Instance Account Metadata",
        request=InstanceAccountPayloadSerializer,
        responses={200: InstanceAccountRecordSerializer},
        description="Register or update account metadata such as password and remark.",
    )
    @method_decorator(
        permission_required("sql.instance_account_manage", raise_exception=True)
    )
    def put(self, request):
        serializer = InstanceAccountPayloadSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data
        instance = _operation_account_instance(request.user, data["instance_id"])
        _validate_account_identity(instance, data)

        defaults = {"remark": data.get("remark", "")}
        account, _ = InstanceAccount.objects.update_or_create(
            instance=instance,
            user=data["user"],
            host=data.get("host", ""),
            db_name=data.get("db_name", ""),
            defaults=defaults,
        )
        return success_response(
            data=InstanceAccountRecordSerializer(account).data,
            detail="Account metadata updated successfully.",
        )


class InstanceOperationAccountPassword(views.APIView):
    """Reset a live account password and update saved metadata."""

    @extend_schema(
        summary="Reset Instance Account Password",
        request=InstanceAccountPasswordSerializer,
        responses={200: InstanceAccountRecordSerializer},
        description="Reset a database account password.",
    )
    @method_decorator(
        permission_required("sql.instance_account_manage", raise_exception=True)
    )
    def post(self, request):
        serializer = InstanceAccountPasswordSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data
        instance = _operation_account_instance(request.user, data["instance_id"])
        _validate_account_identity(instance, data, require_password=True)

        user_host = _mysql_user_host(
            data.get("user", ""), data.get("host", ""), data.get("user_host", "")
        )
        db_name_user = _mongo_db_name_user(
            data.get("db_name", ""), data.get("user", ""), data.get("db_name_user", "")
        )

        try:
            engine = get_engine(instance=instance)
            exec_result = engine.reset_instance_user_pwd(
                user_host=user_host,
                db_name_user=db_name_user,
                reset_pwd=data["password"],
            )
        except Exception as exc:
            logger.exception(
                "Failed to reset account password for instance_id=%s user=%s",
                instance.id,
                data["user"],
            )
            raise serializers.ValidationError({"errors": str(exc)})
        finally:
            if "engine" in locals():
                engine.close()

        if exec_result.error:
            raise serializers.ValidationError({"errors": exec_result.error})

        user, host, db_name = _account_metadata_fields(instance, data)
        account, _created = InstanceAccount.objects.get_or_create(
            instance=instance,
            user=user,
            host=host,
            db_name=db_name,
            defaults={"remark": ""},
        )
        return success_response(
            data=InstanceAccountRecordSerializer(account).data,
            detail="Password reset successfully.",
        )


class InstanceOperationAccountLock(views.APIView):
    """Lock or unlock a MySQL account."""

    @extend_schema(
        summary="Lock Or Unlock Instance Account",
        request=InstanceAccountLockSerializer,
        description="Lock or unlock a MySQL account.",
    )
    @method_decorator(
        permission_required("sql.instance_account_manage", raise_exception=True)
    )
    def post(self, request):
        serializer = InstanceAccountLockSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data
        instance = _operation_account_instance(
            request.user, data["instance_id"], db_type=["mysql"]
        )

        try:
            engine = get_engine(instance=instance)
            escaped_user_host = _parse_mysql_account_identifier(data["user_host"])
            action = "LOCK" if data["locked"] else "UNLOCK"
            lock_sql = f"ALTER USER {escaped_user_host} ACCOUNT {action};"
            exec_result = engine.execute(db_name="mysql", sql=lock_sql)
        except serializers.ValidationError:
            raise
        except Exception as exc:
            logger.exception(
                "Failed to change account lock state for instance_id=%s user_host=%s",
                instance.id,
                data["user_host"],
            )
            raise serializers.ValidationError({"errors": str(exc)})
        finally:
            if "engine" in locals():
                engine.close()

        return _account_result(exec_result, "Account lock state updated successfully.")


class InstanceOperationAccountDelete(views.APIView):
    """Delete a live account and saved metadata."""

    @extend_schema(
        summary="Delete Instance Account",
        request=InstanceAccountDeleteSerializer,
        description="Delete a database account and matching saved metadata.",
    )
    @method_decorator(
        permission_required("sql.instance_account_manage", raise_exception=True)
    )
    def delete(self, request):
        serializer = InstanceAccountDeleteSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data
        instance = _operation_account_instance(request.user, data["instance_id"])
        _validate_account_identity(instance, data)

        user_host = _mysql_user_host(
            data.get("user", ""), data.get("host", ""), data.get("user_host", "")
        )
        db_name_user = _mongo_db_name_user(
            data.get("db_name", ""), data.get("user", ""), data.get("db_name_user", "")
        )

        try:
            engine = get_engine(instance=instance)
            exec_result = engine.drop_instance_user(
                user_host=user_host, db_name_user=db_name_user
            )
        except Exception as exc:
            logger.exception(
                "Failed to delete account for instance_id=%s user=%s",
                instance.id,
                data["user"],
            )
            raise serializers.ValidationError({"errors": str(exc)})
        finally:
            if "engine" in locals():
                engine.close()

        if exec_result.error:
            raise serializers.ValidationError({"errors": exec_result.error})

        user, host, db_name = _account_metadata_fields(instance, data)
        InstanceAccount.objects.filter(
            instance=instance, user=user, host=host, db_name=db_name
        ).delete()
        return success_response(detail="Account deleted successfully.")


class InstanceOperationAccountGrant(views.APIView):
    """Grant or revoke account privileges."""

    @extend_schema(
        summary="Grant Or Revoke Instance Account Privileges",
        request=InstanceAccountGrantSerializer,
        description="Apply MySQL grants/revokes or Mongo role changes for an account.",
    )
    @method_decorator(
        permission_required("sql.instance_account_manage", raise_exception=True)
    )
    def post(self, request):
        serializer = InstanceAccountGrantSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data
        instance = _operation_account_instance(request.user, data["instance_id"])
        grant_sql = ""

        try:
            engine = get_engine(instance=instance)
            if instance.db_type == "mysql":
                user_host = data.get("user_host", "")
                if not user_host:
                    raise serializers.ValidationError(
                        {"user_host": "This field is required for MySQL grants."}
                    )
                escaped_user_host = _parse_mysql_account_identifier(user_host)
                grant_sql = _mysql_account_grant_sql(data, escaped_user_host)
                exec_result = engine.execute(db_name="mysql", sql=grant_sql)
            elif instance.db_type == "mongo":
                db_name_user = data.get("db_name_user", "")
                if "." not in db_name_user:
                    raise serializers.ValidationError(
                        {"db_name_user": "Expected database.user for Mongo grants."}
                    )
                db_name, user = db_name_user.split(".", 1)
                exec_result = ResultSet()
                try:
                    conn = engine.get_connection()
                    conn[db_name].command(
                        "updateUser", user, roles=data.get("roles", [])
                    )
                except Exception as exc:
                    exec_result.error = str(exc)
            else:
                raise serializers.ValidationError(
                    {"errors": f"Unsupported instance type: {instance.db_type}"}
                )
        except serializers.ValidationError:
            raise
        except Exception as exc:
            logger.exception(
                "Failed to change account privileges for instance_id=%s",
                instance.id,
            )
            raise serializers.ValidationError({"errors": str(exc)})
        finally:
            if "engine" in locals():
                engine.close()

        return _account_result(
            exec_result,
            "Account privileges updated successfully.",
            data={"grant_sql": grant_sql},
        )


class InstanceOperationParamInstanceList(views.APIView):
    """List instances available to parameter-management operators."""

    @extend_schema(
        summary="Instance Operation Parameter Instances",
        responses={200: DataDictionaryInstanceSerializer(many=True)},
        description="List MySQL and GoInception instances visible to the current parameter operator.",
    )
    @method_decorator(permission_required("sql.menu_param", raise_exception=True))
    def get(self, request):
        instances = _operation_param_queryset(request.user)
        return success_response(
            data=DataDictionaryInstanceSerializer(instances, many=True).data
        )


class InstanceOperationParamList(views.APIView):
    """List runtime parameters merged with saved parameter templates."""

    @extend_schema(
        summary="Instance Operation Parameter List",
        responses={200: InstanceParamListSerializer},
        parameters=[
            OpenApiParameter(
                name="instance_id",
                type=OpenApiTypes.INT,
                location=OpenApiParameter.QUERY,
                required=True,
                description="Instance ID.",
            ),
            OpenApiParameter(
                name="editable",
                type=OpenApiTypes.BOOL,
                location=OpenApiParameter.QUERY,
                description="Filter editable or read-only parameters.",
            ),
            OpenApiParameter(
                name="search",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY,
                description="Match parameter name or description.",
            ),
        ],
        description="List runtime instance parameters with template metadata.",
    )
    @method_decorator(permission_required("sql.param_view", raise_exception=True))
    def get(self, request):
        instance_id = request.query_params.get("instance_id")
        editable_filter = request.query_params.get("editable")
        search = request.query_params.get("search", "").strip().lower()
        if not instance_id:
            return success_response(data={"count": 0, "results": []})

        instance = _operation_param_instance(request.user, instance_id)
        template_queryset = ParamTemplate.objects.filter(db_type=instance.db_type)
        if search:
            template_queryset = template_queryset.filter(
                variable_name__icontains=search
            )

        configured_params = {}
        for param in template_queryset.values(
            "id",
            "variable_name",
            "default_value",
            "valid_values",
            "description",
            "editable",
        ):
            param["variable_name"] = param["variable_name"].lower()
            param["configured"] = True
            configured_params[param["variable_name"]] = param

        try:
            engine = get_engine(instance=instance)
            variables = engine.get_variables()
        except Exception as exc:
            logger.exception(
                "Failed to list instance parameters for instance_id=%s",
                instance_id,
            )
            raise serializers.ValidationError({"errors": str(exc)})
        finally:
            if "engine" in locals():
                engine.close()

        if variables.error:
            raise serializers.ValidationError({"errors": variables.error})

        rows = []
        for variable in variables.rows:
            variable_name = str(variable[0]).lower()
            runtime_value = "" if len(variable) < 2 else variable[1]
            row = {
                "variable_name": variable_name,
                "runtime_value": runtime_value,
                "default_value": "",
                "valid_values": "",
                "description": "",
                "editable": False,
                "configured": False,
            }
            if variable_name in configured_params:
                row.update(configured_params[variable_name])
            if (
                search
                and search
                not in " ".join(
                    [row["variable_name"], row.get("description", "")]
                ).lower()
            ):
                continue
            if editable_filter == "true" and not row["editable"]:
                continue
            if editable_filter == "false" and row["editable"]:
                continue
            rows.append(row)

        payload = {"count": len(rows), "results": rows}
        return success_response(data=InstanceParamListSerializer(payload).data)


class InstanceOperationParamHistory(views.APIView):
    """List parameter change history."""

    @extend_schema(
        summary="Instance Operation Parameter History",
        responses={200: InstanceParamHistoryListSerializer},
        parameters=[
            OpenApiParameter(
                name="instance_id",
                type=OpenApiTypes.INT,
                location=OpenApiParameter.QUERY,
                required=True,
                description="Instance ID.",
            ),
            OpenApiParameter(
                name="search",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY,
                description="Match parameter name.",
            ),
        ],
        description="List parameter edits for a visible instance.",
    )
    @method_decorator(permission_required("sql.param_view", raise_exception=True))
    def get(self, request):
        instance_id = request.query_params.get("instance_id")
        search = request.query_params.get("search", "").strip()
        page = _safe_positive_int(request.query_params.get("page", 1), 1, 1000000)
        size = _safe_positive_int(request.query_params.get("size", 20), 20, 100)
        if not instance_id:
            return success_response(data={"count": 0, "results": []})

        instance = _operation_param_instance(request.user, instance_id)
        queryset = ParamHistory.objects.filter(instance=instance).select_related(
            "instance"
        )
        if search:
            queryset = queryset.filter(variable_name__icontains=search)

        count = queryset.count()
        offset = (page - 1) * size
        history = queryset[offset : offset + size]
        payload = {"count": count, "results": history}
        return success_response(data=InstanceParamHistoryListSerializer(payload).data)


class InstanceOperationParamEdit(views.APIView):
    """Edit an online dynamic parameter and record history."""

    @extend_schema(
        summary="Edit Instance Parameter",
        request=InstanceParamEditSerializer,
        description="Set a dynamic instance parameter and record the change history.",
    )
    @method_decorator(permission_required("sql.param_edit", raise_exception=True))
    def post(self, request):
        serializer = InstanceParamEditSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data
        instance = _operation_param_instance(request.user, data["instance_id"])

        try:
            engine = get_engine(instance=instance)
            variable_name = engine.escape_string(data["variable_name"])
            variable_value = engine.escape_string(data["runtime_value"])
            param_template = ParamTemplate.objects.filter(
                db_type=instance.db_type, variable_name__iexact=variable_name
            ).first()
            if not param_template:
                raise serializers.ValidationError(
                    {"errors": "Please configure this parameter in the template first."}
                )
            if not param_template.editable:
                raise serializers.ValidationError(
                    {"errors": "This parameter is not marked editable."}
                )

            current_variables = engine.get_variables(variables=[variable_name])
            if current_variables.error:
                raise serializers.ValidationError({"errors": current_variables.error})
            if not current_variables.rows:
                raise serializers.ValidationError(
                    {"errors": "Parameter was not returned by the instance."}
                )

            current_row = current_variables.rows[0]
            if (
                not isinstance(current_row, (list, tuple))
                or len(current_row) < 2
                or current_row[1] is None
            ):
                raise serializers.ValidationError(
                    {"errors": "Parameter returned unexpected row shape from instance."}
                )

            runtime_value = current_row[1]
            if str(variable_value) == str(runtime_value):
                raise serializers.ValidationError(
                    {
                        "errors": "Parameter value matches runtime value; no update was made."
                    }
                )

            set_result = engine.set_variable(
                variable_name=variable_name, variable_value=variable_value
            )
        except serializers.ValidationError:
            raise
        except Exception as exc:
            logger.exception(
                "Failed to edit instance parameter for instance_id=%s variable=%s",
                instance.id,
                data["variable_name"],
            )
            raise serializers.ValidationError({"errors": str(exc)})
        finally:
            if "engine" in locals():
                engine.close()

        if set_result.error:
            raise serializers.ValidationError(
                {"errors": f"Set variable failed, error: {set_result.error}"}
            )

        ParamHistory.objects.create(
            instance=instance,
            variable_name=variable_name,
            old_var=runtime_value,
            new_var=variable_value,
            set_sql=set_result.full_sql,
            user_name=request.user.username,
            user_display=request.user.display,
        )
        return success_response(
            detail="Parameter updated successfully. Persist it manually in the database config file if required."
        )


class InstanceOperationDiagnosticInstanceList(views.APIView):
    """List instances available to session diagnostics operators."""

    @extend_schema(
        summary="Instance Operation Diagnostic Instances",
        responses={200: DataDictionaryInstanceSerializer(many=True)},
        description="List user-visible instances for session diagnostics.",
    )
    @method_decorator(
        permission_required("sql.menu_dbdiagnostic", raise_exception=True)
    )
    def get(self, request):
        instances = _operation_diagnostic_queryset(request.user)
        return success_response(
            data=DataDictionaryInstanceSerializer(instances, many=True).data
        )


class InstanceOperationDiagnosticProcessList(views.APIView):
    """List live sessions/processes for an instance."""

    @extend_schema(
        summary="Instance Operation Process List",
        responses={200: InstanceDiagnosticListSerializer},
        parameters=[
            OpenApiParameter(
                name="instance_id",
                type=OpenApiTypes.INT,
                location=OpenApiParameter.QUERY,
                required=True,
                description="Instance ID.",
            ),
            OpenApiParameter(
                name="command_type",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY,
                description="Process command filter, such as Query, Sleep, Not Sleep, or All.",
            ),
        ],
        description="List process/session diagnostics for the selected instance.",
    )
    @method_decorator(permission_required("sql.process_view", raise_exception=True))
    def get(self, request):
        instance_id = request.query_params.get("instance_id")
        command_type = request.query_params.get("command_type", "Query")
        if not instance_id:
            return success_response(data={"count": 0, "results": []})

        instance = _operation_diagnostic_instance(request.user, instance_id)
        allowed_params = {"instance_id", "command_type", *ALLOWED_PROCESSLIST_PARAMS}
        unsupported_params = set(request.query_params) - allowed_params
        if unsupported_params:
            return Response(
                {
                    "errors": "Unsupported process list parameter(s): "
                    + ", ".join(sorted(unsupported_params))
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        request_kwargs = {
            key: request.query_params[key]
            for key in ALLOWED_PROCESSLIST_PARAMS
            if key in request.query_params
        }

        try:
            engine = get_engine(instance=instance)
            query_result = engine.processlist(
                command_type=command_type, **request_kwargs
            )
        except Exception as exc:
            logger.exception(
                "Failed to list diagnostic processes for instance_id=%s",
                instance.id,
            )
            raise serializers.ValidationError({"errors": str(exc)})
        finally:
            if "engine" in locals():
                engine.close()

        if query_result.error:
            raise serializers.ValidationError({"errors": query_result.error})

        rows = query_result.to_dict()
        return success_response(
            data=InstanceDiagnosticListSerializer(
                {"count": len(rows), "results": rows}
            ).data
        )


class InstanceOperationDiagnosticKillPreview(views.APIView):
    """Build kill SQL for selected process IDs."""

    @extend_schema(
        summary="Build Session Kill Command",
        request=InstanceDiagnosticKillPreviewSerializer,
        responses={200: InstanceDiagnosticKillResultSerializer},
        description="Generate the kill command for selected live process IDs.",
    )
    @method_decorator(permission_required("sql.process_kill", raise_exception=True))
    def post(self, request):
        serializer = InstanceDiagnosticKillPreviewSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data
        instance = _operation_diagnostic_instance(request.user, data["instance_id"])

        try:
            engine = get_engine(instance=instance)
            kill_sql = engine.get_kill_command(data["thread_ids"])
        except AttributeError:
            raise serializers.ValidationError(
                {
                    "errors": f"{instance.db_type} does not support kill command generation."
                }
            )
        except Exception as exc:
            logger.exception(
                "Failed to build kill command for instance_id=%s", instance.id
            )
            raise serializers.ValidationError({"errors": str(exc)})
        finally:
            if "engine" in locals():
                engine.close()

        return success_response(
            data=InstanceDiagnosticKillResultSerializer(
                {"kill_sql": kill_sql or ""}
            ).data
        )


class InstanceOperationDiagnosticKill(views.APIView):
    """Terminate selected sessions."""

    @extend_schema(
        summary="Kill Sessions",
        request=InstanceDiagnosticKillPreviewSerializer,
        description="Terminate selected sessions for supported database engines.",
    )
    @method_decorator(permission_required("sql.process_kill", raise_exception=True))
    def post(self, request):
        serializer = InstanceDiagnosticKillPreviewSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data
        instance = _operation_diagnostic_instance(request.user, data["instance_id"])

        try:
            engine = get_engine(instance=instance)
            if instance.db_type in ["mysql", "doris"]:
                result = engine.kill(data["thread_ids"])
            elif instance.db_type == "mongo":
                result = engine.kill_op(data["thread_ids"])
            elif instance.db_type == "oracle":
                result = engine.kill_session(data["thread_ids"])
            else:
                raise serializers.ValidationError(
                    {
                        "errors": f"{instance.db_type} does not support session termination."
                    }
                )
        except serializers.ValidationError:
            raise
        except Exception as exc:
            logger.exception("Failed to kill sessions for instance_id=%s", instance.id)
            raise serializers.ValidationError({"errors": str(exc)})
        finally:
            if "engine" in locals():
                engine.close()

        return _account_result(result, "Sessions terminated successfully.")


class InstanceOperationDiagnosticTablespace(views.APIView):
    """List tablespace usage."""

    @extend_schema(
        summary="Instance Operation Tablespace",
        responses={200: InstanceDiagnosticListSerializer},
        parameters=[
            OpenApiParameter(
                name="instance_id",
                type=OpenApiTypes.INT,
                location=OpenApiParameter.QUERY,
                required=True,
                description="Instance ID.",
            ),
        ],
        description="List tablespace usage for supported instances.",
    )
    @method_decorator(permission_required("sql.tablespace_view", raise_exception=True))
    def get(self, request):
        instance_id = request.query_params.get("instance_id")
        page = _safe_positive_int(request.query_params.get("page", 1), 1, 1000000)
        size = _safe_positive_int(request.query_params.get("size", 14), 14, 100)
        if not instance_id:
            return success_response(data={"count": 0, "results": []})

        instance = _operation_diagnostic_instance(request.user, instance_id)
        offset = (page - 1) * size

        try:
            engine = get_engine(instance=instance)
            query_result = engine.tablespace(offset, size)
            count_result = engine.tablespace_count()
        except AttributeError:
            raise serializers.ValidationError(
                {"errors": f"{instance.db_type} does not support tablespace queries."}
            )
        except Exception as exc:
            logger.exception(
                "Failed to list tablespace for instance_id=%s", instance.id
            )
            raise serializers.ValidationError({"errors": str(exc)})
        finally:
            if "engine" in locals():
                engine.close()

        if query_result.error:
            raise serializers.ValidationError({"errors": query_result.error})
        if count_result.error:
            raise serializers.ValidationError({"errors": count_result.error})

        total = count_result.rows[0][0] if count_result.rows else 0
        return success_response(
            data=InstanceDiagnosticListSerializer(
                {"count": total, "results": query_result.to_dict()}
            ).data
        )


class InstanceOperationDiagnosticTransactions(views.APIView):
    """List long-running transactions."""

    @extend_schema(
        summary="Instance Operation Transactions",
        responses={200: InstanceDiagnosticListSerializer},
        description="List long-running transactions for supported instances.",
    )
    @method_decorator(permission_required("sql.trx_view", raise_exception=True))
    def get(self, request):
        instance_id = request.query_params.get("instance_id")
        if not instance_id:
            return success_response(data={"count": 0, "results": []})

        instance = _operation_diagnostic_instance(request.user, instance_id)
        try:
            engine = get_engine(instance=instance)
            query_result = engine.get_long_transaction()
        except AttributeError:
            raise serializers.ValidationError(
                {
                    "errors": f"{instance.db_type} does not support transaction diagnostics."
                }
            )
        except Exception as exc:
            logger.exception(
                "Failed to list transactions for instance_id=%s", instance.id
            )
            raise serializers.ValidationError({"errors": str(exc)})
        finally:
            if "engine" in locals():
                engine.close()

        if query_result.error:
            raise serializers.ValidationError({"errors": query_result.error})
        rows = query_result.to_dict()
        return success_response(
            data=InstanceDiagnosticListSerializer(
                {"count": len(rows), "results": rows}
            ).data
        )


class InstanceOperationDiagnosticLocks(views.APIView):
    """List lock waits."""

    @extend_schema(
        summary="Instance Operation Locks",
        responses={200: InstanceDiagnosticListSerializer},
        description="List lock waits for supported instances.",
    )
    @method_decorator(permission_required("sql.trxandlocks_view", raise_exception=True))
    def get(self, request):
        instance_id = request.query_params.get("instance_id")
        if not instance_id:
            return success_response(data={"count": 0, "results": []})

        instance = _operation_diagnostic_instance(request.user, instance_id)
        try:
            engine = get_engine(instance=instance)
            if instance.db_type == "mysql":
                query_result = engine.trxandlocks()
            elif instance.db_type == "oracle":
                query_result = engine.lock_info()
            else:
                raise serializers.ValidationError(
                    {"errors": f"{instance.db_type} does not support lock diagnostics."}
                )
        except serializers.ValidationError:
            raise
        except Exception as exc:
            logger.exception("Failed to list locks for instance_id=%s", instance.id)
            raise serializers.ValidationError({"errors": str(exc)})
        finally:
            if "engine" in locals():
                engine.close()

        if query_result.error:
            raise serializers.ValidationError({"errors": query_result.error})
        rows = query_result.to_dict()
        return success_response(
            data=InstanceDiagnosticListSerializer(
                {"count": len(rows), "results": rows}
            ).data
        )


class InstanceTagList(generics.ListAPIView):
    """List and create instance tags for inventory management."""

    pagination_class = CustomizedPagination
    serializer_class = InstanceTagManagementSerializer
    queryset = InstanceTag.objects.all().order_by("id")

    def get_queryset(self):
        queryset = super().get_queryset()
        search = self.request.query_params.get("search", "").strip()
        ordering = self.request.query_params.get("ordering", "").strip()

        if search:
            search_filter = Q(tag_code__icontains=search) | Q(
                tag_name__icontains=search
            )
            if search.isdigit():
                search_filter |= Q(id=int(search))
            queryset = queryset.filter(search_filter)

        if ordering in {
            "id",
            "-id",
            "tag_code",
            "-tag_code",
            "tag_name",
            "-tag_name",
            "active",
            "-active",
        }:
            queryset = queryset.order_by(ordering, "id")

        return queryset

    @extend_schema(
        summary="Instance Tag List",
        responses={200: InstanceTagManagementSerializer},
        parameters=[
            OpenApiParameter(
                name="search",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY,
                description="Match tag ID, code, or name.",
            ),
            OpenApiParameter(
                name="ordering",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY,
                description="Ordering key, e.g. tag_name or -active.",
            ),
        ],
        description="List instance tags available to inventory administrators.",
    )
    @method_decorator(permission_required("sql.menu_instance", raise_exception=True))
    def get(self, request):
        tags = self.filter_queryset(self.get_queryset())
        page_tags = self.paginate_queryset(queryset=tags)
        serializer_obj = self.get_serializer(page_tags, many=True)
        return self.get_paginated_response(serializer_obj.data)

    @extend_schema(
        summary="Create Instance Tag",
        request=InstanceTagCreateSerializer,
        responses={201: InstanceTagManagementSerializer},
        description="Create a new instance tag.",
    )
    @method_decorator(permission_required("sql.menu_instance", raise_exception=True))
    def post(self, request):
        serializer = InstanceTagCreateSerializer(data=request.data)
        if serializer.is_valid():
            tag = serializer.save()
            return success_response(
                data=InstanceTagManagementSerializer(tag).data,
                detail="Instance tag created successfully.",
                status_code=status.HTTP_201_CREATED,
            )
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class InstanceTagDetail(views.APIView):
    """Get and update a single instance tag."""

    serializer_class = InstanceTagManagementSerializer

    def get_object(self, pk):
        try:
            return InstanceTag.objects.get(pk=pk)
        except InstanceTag.DoesNotExist:
            raise Http404

    @staticmethod
    def _validate_deactivation(tag, next_active):
        if next_active or tag.active is False:
            return
        if tag.instance_set.exists():
            raise serializers.ValidationError(
                {
                    "active": (
                        "This tag is assigned to one or more instances. "
                        "Remove it from those instances before deactivating it."
                    )
                }
            )

    @extend_schema(
        summary="Instance Tag Detail",
        responses={200: InstanceTagManagementSerializer},
        description="Get a single instance tag for editing.",
    )
    @method_decorator(permission_required("sql.menu_instance", raise_exception=True))
    def get(self, request, pk):
        tag = self.get_object(pk)
        return success_response(data=InstanceTagManagementSerializer(tag).data)

    @extend_schema(
        summary="Update Instance Tag",
        request=InstanceTagUpdateSerializer,
        responses={200: InstanceTagManagementSerializer},
        description="Update an instance tag. Tag code remains immutable.",
    )
    @method_decorator(permission_required("sql.menu_instance", raise_exception=True))
    def put(self, request, pk):
        tag = self.get_object(pk)
        serializer = InstanceTagUpdateSerializer(tag, data=request.data)
        if serializer.is_valid():
            self._validate_deactivation(
                tag, serializer.validated_data.get("active", True)
            )
            serializer.save()
            return success_response(
                data=InstanceTagManagementSerializer(tag).data,
                detail="Instance tag updated successfully.",
            )
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class InstanceConnectionTest(views.APIView):
    """Check whether a configured instance is reachable."""

    @extend_schema(
        summary="Test Instance Connection",
        responses={200: InstanceConnectionTestResultSerializer},
        description="Run a connection test for an instance. Restricted to superusers to match legacy frontend behavior.",
    )
    def post(self, request, pk):
        if not request.user.is_superuser:
            raise PermissionDenied("Only superusers can test instance connections.")

        try:
            instance = Instance.objects.get(pk=pk)
        except Instance.DoesNotExist:
            raise Http404

        try:
            query_engine = get_engine(instance=instance)
            test_result = query_engine.test_connection()
        except serializers.ValidationError:
            raise
        except Exception:
            logger.exception("Failed instance connection test for instance_id=%s", pk)
            raise serializers.ValidationError(
                {"errors": "Unable to connect to instance. Check configuration."}
            )

        if test_result.error:
            raise serializers.ValidationError(
                {"errors": "Unable to connect to instance. Check configuration."}
            )

        payload = InstanceConnectionTestResultSerializer(
            {"success": True, "message": "Connection successful."}
        ).data
        return success_response(data=payload, detail="Connection successful.")


class InstanceDraftConnectionTest(views.APIView):
    """Check whether an unsaved instance configuration is reachable."""

    @extend_schema(
        summary="Test Draft Instance Connection",
        request=InstanceConnectionTestRequestSerializer,
        responses={200: InstanceConnectionTestResultSerializer},
        description="Validate draft instance connection settings without creating an instance record.",
    )
    @method_decorator(permission_required("sql.menu_instance", raise_exception=True))
    def post(self, request):
        serializer = InstanceConnectionTestRequestSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        instance = serializer.build_instance()

        try:
            query_engine = get_engine(instance=instance)
            test_result = query_engine.test_connection()
        except serializers.ValidationError:
            raise
        except Exception:
            logger.exception("Failed draft instance connection test")
            raise serializers.ValidationError(
                {"errors": "Unable to connect to instance. Check configuration."}
            )

        if test_result.error:
            raise serializers.ValidationError(
                {"errors": "Unable to connect to instance. Check configuration."}
            )

        payload = InstanceConnectionTestResultSerializer(
            {"success": True, "message": "Connection successful."}
        ).data
        return success_response(data=payload, detail="Connection successful.")


class InstanceResource(views.APIView):
    """
    Get resource information inside an instance: database, schema, table, column.
    """

    @extend_schema(
        summary="Instance Resources",
        responses={200: InstanceResourceListSerializer},
        parameters=[
            OpenApiParameter(
                name="instance_id",
                type=OpenApiTypes.INT,
                location=OpenApiParameter.QUERY,
                required=True,
                description="Instance ID.",
            ),
            OpenApiParameter(
                name="resource_type",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY,
                required=True,
                description="Resource type: database, schema, table, column.",
            ),
            OpenApiParameter(
                name="db_name",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY,
                description="Database name.",
            ),
            OpenApiParameter(
                name="schema_name",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY,
                description="Schema name.",
            ),
            OpenApiParameter(
                name="tb_name",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY,
                description="Table name.",
            ),
        ],
        description="Get resource information inside an instance.",
    )
    def get(self, request):
        serializer = InstanceResourceSerializer(data=request.query_params)
        serializer.is_valid(raise_exception=True)

        data = serializer.validated_data
        instance_id = data["instance_id"]
        resource_type = data["resource_type"]
        db_name = data.get("db_name", "")
        schema_name = data.get("schema_name", "")
        tb_name = data.get("tb_name", "")
        if not user_instances(request.user).filter(id=instance_id).exists():
            raise serializers.ValidationError(
                {"errors": "The instance is not associated with your group."}
            )
        instance = Instance.objects.get(pk=instance_id)

        try:
            query_engine = get_engine(instance=instance)
            db_name = query_engine.escape_string(db_name)
            schema_name = query_engine.escape_string(schema_name)
            tb_name = query_engine.escape_string(tb_name)
            if resource_type == "database":
                resource = query_engine.get_all_databases()
                resource.rows = filter_db_list(
                    db_list=resource.rows,
                    db_name_regex=query_engine.instance.show_db_name_regex,
                    is_match_regex=True,
                )
                resource.rows = filter_db_list(
                    db_list=resource.rows,
                    db_name_regex=query_engine.instance.denied_db_name_regex,
                    is_match_regex=False,
                )
            elif resource_type == "schema" and db_name:
                resource = query_engine.get_all_schemas(db_name=db_name)
            elif resource_type == "table" and db_name:
                resource = query_engine.get_all_tables(
                    db_name=db_name, schema_name=schema_name
                )
            elif resource_type == "column" and db_name and tb_name:
                resource = query_engine.get_all_columns_by_tb(
                    db_name=db_name, tb_name=tb_name, schema_name=schema_name
                )
            else:
                raise serializers.ValidationError(
                    {"errors": "Unsupported resource type or incomplete parameters."}
                )
        except Exception as msg:
            raise serializers.ValidationError({"errors": str(msg)})
        else:
            if resource.error:
                raise serializers.ValidationError({"errors": resource.error})
            resource = {"count": len(resource.rows), "result": resource.rows}
            serializer_obj = InstanceResourceListSerializer(resource)
            return success_response(data=serializer_obj.data)
