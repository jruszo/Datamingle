from django.conf import settings
from django.contrib.auth.decorators import permission_required
from django.db.models import Q
from django.http import Http404
from django.utils.decorators import method_decorator
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiTypes
from rest_framework import generics, serializers, status, views
from rest_framework.exceptions import PermissionDenied
from rest_framework.response import Response

from sql.engines import engine_map, get_engine
from sql.models import AliyunRdsConfig, Instance, InstanceTag, ResourceGroup, Tunnel
from sql.utils.resource_group import user_instances
from sql.utils.sql_utils import filter_db_list

from .pagination import CustomizedPagination
from .response import success_response
from .serializers import (
    AliyunRdsSerializer,
    ChoiceOptionSerializer,
    InstanceConnectionTestResultSerializer,
    InstanceConnectionTestRequestSerializer,
    InstanceCreateSerializer,
    InstanceDetailSerializer,
    InstanceEditorSerializer,
    InstanceListSerializer,
    InstanceMetadataSerializer,
    InstanceResourceListSerializer,
    InstanceResourceSerializer,
    InstanceTagLookupSerializer,
    ResourceGroupLookupSerializer,
    TunnelLookupSerializer,
    TunnelSerializer,
)


def _require_any_permission(request, *perm_list):
    if request.user.is_superuser:
        return
    if any(request.user.has_perm(perm) for perm in perm_list):
        return
    raise PermissionDenied(
        f"Missing required permission. Need one of: {', '.join(perm_list)}"
    )


class InstanceList(generics.ListAPIView):
    """
    List all instances or create a new instance configuration.
    """

    pagination_class = CustomizedPagination
    serializer_class = InstanceListSerializer
    queryset = Instance.objects.all().select_related("tunnel").order_by("id")

    def get_queryset(self):
        queryset = (
            super()
            .get_queryset()
            .prefetch_related("instance_tag", "resource_group")
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
        description="List available instance types, enabled database types, active tags, tunnels, and resource groups.",
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
            "tunnels": Tunnel.objects.all().order_by("tunnel_name", "id"),
            "resource_groups": ResourceGroup.objects.filter(is_deleted=0).order_by(
                "group_name", "group_id"
            ),
        }
        serializer = InstanceMetadataSerializer(payload)
        return success_response(data=serializer.data)


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
        except Exception as exc:
            raise serializers.ValidationError(
                {"errors": f"Unable to connect to instance. {str(exc)}"}
            )

        if test_result.error:
            raise serializers.ValidationError(
                {"errors": f"Unable to connect to instance. {test_result.error}"}
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
        except Exception as exc:
            raise serializers.ValidationError(
                {"errors": f"Unable to connect to instance. {str(exc)}"}
            )

        if test_result.error:
            raise serializers.ValidationError(
                {"errors": f"Unable to connect to instance. {test_result.error}"}
            )

        payload = InstanceConnectionTestResultSerializer(
            {"success": True, "message": "Connection successful."}
        ).data
        return success_response(data=payload, detail="Connection successful.")


class TunnelList(generics.ListAPIView):
    """
    List all tunnels or create a new tunnel configuration.
    """

    pagination_class = CustomizedPagination
    serializer_class = TunnelSerializer
    queryset = Tunnel.objects.all().order_by("id")

    @extend_schema(
        summary="Tunnel List",
        request=TunnelSerializer,
        responses={200: TunnelSerializer},
        description="List all tunnels (filtering, pagination).",
    )
    @method_decorator(permission_required("sql.menu_instance", raise_exception=True))
    def get(self, request):
        tunnels = self.filter_queryset(self.queryset)
        page_tunnels = self.paginate_queryset(queryset=tunnels)
        serializer_obj = self.get_serializer(page_tunnels, many=True)
        return self.get_paginated_response(serializer_obj.data)

    @extend_schema(
        summary="Create Tunnel",
        request=TunnelSerializer,
        responses={201: TunnelSerializer},
        description="Create a tunnel configuration.",
    )
    @method_decorator(permission_required("sql.menu_instance", raise_exception=True))
    def post(self, request):
        serializer = TunnelSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return success_response(
                data=serializer.data, status_code=status.HTTP_201_CREATED
            )
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class AliyunRdsList(generics.ListAPIView):
    """
    List all Aliyun RDS configs or create a new one.
    """

    pagination_class = CustomizedPagination
    serializer_class = AliyunRdsSerializer
    queryset = AliyunRdsConfig.objects.all().select_related("ak").order_by("id")

    @extend_schema(
        summary="Aliyun RDS List",
        request=AliyunRdsSerializer,
        responses={200: AliyunRdsSerializer},
        description="List all Aliyun RDS configs (filtering, pagination).",
    )
    @method_decorator(permission_required("sql.menu_instance", raise_exception=True))
    def get(self, request):
        aliyunrds = self.filter_queryset(self.queryset)
        page_rds = self.paginate_queryset(queryset=aliyunrds)
        serializer_obj = self.get_serializer(page_rds, many=True)
        return self.get_paginated_response(serializer_obj.data)

    @extend_schema(
        summary="Create Aliyun RDS",
        request=AliyunRdsSerializer,
        responses={201: AliyunRdsSerializer},
        description="Create an Aliyun RDS configuration (including a CloudAccessKey).",
    )
    @method_decorator(permission_required("sql.menu_instance", raise_exception=True))
    def post(self, request):
        serializer = AliyunRdsSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return success_response(
                data=serializer.data, status_code=status.HTTP_201_CREATED
            )
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


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
