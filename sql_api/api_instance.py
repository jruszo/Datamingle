from rest_framework import views, generics, status, serializers
from rest_framework.response import Response
from drf_spectacular.utils import extend_schema
from sql.utils.sql_utils import filter_db_list
from .serializers import (
    InstanceSerializer,
    InstanceDetailSerializer,
    TunnelSerializer,
    AliyunRdsSerializer,
    InstanceResourceSerializer,
    InstanceResourceListSerializer,
)
from .pagination import CustomizedPagination
from .filters import InstanceFilter
from sql.models import Instance, Tunnel, AliyunRdsConfig
from sql.engines import get_engine
from django.http import Http404
import MySQLdb


class InstanceList(generics.ListAPIView):
    """
    List all instances or create a new instance configuration.
    """

    filterset_class = InstanceFilter
    pagination_class = CustomizedPagination
    serializer_class = InstanceSerializer
    queryset = Instance.objects.all().order_by("id")

    @extend_schema(
        summary="Instance List",
        request=InstanceSerializer,
        responses={200: InstanceSerializer},
        description="List all instances (filtering, pagination).",
    )
    def get(self, request):
        instances = self.filter_queryset(self.queryset)
        page_ins = self.paginate_queryset(queryset=instances)
        serializer_obj = self.get_serializer(page_ins, many=True)
        data = {"data": serializer_obj.data}
        return self.get_paginated_response(data)

    @extend_schema(
        summary="Create Instance",
        request=InstanceSerializer,
        responses={201: InstanceSerializer},
        description="Create an instance configuration.",
    )
    def post(self, request):
        serializer = InstanceSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class InstanceDetail(views.APIView):
    """
    Instance operations.
    """

    serializer_class = InstanceDetailSerializer

    def get_object(self, pk):
        try:
            return Instance.objects.get(pk=pk)
        except Instance.DoesNotExist:
            raise Http404

    @extend_schema(
        summary="Update Instance",
        request=InstanceDetailSerializer,
        responses={200: InstanceDetailSerializer},
        description="Update an instance configuration.",
    )
    def put(self, request, pk):
        instance = self.get_object(pk)
        serializer = InstanceDetailSerializer(instance, data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    @extend_schema(summary="Delete Instance", description="Delete an instance configuration.")
    def delete(self, request, pk):
        instance = self.get_object(pk)
        instance.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)


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
    def get(self, request):
        tunnels = self.filter_queryset(self.queryset)
        page_tunnels = self.paginate_queryset(queryset=tunnels)
        serializer_obj = self.get_serializer(page_tunnels, many=True)
        data = {"data": serializer_obj.data}
        return self.get_paginated_response(data)

    @extend_schema(
        summary="Create Tunnel",
        request=TunnelSerializer,
        responses={201: TunnelSerializer},
        description="Create a tunnel configuration.",
    )
    def post(self, request):
        serializer = TunnelSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
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
    def get(self, request):
        aliyunrds = self.filter_queryset(self.queryset)
        page_rds = self.paginate_queryset(queryset=aliyunrds)
        serializer_obj = self.get_serializer(page_rds, many=True)
        data = {"data": serializer_obj.data}
        return self.get_paginated_response(data)

    @extend_schema(
        summary="Create Aliyun RDS",
        request=AliyunRdsSerializer,
        responses={201: AliyunRdsSerializer},
        description="Create an Aliyun RDS configuration (including a CloudAccessKey).",
    )
    def post(self, request):
        serializer = AliyunRdsSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class InstanceResource(views.APIView):
    """
    Get resource information inside an instance: database, schema, table, column.
    """

    @extend_schema(
        summary="Instance Resources",
        request=InstanceResourceSerializer,
        responses={200: InstanceResourceListSerializer},
        description="Get resource information inside an instance.",
    )
    def post(self, request):
        # Parameter validation
        serializer = InstanceResourceSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        instance_id = request.data["instance_id"]
        resource_type = request.data["resource_type"]
        db_name = request.data["db_name"] if "db_name" in request.data.keys() else ""
        schema_name = (
            request.data["schema_name"] if "schema_name" in request.data.keys() else ""
        )
        tb_name = request.data["tb_name"] if "tb_name" in request.data.keys() else ""
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
            raise serializers.ValidationError({"errors": msg})
        else:
            if resource.error:
                raise serializers.ValidationError({"errors": resource.error})
            else:
                resource = {"count": len(resource.rows), "result": resource.rows}
                serializer_obj = InstanceResourceListSerializer(resource)
                return Response(serializer_obj.data)
