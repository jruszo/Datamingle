import re

from django.db import transaction
from django.db.models import Count, Prefetch, Q
from django.shortcuts import get_object_or_404
from django.utils import timezone
from rest_framework import generics, permissions, serializers, status, views
from rest_framework.exceptions import PermissionDenied, ValidationError

from api_agents.dispatch import send_agent_message
from api_agents.models import AgentNodeAssignment
from api_agents.models import AgentStatus
from api_agents.services import (
    has_active_agent_websocket,
)
from api_core.pagination import CustomizedPagination
from api_core.response import success_response
from api_infrastructure.serializers import (
    DatabaseServiceSerializer,
    DatabaseServiceWriteSerializer,
    InfrastructureNodeRemoteManagerRecordSerializer,
    InfrastructureNodeRemoteManagerSerializer,
    InfrastructureNodeSerializer,
    InfrastructureNodeWriteSerializer,
    MysqlClusterSerializer,
    RecommendationStatusSerializer,
    ServiceRecommendationSerializer,
)
from sql.models import (
    InfrastructureNode,
    Instance,
    MysqlCluster,
    MysqlTopologyAlert,
    ServiceRecommendation,
)
from sql.inventory import refresh_instance_inventory_snapshot
from sql.utils.team import user_groups, user_instances

INFRASTRUCTURE_MENU_PERMISSIONS = (
    "sql.menu_infrastructure",
    "sql.menu_instance",
    "sql.menu_instance_list",
    "api_agents.menu_agent",
)
MONITORING_LABEL_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
MAX_MONITORING_LABEL_FILTER_VALUES = 32
MAX_MONITORING_LABEL_VALUE_LENGTH = 256


def _has_permission(user, permission):
    return bool(user and user.is_authenticated and user.has_perm(permission))


def _require_any_permission(request, *permissions_list):
    if request.user.is_superuser:
        return
    if any(
        _has_permission(request.user, permission) for permission in permissions_list
    ):
        return
    raise PermissionDenied(
        f"Missing required permission. Need one of: {', '.join(permissions_list)}"
    )


def request_can_manage_infrastructure(request):
    return bool(
        request.user.is_superuser
        or _has_permission(request.user, "sql.menu_infrastructure")
        or _has_permission(request.user, "sql.menu_instance")
    )


def _require_manage_infrastructure(request):
    if request_can_manage_infrastructure(request):
        return
    raise PermissionDenied(
        "Missing required permission. Need sql.menu_infrastructure or sql.menu_instance."
    )


def _parse_monitoring_label_filters(request):
    filters = []
    for parameter, values in request.query_params.lists():
        if parameter.startswith("lf."):
            mode = "include"
        elif parameter.startswith("lx."):
            mode = "exclude"
        else:
            continue
        label = parameter[3:]
        if not MONITORING_LABEL_NAME_RE.fullmatch(label):
            raise ValidationError({"labels": f'"{label}" is not a valid label name.'})
        oversized_values = [
            value.strip()
            for value in values
            if len(value.strip()) > MAX_MONITORING_LABEL_VALUE_LENGTH
        ]
        if oversized_values:
            raise ValidationError(
                {
                    "labels": (
                        f'Values for label "{label}" must be '
                        f"{MAX_MONITORING_LABEL_VALUE_LENGTH} characters or fewer."
                    )
                }
            )
        normalized_values = sorted({value.strip() for value in values if value.strip()})
        if len(normalized_values) > MAX_MONITORING_LABEL_FILTER_VALUES:
            raise ValidationError(
                {
                    "labels": (
                        f"At most {MAX_MONITORING_LABEL_FILTER_VALUES} values are "
                        f'allowed for label "{label}".'
                    )
                }
            )
        if normalized_values:
            filters.append((label, mode, normalized_values))
    return filters


def _apply_monitoring_label_filters(queryset, filters):
    for label, mode, values in filters:
        lookup = {f"monitoring_labels__{label}__in": values}
        if mode == "include":
            queryset = queryset.filter(**lookup)
            continue
        has_label = Q(monitoring_labels__has_key=label)
        queryset = queryset.filter(~has_label | (has_label & ~Q(**lookup)))
    return queryset


def _active_node_agent(node):
    local_agents = (
        node.local_agents.filter(enabled=True, status=AgentStatus.ONLINE)
        .exclude(status=AgentStatus.REVOKED)
        .order_by("-last_seen_at", "name", "id")
    )
    for agent in local_agents:
        if has_active_agent_websocket(agent):
            return agent

    assignments = (
        node.agent_assignments.filter(
            enabled=True,
            command_enabled=True,
            agent__enabled=True,
            agent__status=AgentStatus.ONLINE,
        )
        .select_related("agent")
        .order_by("-agent__last_seen_at", "agent_id")
    )
    for assignment in assignments:
        if has_active_agent_websocket(assignment.agent):
            return assignment.agent
    return None


class InfrastructureNodeListCreateView(generics.ListAPIView):
    permission_classes = [permissions.IsAuthenticated]
    pagination_class = CustomizedPagination
    serializer_class = InfrastructureNodeSerializer

    def get_visible_services(self):
        return (
            user_instances(self.request.user)
            .select_related("node", "mysql_cluster")
            .prefetch_related("resource_group")
        )

    def get_serializer_context(self):
        context = super().get_serializer_context()
        context["visible_service_ids"] = list(
            self.get_visible_services().values_list("id", flat=True)
        )
        return context

    def get_queryset(self):
        visible_services = self.get_visible_services()
        visible_service_ids = visible_services.values("id")
        queryset = (
            InfrastructureNode.objects.annotate(
                service_count=Count(
                    "services",
                    filter=Q(services__id__in=visible_service_ids),
                    distinct=True,
                ),
                recommendation_count=Count(
                    "service_recommendations",
                    filter=Q(
                        service_recommendations__status=(
                            ServiceRecommendation.STATUS_RECOMMENDED
                        )
                    ),
                    distinct=True,
                ),
            )
            .prefetch_related(
                "resource_group",
                "local_agents",
                "agent_assignments__agent",
                "service_recommendations",
                Prefetch(
                    "services",
                    queryset=visible_services.order_by("instance_name", "id"),
                    to_attr="visible_services",
                ),
            )
            .order_by("name", "id")
        )
        if not request_can_manage_infrastructure(self.request):
            visible_teams = user_groups(self.request.user)
            queryset = queryset.filter(
                Q(resource_group__in=visible_teams)
                | Q(services__id__in=visible_service_ids)
            ).distinct()
        search = self.request.query_params.get("search", "").strip()
        if search:
            visible_service_search = Q(services__id__in=visible_service_ids) & (
                Q(services__instance_name__icontains=search)
                | Q(services__host__icontains=search)
            )
            queryset = queryset.filter(
                Q(name__icontains=search)
                | Q(address__icontains=search)
                | Q(description__icontains=search)
                | visible_service_search
                | Q(local_agents__name__icontains=search)
                | Q(local_agents__hostname__icontains=search)
            ).distinct()
        queryset = _apply_monitoring_label_filters(
            queryset, _parse_monitoring_label_filters(self.request)
        )
        return queryset

    def get(self, request):
        _require_any_permission(request, *INFRASTRUCTURE_MENU_PERMISSIONS)
        return super().get(request)

    def post(self, request):
        _require_manage_infrastructure(request)
        serializer = InfrastructureNodeWriteSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        node = serializer.save()
        return success_response(
            data=InfrastructureNodeSerializer(
                node,
                context={
                    "visible_service_ids": list(
                        self.get_visible_services().values_list("id", flat=True)
                    )
                },
            ).data,
            detail="Infrastructure node created.",
            status_code=status.HTTP_201_CREATED,
        )


class InfrastructureNodeLabelNamesView(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request):
        _require_any_permission(request, *INFRASTRUCTURE_MENU_PERMISSIONS)
        nodes = InfrastructureNodeListCreateView()
        nodes.request = request
        names = set()
        for labels in nodes.get_queryset().values_list("monitoring_labels", flat=True):
            names.update((labels or {}).keys())
        return success_response(data=sorted(names))


class InfrastructureNodeLabelValuesView(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request, label_name):
        _require_any_permission(request, *INFRASTRUCTURE_MENU_PERMISSIONS)
        if not MONITORING_LABEL_NAME_RE.fullmatch(label_name):
            raise ValidationError({"label": "Invalid label name."})
        nodes = InfrastructureNodeListCreateView()
        nodes.request = request
        values = {
            str(labels[label_name])
            for labels in nodes.get_queryset().values_list(
                "monitoring_labels", flat=True
            )
            if label_name in (labels or {})
        }
        return success_response(data=sorted(values))


class MysqlClusterListView(generics.ListAPIView):
    permission_classes = [permissions.IsAuthenticated]
    pagination_class = CustomizedPagination
    serializer_class = MysqlClusterSerializer

    def get_queryset(self):
        return mysql_cluster_queryset_for_request(self.request)

    def get(self, request):
        _require_any_permission(request, *INFRASTRUCTURE_MENU_PERMISSIONS)
        return super().get(request)


def mysql_cluster_queryset_for_request(request):
    visible_service_ids = user_instances(request.user, db_type=["mysql"]).values("id")
    queryset = MysqlCluster.objects.select_related("primary_instance")
    member_count = Count("instances", distinct=True)
    active_alert_filter = Q(alerts__status=MysqlTopologyAlert.STATUS_ACTIVE)
    active_alerts = MysqlTopologyAlert.objects.filter(
        status=MysqlTopologyAlert.STATUS_ACTIVE
    )
    if not request.user.is_superuser:
        queryset = queryset.filter(instances__id__in=visible_service_ids).distinct()
        member_count = Count(
            "instances",
            filter=Q(instances__id__in=visible_service_ids),
            distinct=True,
        )
        active_alert_filter &= Q(alerts__instance_id__in=visible_service_ids)
        active_alerts = active_alerts.filter(instance_id__in=visible_service_ids)
    return (
        queryset.annotate(
            member_count=member_count,
            active_alert_count=Count(
                "alerts",
                filter=active_alert_filter,
                distinct=True,
            ),
        )
        .prefetch_related(
            Prefetch(
                "alerts",
                queryset=active_alerts.select_related("instance"),
                to_attr="active_alert_records",
            )
        )
        .order_by("name", "id")
    )


class MysqlClusterDetailView(generics.RetrieveUpdateAPIView):
    permission_classes = [permissions.IsAuthenticated]
    serializer_class = MysqlClusterSerializer
    lookup_url_kwarg = "cluster_id"

    def get_queryset(self):
        return mysql_cluster_queryset_for_request(self.request)

    def retrieve(self, request, *args, **kwargs):
        _require_any_permission(request, *INFRASTRUCTURE_MENU_PERMISSIONS)
        serializer = self.get_serializer(self.get_object())
        return success_response(data=serializer.data)

    def patch(self, request, *args, **kwargs):
        _require_manage_infrastructure(request)
        cluster = self.get_object()
        serializer = self.get_serializer(cluster, data=request.data, partial=True)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return success_response(data=serializer.data)


class InfrastructureNodeDetailView(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    def get_object(self, node_id):
        return get_object_or_404(InfrastructureNode, pk=node_id)

    def get_visible_services(self, request):
        return user_instances(request.user)

    def get_visible_service_ids(self, request):
        return list(self.get_visible_services(request).values_list("id", flat=True))

    def ensure_node_visible(self, request, node):
        if request_can_manage_infrastructure(request):
            return
        visible_teams = user_groups(request.user)
        if node.resource_group.filter(
            team_id__in=[team.team_id for team in visible_teams]
        ).exists():
            return
        if self.get_visible_services(request).filter(node=node).exists():
            return
        raise PermissionDenied("You do not have access to this infrastructure node.")

    def get(self, request, node_id):
        _require_any_permission(request, *INFRASTRUCTURE_MENU_PERMISSIONS)
        node = self.get_object(node_id)
        self.ensure_node_visible(request, node)
        return success_response(
            data=InfrastructureNodeSerializer(
                node,
                context={"visible_service_ids": self.get_visible_service_ids(request)},
            ).data
        )

    def patch(self, request, node_id):
        _require_manage_infrastructure(request)
        node = self.get_object(node_id)
        serializer = InfrastructureNodeWriteSerializer(
            node, data=request.data, partial=True
        )
        serializer.is_valid(raise_exception=True)
        node = serializer.save()
        return success_response(
            data=InfrastructureNodeSerializer(
                node,
                context={"visible_service_ids": self.get_visible_service_ids(request)},
            ).data
        )


class InfrastructureServiceCreateView(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    def post(self, request):
        _require_manage_infrastructure(request)
        serializer = DatabaseServiceWriteSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        service = serializer.save()
        return success_response(
            data=DatabaseServiceSerializer(service).data,
            detail="Service created.",
            status_code=status.HTTP_201_CREATED,
        )


class InfrastructureServiceDetailView(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    def get_object(self, service_id):
        return get_object_or_404(
            Instance.objects.select_related("node", "mysql_cluster").prefetch_related(
                "resource_group"
            ),
            pk=service_id,
        )

    def patch(self, request, service_id):
        _require_manage_infrastructure(request)
        service = self.get_object(service_id)
        serializer = DatabaseServiceWriteSerializer(
            service, data=request.data, partial=True
        )
        serializer.is_valid(raise_exception=True)
        service = serializer.save()
        return success_response(data=DatabaseServiceSerializer(service).data)


class InfrastructureServiceConnectionTestView(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    def post(self, request, service_id):
        _require_manage_infrastructure(request)
        service = get_object_or_404(Instance, pk=service_id)
        result = refresh_instance_inventory_snapshot(instance=service)
        if not result["success"]:
            raise serializers.ValidationError(
                {"errors": result.get("error") or "Agent inventory collection failed."}
            )
        return success_response(
            data={"message": "Connection successful and inventory refreshed."},
            detail="Agent inventory collection completed.",
        )


class InfrastructureNodeDiscoverView(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    def post(self, request, node_id):
        _require_manage_infrastructure(request)
        node = get_object_or_404(InfrastructureNode, pk=node_id)
        agent = _active_node_agent(node)
        if agent is None:
            raise serializers.ValidationError(
                {
                    "errors": "No online websocket-connected agent is assigned to this node."
                }
            )

        delivered = send_agent_message(
            agent.id,
            {
                "type": "infrastructure.discover",
                "node_id": node.id,
                "node": {
                    "id": node.id,
                    "name": node.name,
                    "address": node.address,
                    "metadata": node.metadata,
                },
            },
            agent=agent,
        )
        if not delivered:
            raise serializers.ValidationError(
                {"errors": "Agent websocket is not connected."}
            )
        return success_response(
            data={"agent_id": agent.id},
            detail="Discovery requested.",
            status_code=status.HTTP_202_ACCEPTED,
        )


class ServiceRecommendationDetailView(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    def patch(self, request, recommendation_id):
        _require_manage_infrastructure(request)
        recommendation = get_object_or_404(ServiceRecommendation, pk=recommendation_id)
        serializer = RecommendationStatusSerializer(
            recommendation, data=request.data, partial=True
        )
        serializer.is_valid(raise_exception=True)
        recommendation = serializer.save()
        return success_response(
            data=ServiceRecommendationSerializer(recommendation).data
        )


class InfrastructureNodeRemoteManagerView(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    def get_object(self, node_id):
        return get_object_or_404(InfrastructureNode, pk=node_id)

    def put(self, request, node_id):
        _require_any_permission(request, "api_agents.menu_agent")
        node = self.get_object(node_id)
        serializer = InfrastructureNodeRemoteManagerSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data
        agent = data.pop("agent")

        with transaction.atomic():
            for assignment in node.agent_assignments.select_for_update().exclude(
                agent=agent
            ):
                assignment.delete()

            assignment = (
                AgentNodeAssignment.objects.select_for_update()
                .filter(agent=agent, node=node)
                .first()
            )
            if assignment is None:
                assignment = AgentNodeAssignment(agent=agent, node=node)
            assignment.enabled = True
            for field, value in data.items():
                setattr(assignment, field, value)
            assignment.save()

        return success_response(
            data=InfrastructureNodeRemoteManagerRecordSerializer(assignment).data,
            detail="Remote manager assigned.",
        )

    def delete(self, request, node_id):
        _require_any_permission(request, "api_agents.menu_agent")
        node = self.get_object(node_id)
        with transaction.atomic():
            for assignment in node.agent_assignments.select_for_update():
                assignment.delete()
        return success_response(detail="Remote manager cleared.")
