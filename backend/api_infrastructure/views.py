from django.db import transaction
from django.db.models import Count, Prefetch, Q
from django.shortcuts import get_object_or_404
from rest_framework import generics, permissions, status, views
from rest_framework.exceptions import PermissionDenied

from api_agents.models import AgentNodeAssignment
from api_core.pagination import CustomizedPagination
from api_core.response import success_response
from api_infrastructure.serializers import (
    InfrastructureNodeRemoteManagerRecordSerializer,
    InfrastructureNodeRemoteManagerSerializer,
    InfrastructureNodeSerializer,
    InfrastructureNodeWriteSerializer,
)
from sql.models import InfrastructureNode
from sql.utils.resource_group import user_instances

INFRASTRUCTURE_MENU_PERMISSIONS = (
    "sql.menu_instance",
    "sql.menu_instance_list",
    "sql.menu_database",
    "api_agents.menu_agent",
)


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


def _require_permission(request, permission):
    if request.user.is_superuser or _has_permission(request.user, permission):
        return
    raise PermissionDenied(f"Missing required permission: {permission}")


def request_can_manage_infrastructure(request):
    return bool(
        request.user.is_superuser
        or _has_permission(request.user, "sql.menu_instance")
        or _has_permission(request.user, "api_agents.menu_agent")
    )


class InfrastructureNodeListCreateView(generics.ListAPIView):
    permission_classes = [permissions.IsAuthenticated]
    pagination_class = CustomizedPagination
    serializer_class = InfrastructureNodeSerializer

    def get_visible_services(self):
        return user_instances(self.request.user).select_related("node")

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
                )
            )
            .prefetch_related(
                Prefetch(
                    "services",
                    queryset=visible_services.order_by("instance_name", "id"),
                    to_attr="visible_services",
                ),
                "local_agents",
                "agent_assignments__agent",
            )
            .order_by("node_name", "id")
        )
        if not request_can_manage_infrastructure(self.request):
            queryset = queryset.filter(services__id__in=visible_service_ids).distinct()
        search = self.request.query_params.get("search", "").strip()
        if search:
            visible_service_search = Q(services__id__in=visible_service_ids) & (
                Q(services__instance_name__icontains=search)
                | Q(services__host__icontains=search)
            )
            queryset = queryset.filter(
                Q(node_name__icontains=search)
                | Q(hostname__icontains=search)
                | Q(environment__icontains=search)
                | Q(provider__icontains=search)
                | visible_service_search
                | Q(local_agents__name__icontains=search)
                | Q(local_agents__hostname__icontains=search)
            ).distinct()
        return queryset

    def get(self, request):
        _require_any_permission(request, *INFRASTRUCTURE_MENU_PERMISSIONS)
        return super().get(request)

    def post(self, request):
        _require_permission(request, "sql.menu_instance")
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
        _require_permission(request, "sql.menu_instance")
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


class InfrastructureNodeRemoteManagerView(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    def get_object(self, node_id):
        return get_object_or_404(InfrastructureNode, pk=node_id)

    def put(self, request, node_id):
        _require_permission(request, "api_agents.menu_agent")
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
        _require_permission(request, "api_agents.menu_agent")
        node = self.get_object(node_id)
        with transaction.atomic():
            for assignment in node.agent_assignments.select_for_update():
                assignment.delete()
        return success_response(detail="Remote manager cleared.")
