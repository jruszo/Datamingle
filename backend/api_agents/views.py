from django.db import transaction
from django.db.models import Count
from django.db.models import Q
from django.shortcuts import get_object_or_404
from rest_framework import generics, permissions, status, views

from api_agents.models import (
    Agent,
    AgentCommand,
    AgentCommandStatus,
    AgentInstanceAssignment,
    AgentStatus,
    AgentToolArtifact,
)
from api_agents.serializers import (
    AgentAssignmentReplaceSerializer,
    AgentAssignmentSerializer,
    AgentCommandDetailSerializer,
    AgentCommandSummarySerializer,
    AgentCreateSerializer,
    AgentDetailSerializer,
    AgentListSerializer,
    AgentToolArtifactSerializer,
    AgentUpdateSerializer,
)
from api_core.pagination import CustomizedPagination
from api_core.response import success_response
from api_agents.dispatch import notify_config_changed
from api_agents.services import (
    build_agent_install_command,
    issue_agent_api_key,
    notify_tool_artifact_changed,
    request_command_cancel,
    revoke_agent_api_key,
)

TERMINAL_COMMAND_STATUSES = {
    AgentCommandStatus.SUCCEEDED,
    AgentCommandStatus.FAILED,
    AgentCommandStatus.CANCELLED,
    AgentCommandStatus.EXPIRED,
}


class AgentMenuPermission(permissions.BasePermission):
    def has_permission(self, request, view):
        return bool(
            request.user
            and request.user.is_authenticated
            and (
                request.user.is_superuser
                or request.user.has_perm("api_agents.menu_agent")
            )
        )


class AgentListCreateView(generics.ListAPIView):
    permission_classes = [AgentMenuPermission]
    pagination_class = CustomizedPagination
    serializer_class = AgentListSerializer

    def get_queryset(self):
        queryset = Agent.objects.annotate(assignment_count=Count("assignments"))
        search = self.request.query_params.get("search", "").strip()
        if search:
            queryset = queryset.filter(
                Q(name__icontains=search)
                | Q(display_name__icontains=search)
                | Q(status__icontains=search)
                | Q(hostname__icontains=search)
                | Q(agent_version__icontains=search)
            )
        return queryset.order_by("name", "id")

    def post(self, request):
        serializer = AgentCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        agent = serializer.save()
        issued_key = issue_agent_api_key(agent)
        data = AgentDetailSerializer(agent).data
        data["api_key"] = issued_key.value
        data["api_key_backend"] = issued_key.backend
        data["install_command"] = build_agent_install_command(request, issued_key.value)
        return success_response(
            data=data,
            detail="Agent created.",
            status_code=status.HTTP_201_CREATED,
        )


class AgentDetailView(views.APIView):
    permission_classes = [AgentMenuPermission]

    def get_object(self, agent_id):
        return get_object_or_404(
            Agent.objects.annotate(assignment_count=Count("assignments")), pk=agent_id
        )

    def get(self, request, agent_id):
        return success_response(
            data=AgentDetailSerializer(self.get_object(agent_id)).data
        )

    def patch(self, request, agent_id):
        agent = self.get_object(agent_id)
        serializer = AgentUpdateSerializer(agent, data=request.data, partial=True)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return success_response(data=AgentDetailSerializer(agent).data)

    def delete(self, request, agent_id):
        agent = self.get_object(agent_id)
        revoke_agent_api_key(agent)
        agent.enabled = False
        agent.status = AgentStatus.REVOKED
        agent.save(update_fields=["enabled", "status", "update_time"])
        return success_response(
            data=AgentDetailSerializer(agent).data, detail="Agent revoked."
        )


class AgentAssignmentListReplaceView(views.APIView):
    permission_classes = [AgentMenuPermission]

    def get_agent(self, agent_id):
        return get_object_or_404(Agent, pk=agent_id)

    def get(self, request, agent_id):
        agent = self.get_agent(agent_id)
        assignments = agent.assignments.select_related("instance")
        return success_response(
            data=AgentAssignmentSerializer(assignments, many=True).data
        )

    def put(self, request, agent_id):
        agent = self.get_agent(agent_id)
        serializer = AgentAssignmentReplaceSerializer(
            data=request.data,
            context={"agent": agent},
        )
        serializer.is_valid(raise_exception=True)

        with transaction.atomic():
            agent.assignments.all().delete()
            assignments = []
            for item in serializer.validated_data["assignments"]:
                assignment = AgentInstanceAssignment(agent=agent, **item)
                assignment.full_clean()
                assignments.append(assignment)
            AgentInstanceAssignment.objects.bulk_create(assignments)
            agent.bump_desired_config_revision(
                summary={
                    "action": "assignments.replaced",
                    "assignment_count": len(assignments),
                },
                created_by=request.user,
            )
            transaction.on_commit(
                lambda: notify_config_changed(agent, reason="assignments.replaced")
            )

        saved = agent.assignments.select_related("instance")
        return success_response(data=AgentAssignmentSerializer(saved, many=True).data)


class AgentCommandListView(generics.ListAPIView):
    permission_classes = [AgentMenuPermission]
    pagination_class = CustomizedPagination
    serializer_class = AgentCommandSummarySerializer

    def get_queryset(self):
        get_object_or_404(Agent, pk=self.kwargs["agent_id"])
        queryset = AgentCommand.objects.select_related("instance").filter(
            agent_id=self.kwargs["agent_id"]
        )

        status_filter = self.request.query_params.get("status", "").strip()
        if status_filter:
            queryset = queryset.filter(status=status_filter)

        search = self.request.query_params.get("search", "").strip()
        if search:
            queryset = queryset.filter(
                Q(command_type__icontains=search)
                | Q(workflow_type__icontains=search)
                | Q(workflow_id__icontains=search)
                | Q(instance__instance_name__icontains=search)
            )

        return queryset.order_by("-create_time", "-id")


class AgentCommandDetailView(views.APIView):
    permission_classes = [AgentMenuPermission]

    def get_object(self, agent_id, command_id):
        return get_object_or_404(
            AgentCommand.objects.select_related("instance")
            .prefetch_related("events")
            .filter(agent_id=agent_id),
            pk=command_id,
        )

    def get(self, request, agent_id, command_id):
        command = self.get_object(agent_id, command_id)
        return success_response(data=AgentCommandDetailSerializer(command).data)


class AgentCommandCancelView(views.APIView):
    permission_classes = [AgentMenuPermission]

    def get_object(self, agent_id, command_id):
        return get_object_or_404(
            AgentCommand.objects.select_related("instance")
            .prefetch_related("events")
            .filter(agent_id=agent_id),
            pk=command_id,
        )

    def post(self, request, agent_id, command_id):
        command = self.get_object(agent_id, command_id)
        if command.status in TERMINAL_COMMAND_STATUSES:
            return success_response(
                data=AgentCommandDetailSerializer(command).data,
                detail="Command is already finished.",
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        if command.cancel_requested_at is None:
            command = request_command_cancel(command)
        return success_response(
            data=AgentCommandDetailSerializer(command).data,
            detail="Command cancellation requested.",
        )


class AgentToolArtifactListCreateView(generics.ListAPIView):
    permission_classes = [permissions.IsAdminUser]
    pagination_class = CustomizedPagination
    serializer_class = AgentToolArtifactSerializer

    def get_queryset(self):
        return AgentToolArtifact.objects.all()

    def post(self, request):
        serializer = AgentToolArtifactSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        artifact = serializer.save()
        notify_tool_artifact_changed(
            artifact, action="tool_artifact.created", user=request.user
        )
        return success_response(
            data=AgentToolArtifactSerializer(artifact).data,
            detail="Tool artifact created.",
            status_code=status.HTTP_201_CREATED,
        )


class AgentToolArtifactDetailView(views.APIView):
    permission_classes = [permissions.IsAdminUser]

    def get_object(self, artifact_id):
        return get_object_or_404(AgentToolArtifact, pk=artifact_id)

    def patch(self, request, artifact_id):
        artifact = self.get_object(artifact_id)
        serializer = AgentToolArtifactSerializer(
            artifact, data=request.data, partial=True
        )
        serializer.is_valid(raise_exception=True)
        artifact = serializer.save()
        notify_tool_artifact_changed(
            artifact, action="tool_artifact.updated", user=request.user
        )
        return success_response(data=AgentToolArtifactSerializer(artifact).data)

    def delete(self, request, artifact_id):
        artifact = self.get_object(artifact_id)
        artifact.enabled = False
        artifact.save(update_fields=["enabled", "update_time"])
        notify_tool_artifact_changed(
            artifact, action="tool_artifact.disabled", user=request.user
        )
        return success_response(
            data=AgentToolArtifactSerializer(artifact).data,
            detail="Tool artifact disabled.",
        )
