from datetime import timedelta

from django.core.exceptions import PermissionDenied
from django.db import transaction
from django.utils import timezone
from rest_framework import permissions, serializers, status, views
from rest_framework.response import Response

from api_agents.authentication import AgentAPIKeyAuthentication
from api_agents.models import AgentCommand, AgentCommandStatus, AgentStatus
from api_agents.services import build_agent_config, complete_agent_workflow_command

TERMINAL_COMMAND_STATUSES = {
    AgentCommandStatus.SUCCEEDED,
    AgentCommandStatus.FAILED,
    AgentCommandStatus.CANCELLED,
    AgentCommandStatus.EXPIRED,
}


class IsAuthenticatedAgent(permissions.BasePermission):
    def has_permission(self, request, view):
        return bool(request.auth and getattr(request.auth, "can_connect", False))


class AgentRegisterSerializer(serializers.Serializer):
    install_id = serializers.CharField(max_length=80)
    name = serializers.CharField(max_length=128, allow_blank=True, required=False)
    address = serializers.CharField(max_length=200, allow_blank=True, required=False)
    hostname = serializers.CharField(max_length=255, allow_blank=True, required=False)
    platform = serializers.CharField(max_length=64, allow_blank=True, required=False)
    architecture = serializers.CharField(
        max_length=64, allow_blank=True, required=False
    )
    agent_version = serializers.CharField(
        max_length=64, allow_blank=True, required=False
    )
    config_revision = serializers.IntegerField(min_value=0, required=False, default=0)


class AgentHeartbeatSerializer(serializers.Serializer):
    install_id = serializers.CharField(max_length=80)
    status = serializers.ChoiceField(
        choices=(AgentStatus.ONLINE, AgentStatus.OFFLINE),
        required=False,
        default=AgentStatus.ONLINE,
    )
    config_revision = serializers.IntegerField(min_value=0, required=False, default=0)
    module_health = serializers.ListField(
        child=serializers.DictField(), required=False, default=list
    )


class AgentCommandLeaseSerializer(serializers.Serializer):
    lease_owner = serializers.CharField(
        max_length=128, allow_blank=True, required=False
    )
    lease_seconds = serializers.IntegerField(
        min_value=1, max_value=3600, required=False, default=300
    )


class AgentCommandProgressSerializer(AgentCommandLeaseSerializer):
    message = serializers.CharField(allow_blank=True, required=False, default="")
    payload = serializers.DictField(required=False, default=dict)


class AgentCommandFinishSerializer(serializers.Serializer):
    message = serializers.CharField(allow_blank=True, required=False, default="")
    result = serializers.DictField(required=False, default=dict)


class AgentCommandFailSerializer(serializers.Serializer):
    message = serializers.CharField(allow_blank=True, required=False, default="")
    error = serializers.DictField(required=False, default=dict)


class AgentRegisterView(views.APIView):
    authentication_classes = [AgentAPIKeyAuthentication]
    permission_classes = [IsAuthenticatedAgent]
    throttle_classes = []

    def post(self, request):
        serializer = AgentRegisterSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        agent = request.auth
        data = serializer.validated_data

        install_id = data["install_id"].strip()
        if agent.install_id and agent.install_id != install_id:
            raise PermissionDenied("Agent install ID is already bound.")

        now = timezone.now()
        address = data.get("address", "").strip()
        with transaction.atomic():
            agent.install_id = install_id
            agent.name = data.get("name") or agent.name
            agent.hostname = data.get("hostname") or agent.hostname
            agent.platform = data.get("platform") or agent.platform
            agent.architecture = data.get("architecture") or agent.architecture
            agent.agent_version = data.get("agent_version") or agent.agent_version
            agent.last_seen_at = now
            agent.last_connected_at = agent.last_connected_at or now
            agent.last_config_revision = (
                data.get("config_revision") or agent.last_config_revision
            )
            agent.status = AgentStatus.ONLINE
            agent.save(
                update_fields=[
                    "install_id",
                    "name",
                    "hostname",
                    "platform",
                    "architecture",
                    "agent_version",
                    "last_seen_at",
                    "last_connected_at",
                    "last_config_revision",
                    "status",
                    "update_time",
                ]
            )
            if agent.local_node_id:
                node = agent.local_node
                metadata = dict(node.metadata or {})
                metadata["agent_host"] = {
                    "agent_id": agent.id,
                    "install_id": install_id,
                    "hostname": agent.hostname,
                    "platform": agent.platform,
                    "architecture": agent.architecture,
                    "agent_version": agent.agent_version,
                    "last_registered_at": now.isoformat(),
                }
                metadata["provisioning_status"] = "agent_registered"
                node.metadata = metadata
                update_fields = ["metadata", "update_time"]
                if address:
                    node.address = address
                    update_fields.append("address")
                node.save(update_fields=update_fields)
        return Response(
            {
                "agent_id": agent.id,
                "desired_config_revision": agent.desired_config_revision,
            },
            status=status.HTTP_200_OK,
        )


class AgentConfigView(views.APIView):
    authentication_classes = [AgentAPIKeyAuthentication]
    permission_classes = [IsAuthenticatedAgent]
    throttle_classes = []

    def get(self, request):
        return Response(build_agent_config(request.auth), status=status.HTTP_200_OK)


class AgentHeartbeatView(views.APIView):
    authentication_classes = [AgentAPIKeyAuthentication]
    permission_classes = [IsAuthenticatedAgent]
    throttle_classes = []

    def post(self, request):
        serializer = AgentHeartbeatSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        agent = request.auth
        data = serializer.validated_data

        if agent.install_id and agent.install_id != data["install_id"]:
            raise PermissionDenied("Agent install ID does not match.")

        now = timezone.now()
        metadata = dict(agent.metadata or {})
        metadata["module_health"] = data["module_health"]
        metadata["heartbeat"] = {
            "status": data["status"],
            "config_revision": data["config_revision"],
            "received_at": now.isoformat(),
        }
        agent.metadata = metadata
        agent.status = data["status"]
        agent.last_seen_at = now
        agent.last_config_revision = data["config_revision"]
        agent.save(
            update_fields=[
                "metadata",
                "status",
                "last_seen_at",
                "last_config_revision",
                "update_time",
            ]
        )
        return Response(
            {"desired_config_revision": agent.desired_config_revision},
            status=status.HTTP_200_OK,
        )


def _agent_command(agent, command_id):
    return AgentCommand.objects.select_related("instance").get(
        id=command_id, agent=agent
    )


class AgentCommandDetailView(views.APIView):
    authentication_classes = [AgentAPIKeyAuthentication]
    permission_classes = [IsAuthenticatedAgent]
    throttle_classes = []

    def get(self, request, command_id):
        try:
            command = _agent_command(request.auth, command_id)
        except AgentCommand.DoesNotExist:
            return Response(
                {"detail": "Command not found."}, status=status.HTTP_404_NOT_FOUND
            )
        return Response(
            {
                "id": command.id,
                "agent_id": command.agent_id,
                "instance_id": command.instance_id,
                "command_type": command.command_type,
                "workflow_type": command.workflow_type,
                "workflow_id": command.workflow_id,
                "status": command.status,
                "idempotency_key": command.idempotency_key,
                "payload": command.payload,
                "cancel_requested": bool(command.cancel_requested_at),
            },
            status=status.HTTP_200_OK,
        )


class AgentCommandAckView(views.APIView):
    authentication_classes = [AgentAPIKeyAuthentication]
    permission_classes = [IsAuthenticatedAgent]
    throttle_classes = []

    def post(self, request, command_id):
        try:
            command = _agent_command(request.auth, command_id)
        except AgentCommand.DoesNotExist:
            return Response(
                {"detail": "Command not found."}, status=status.HTTP_404_NOT_FOUND
            )
        if command.status in {
            AgentCommandStatus.QUEUED,
            AgentCommandStatus.DISPATCHED,
        }:
            command.status = AgentCommandStatus.ACCEPTED
            command.accepted_at = timezone.now()
            command.save(update_fields=["status", "accepted_at", "update_time"])
            command.append_event("command.accepted", "Command accepted by agent.")
        return Response({"status": command.status}, status=status.HTTP_200_OK)


class AgentCommandStartView(views.APIView):
    authentication_classes = [AgentAPIKeyAuthentication]
    permission_classes = [IsAuthenticatedAgent]
    throttle_classes = []

    def post(self, request, command_id):
        try:
            command = _agent_command(request.auth, command_id)
        except AgentCommand.DoesNotExist:
            return Response(
                {"detail": "Command not found."}, status=status.HTTP_404_NOT_FOUND
            )
        serializer = AgentCommandLeaseSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        if command.status in {
            AgentCommandStatus.QUEUED,
            AgentCommandStatus.DISPATCHED,
            AgentCommandStatus.ACCEPTED,
        }:
            now = timezone.now()
            command.status = AgentCommandStatus.RUNNING
            command.started_at = command.started_at or now
            _set_command_lease(command, serializer.validated_data, now)
            command.save(
                update_fields=[
                    "status",
                    "started_at",
                    "lease_owner",
                    "lease_expires_at",
                    "update_time",
                ]
            )
            command.append_event("command.started", "Command started by agent.")
        return Response({"status": command.status}, status=status.HTTP_200_OK)


class AgentCommandProgressView(views.APIView):
    authentication_classes = [AgentAPIKeyAuthentication]
    permission_classes = [IsAuthenticatedAgent]
    throttle_classes = []

    def post(self, request, command_id):
        try:
            command = _agent_command(request.auth, command_id)
        except AgentCommand.DoesNotExist:
            return Response(
                {"detail": "Command not found."}, status=status.HTTP_404_NOT_FOUND
            )
        serializer = AgentCommandProgressSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        if command.status in TERMINAL_COMMAND_STATUSES:
            return Response(
                {"detail": "Command is already finished.", "status": command.status},
                status=status.HTTP_409_CONFLICT,
            )
        data = serializer.validated_data
        now = timezone.now()
        _set_command_lease(command, data, now)
        command.save(update_fields=["lease_owner", "lease_expires_at", "update_time"])
        command.append_event(
            "command.progress",
            data.get("message", ""),
            payload=data.get("payload", {}),
        )
        return Response({"status": command.status}, status=status.HTTP_200_OK)


class AgentCommandFinishView(views.APIView):
    authentication_classes = [AgentAPIKeyAuthentication]
    permission_classes = [IsAuthenticatedAgent]
    throttle_classes = []

    def post(self, request, command_id):
        try:
            command = _agent_command(request.auth, command_id)
        except AgentCommand.DoesNotExist:
            return Response(
                {"detail": "Command not found."}, status=status.HTTP_404_NOT_FOUND
            )
        serializer = AgentCommandFinishSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data
        if command.status in TERMINAL_COMMAND_STATUSES:
            return Response({"status": command.status}, status=status.HTTP_200_OK)
        command.status = AgentCommandStatus.SUCCEEDED
        command.result = data.get("result", {})
        command.finished_at = timezone.now()
        command.lease_owner = ""
        command.lease_expires_at = None
        command.save(
            update_fields=[
                "status",
                "result",
                "finished_at",
                "lease_owner",
                "lease_expires_at",
                "update_time",
            ]
        )
        command.append_event("command.succeeded", data.get("message", ""))
        complete_agent_workflow_command(
            command,
            "success",
            message=data.get("message", ""),
            payload={**command.result, "command_id": command.id},
        )
        return Response({"status": command.status}, status=status.HTTP_200_OK)


class AgentCommandFailView(views.APIView):
    authentication_classes = [AgentAPIKeyAuthentication]
    permission_classes = [IsAuthenticatedAgent]
    throttle_classes = []

    def post(self, request, command_id):
        try:
            command = _agent_command(request.auth, command_id)
        except AgentCommand.DoesNotExist:
            return Response(
                {"detail": "Command not found."}, status=status.HTTP_404_NOT_FOUND
            )
        serializer = AgentCommandFailSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data
        if command.status in TERMINAL_COMMAND_STATUSES:
            return Response({"status": command.status}, status=status.HTTP_200_OK)
        command.status = AgentCommandStatus.FAILED
        command.error = data.get("error", {})
        command.finished_at = timezone.now()
        command.lease_owner = ""
        command.lease_expires_at = None
        command.save(
            update_fields=[
                "status",
                "error",
                "finished_at",
                "lease_owner",
                "lease_expires_at",
                "update_time",
            ]
        )
        command.append_event("command.failed", data.get("message", ""))
        complete_agent_workflow_command(
            command,
            "failed",
            message=data.get("message", ""),
            payload={**command.error, "command_id": command.id},
        )
        return Response({"status": command.status}, status=status.HTTP_200_OK)


class AgentCommandCancelledView(views.APIView):
    authentication_classes = [AgentAPIKeyAuthentication]
    permission_classes = [IsAuthenticatedAgent]
    throttle_classes = []

    def post(self, request, command_id):
        try:
            command = _agent_command(request.auth, command_id)
        except AgentCommand.DoesNotExist:
            return Response(
                {"detail": "Command not found."}, status=status.HTTP_404_NOT_FOUND
            )
        serializer = AgentCommandFinishSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data
        if command.status in TERMINAL_COMMAND_STATUSES:
            return Response({"status": command.status}, status=status.HTTP_200_OK)
        command.status = AgentCommandStatus.CANCELLED
        command.result = data.get("result", {})
        command.finished_at = timezone.now()
        command.lease_owner = ""
        command.lease_expires_at = None
        command.save(
            update_fields=[
                "status",
                "result",
                "finished_at",
                "lease_owner",
                "lease_expires_at",
                "update_time",
            ]
        )
        command.append_event("command.cancelled", data.get("message", ""))
        complete_agent_workflow_command(
            command,
            "cancelled",
            message=data.get("message", ""),
            payload={**command.result, "command_id": command.id},
        )
        return Response({"status": command.status}, status=status.HTTP_200_OK)


def _set_command_lease(command, data, now):
    command.lease_owner = data.get("lease_owner", command.lease_owner)
    command.lease_expires_at = now + timedelta(seconds=data.get("lease_seconds", 300))
