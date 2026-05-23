from rest_framework import serializers

from api_agents.models import Agent, AgentNodeAssignment
from sql.models import InfrastructureNode, Instance


class InfrastructureServiceSerializer(serializers.ModelSerializer):
    node_name = serializers.CharField(source="node.node_name", read_only=True)
    inventory_last_refresh_at = serializers.DateTimeField(
        source="inventory_last_success_at", read_only=True
    )

    class Meta:
        model = Instance
        fields = (
            "id",
            "instance_name",
            "type",
            "db_type",
            "host",
            "port",
            "db_name",
            "node",
            "node_name",
            "inventory_status",
            "inventory_detected_hostname",
            "inventory_detected_version",
            "inventory_last_refresh_at",
        )


def agent_summary(agent):
    if agent is None:
        return None

    return {
        "id": agent.id,
        "name": agent.name,
        "display_name": agent.display_name,
        "status": agent.status,
        "hostname": agent.hostname,
        "platform": agent.platform,
        "architecture": agent.architecture,
        "agent_version": agent.agent_version,
        "last_seen_at": agent.last_seen_at,
        "last_config_revision": agent.last_config_revision,
        "desired_config_revision": agent.desired_config_revision,
        "enabled": agent.enabled,
    }


class InfrastructureNodeSerializer(serializers.ModelSerializer):
    service_count = serializers.SerializerMethodField()
    services = serializers.SerializerMethodField()
    local_agent = serializers.SerializerMethodField()
    local_agent_count = serializers.SerializerMethodField()
    remote_manager = serializers.SerializerMethodField()

    def get_service_count(self, obj):
        if hasattr(obj, "service_count"):
            return obj.service_count
        visible_service_ids = self.context.get("visible_service_ids")
        if visible_service_ids is not None:
            return obj.services.filter(id__in=visible_service_ids).count()
        return obj.services.count()

    def get_services(self, obj):
        services = getattr(obj, "visible_services", None)
        if services is None:
            services = obj.services.select_related("node").order_by(
                "instance_name", "id"
            )
            visible_service_ids = self.context.get("visible_service_ids")
            if visible_service_ids is not None:
                services = services.filter(id__in=visible_service_ids)
        return InfrastructureServiceSerializer(services, many=True).data

    def get_local_agent(self, obj):
        agent = (
            obj.local_agents.filter(enabled=True)
            .exclude(status="revoked")
            .order_by("-last_seen_at", "name", "id")
            .first()
        )
        return agent_summary(agent)

    def get_local_agent_count(self, obj):
        return obj.local_agents.filter(enabled=True).exclude(status="revoked").count()

    def get_remote_manager(self, obj):
        assignment = (
            obj.agent_assignments.filter(enabled=True)
            .select_related("agent")
            .order_by("-command_enabled", "id")
            .first()
        )
        if assignment is None:
            return None
        payload = agent_summary(assignment.agent)
        payload.update(
            {
                "assignment_id": assignment.id,
                "command_enabled": assignment.command_enabled,
                "metrics_enabled": assignment.metrics_enabled,
                "online_schema_enabled": assignment.online_schema_enabled,
                "logs_enabled": assignment.logs_enabled,
                "modules": assignment.modules,
                "capabilities": assignment.capabilities,
            }
        )
        return payload

    class Meta:
        model = InfrastructureNode
        fields = (
            "id",
            "node_name",
            "hostname",
            "environment",
            "provider",
            "metadata",
            "enabled",
            "service_count",
            "services",
            "local_agent",
            "local_agent_count",
            "remote_manager",
            "create_time",
            "update_time",
        )


class InfrastructureNodeWriteSerializer(serializers.ModelSerializer):
    def validate_node_name(self, value):
        node_name = value.strip()
        if not node_name:
            raise serializers.ValidationError("Node name cannot be blank.")
        return node_name

    def validate_hostname(self, value):
        return value.strip()

    def validate_environment(self, value):
        return value.strip()

    def validate_provider(self, value):
        return value.strip()

    class Meta:
        model = InfrastructureNode
        fields = (
            "node_name",
            "hostname",
            "environment",
            "provider",
            "metadata",
            "enabled",
        )


class InfrastructureNodeRemoteManagerSerializer(serializers.Serializer):
    agent = serializers.PrimaryKeyRelatedField(
        queryset=Agent.objects.filter(enabled=True).exclude(status="revoked")
    )
    modules = serializers.ListField(
        child=serializers.CharField(), required=False, default=list
    )
    capabilities = serializers.ListField(
        child=serializers.CharField(), required=False, default=list
    )
    command_enabled = serializers.BooleanField(default=True)
    metrics_enabled = serializers.BooleanField(default=True)
    online_schema_enabled = serializers.BooleanField(default=False)
    logs_enabled = serializers.BooleanField(default=False)


class InfrastructureNodeRemoteManagerRecordSerializer(serializers.ModelSerializer):
    agent_name = serializers.CharField(source="agent.name", read_only=True)
    agent_display_name = serializers.CharField(
        source="agent.display_name", read_only=True
    )
    agent_status = serializers.CharField(source="agent.status", read_only=True)
    agent_version = serializers.CharField(source="agent.agent_version", read_only=True)

    class Meta:
        model = AgentNodeAssignment
        fields = (
            "id",
            "agent",
            "agent_name",
            "agent_display_name",
            "agent_status",
            "agent_version",
            "node",
            "enabled",
            "modules",
            "capabilities",
            "command_enabled",
            "metrics_enabled",
            "online_schema_enabled",
            "logs_enabled",
            "create_time",
            "update_time",
        )
