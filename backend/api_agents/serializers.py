from django.db import transaction
from rest_framework import serializers

from sql.utils.team import user_instances
from api_agents.services import notify_node_config_changed
from api_agents.models import (
    Agent,
    AgentCommand,
    AgentCommandEvent,
    AgentInstanceAssignment,
    AgentNodeAssignment,
    AgentToolArtifact,
)
from sql.models import (
    DEFAULT_NODE_EXPORTER_COLLECTORS,
    InfrastructureNode,
    Instance,
    default_node_exporter_collectors,
)

NODE_EXPORTER_COLLECTOR_SET = set(DEFAULT_NODE_EXPORTER_COLLECTORS)


def normalize_node_exporter_collectors(value):
    if value in (None, ""):
        return list(DEFAULT_NODE_EXPORTER_COLLECTORS)
    if not isinstance(value, list):
        raise serializers.ValidationError("Collectors must be a list.")

    normalized = []
    invalid = []
    seen = set()
    for item in value:
        collector = str(item).strip()
        if not collector:
            continue
        if collector not in NODE_EXPORTER_COLLECTOR_SET:
            invalid.append(collector)
            continue
        if collector not in seen:
            normalized.append(collector)
            seen.add(collector)
    if invalid:
        raise serializers.ValidationError(
            f"Unknown node_exporter collectors: {', '.join(sorted(set(invalid)))}."
        )
    return normalized


def infrastructure_node_queryset(current_node_id=None):
    queryset = InfrastructureNode.objects.filter(enabled=True)
    if current_node_id:
        queryset = queryset | InfrastructureNode.objects.filter(pk=current_node_id)
    return queryset


class AgentListSerializer(serializers.ModelSerializer):
    assignment_count = serializers.SerializerMethodField()
    local_node_name = serializers.CharField(source="local_node.name", read_only=True)

    def get_assignment_count(self, obj):
        if hasattr(obj, "assignment_count"):
            return obj.assignment_count
        return obj.assignments.count()

    class Meta:
        model = Agent
        fields = (
            "id",
            "organization_id",
            "name",
            "display_name",
            "status",
            "hostname",
            "platform",
            "architecture",
            "agent_version",
            "last_seen_at",
            "last_connected_at",
            "last_disconnected_at",
            "last_config_revision",
            "desired_config_revision",
            "enabled",
            "local_node",
            "local_node_name",
            "assignment_count",
            "create_time",
            "update_time",
        )


class AgentCreateSerializer(serializers.ModelSerializer):
    name = serializers.CharField(max_length=128, required=False, allow_blank=True)
    display_name = serializers.CharField(
        max_length=200, required=False, allow_blank=True
    )
    node_name = serializers.CharField(
        max_length=128, required=False, allow_blank=True, write_only=True
    )
    local_node = serializers.PrimaryKeyRelatedField(
        queryset=InfrastructureNode.objects.filter(enabled=True),
        required=False,
        allow_null=True,
    )
    monitoring_enabled = serializers.BooleanField(default=True, write_only=True)
    monitoring_collectors = serializers.ListField(
        child=serializers.CharField(),
        required=False,
        write_only=True,
        default=default_node_exporter_collectors,
    )

    class Meta:
        model = Agent
        fields = (
            "id",
            "name",
            "display_name",
            "metadata",
            "local_node",
            "node_name",
            "monitoring_enabled",
            "monitoring_collectors",
        )
        read_only_fields = ("id",)

    def validate_name(self, value):
        value = value.strip()
        if not value:
            return ""
        return value

    def validate_node_name(self, value):
        value = value.strip()
        if not value:
            return ""
        if InfrastructureNode.objects.filter(name=value).exists():
            raise serializers.ValidationError("A node with this name already exists.")
        return value

    def validate_monitoring_collectors(self, value):
        return normalize_node_exporter_collectors(value)

    def validate(self, attrs):
        attrs = super().validate(attrs)
        local_node = attrs.get("local_node")
        node_name = attrs.get("node_name", "").strip()
        agent_name = attrs.get("name", "").strip()

        if local_node is None and not node_name and not agent_name:
            raise serializers.ValidationError({"node_name": "Node name is required."})

        if local_node is None and not node_name:
            attrs["node_name"] = agent_name
            node_name = agent_name
        if (
            local_node is None
            and InfrastructureNode.objects.filter(name=node_name).exists()
        ):
            raise serializers.ValidationError(
                {"node_name": "A node with this name already exists."}
            )
        if agent_name and Agent.objects.filter(name=agent_name).exists():
            raise serializers.ValidationError(
                {"name": "An agent with this name already exists."}
            )
        return attrs

    def create(self, validated_data):
        local_node = validated_data.pop("local_node", None)
        node_name = validated_data.pop("node_name", "").strip()
        metadata = validated_data.pop("metadata", {})
        agent_name = validated_data.pop("name", "").strip()
        display_name = validated_data.pop("display_name", "").strip()
        monitoring_enabled = validated_data.pop("monitoring_enabled", True)
        monitoring_collectors = validated_data.pop(
            "monitoring_collectors", list(DEFAULT_NODE_EXPORTER_COLLECTORS)
        )

        with transaction.atomic():
            if local_node is None:
                local_node = InfrastructureNode.objects.create(
                    name=node_name,
                    address="",
                    monitoring_enabled=monitoring_enabled,
                    monitoring_collectors=monitoring_collectors,
                    metadata={"provisioning_status": "pending_agent_install"},
                )
            else:
                previous_monitoring_enabled = local_node.monitoring_enabled
                previous_monitoring_collectors = list(
                    local_node.monitoring_collectors or []
                )
                local_node.monitoring_enabled = monitoring_enabled
                local_node.monitoring_collectors = monitoring_collectors
                monitoring_changed = (
                    previous_monitoring_enabled != monitoring_enabled
                    or previous_monitoring_collectors != monitoring_collectors
                )
                if monitoring_changed:
                    local_node.save(
                        update_fields=[
                            "monitoring_enabled",
                            "monitoring_collectors",
                            "update_time",
                        ]
                    )
                    notify_node_config_changed(
                        local_node,
                        summary={
                            "action": "node.monitoring_changed",
                            "node_id": local_node.id,
                            "monitoring_enabled": monitoring_enabled,
                            "monitoring_collectors": monitoring_collectors,
                        },
                        reason="node.monitoring_changed",
                    )

            if not agent_name:
                agent_name = local_node.name
            if not display_name:
                display_name = f"{local_node.name} Agent"

            return Agent.objects.create(
                **validated_data,
                name=agent_name,
                display_name=display_name,
                metadata=metadata,
                local_node=local_node,
            )


class AgentUpdateSerializer(serializers.ModelSerializer):
    local_node = serializers.PrimaryKeyRelatedField(
        queryset=InfrastructureNode.objects.filter(enabled=True),
        required=False,
        allow_null=True,
    )

    class Meta:
        model = Agent
        fields = ("display_name", "enabled", "metadata", "local_node")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["local_node"].queryset = infrastructure_node_queryset(
            getattr(self.instance, "local_node_id", None)
        )


class AgentDetailSerializer(AgentListSerializer):
    assignments = serializers.SerializerMethodField()
    recent_commands = serializers.SerializerMethodField()

    class Meta(AgentListSerializer.Meta):
        fields = AgentListSerializer.Meta.fields + (
            "metadata",
            "assignments",
            "recent_commands",
        )

    def get_assignments(self, obj):
        return AgentAssignmentSerializer(
            obj.assignments.select_related("instance", "node_assignment", "local_node"),
            many=True,
        ).data

    def get_recent_commands(self, obj):
        commands = obj.commands.select_related("instance").order_by("-create_time")[:5]
        return AgentCommandSummarySerializer(commands, many=True).data


class AgentAssignmentSerializer(serializers.ModelSerializer):
    instance_name = serializers.CharField(
        source="instance.instance_name", read_only=True
    )
    db_type = serializers.CharField(source="instance.db_type", read_only=True)
    host = serializers.CharField(source="instance.host", read_only=True)
    port = serializers.IntegerField(source="instance.port", read_only=True)
    workflow_enabled = serializers.BooleanField(
        source="instance.workflow_enabled", read_only=True
    )
    node_assignment = serializers.IntegerField(
        source="node_assignment_id", read_only=True
    )
    node = serializers.IntegerField(source="node_assignment.node_id", read_only=True)
    local_node = serializers.IntegerField(source="local_node_id", read_only=True)
    inherited = serializers.SerializerMethodField()

    def get_inherited(self, obj):
        return bool(obj.node_assignment_id or obj.local_node_id)

    class Meta:
        model = AgentInstanceAssignment
        fields = (
            "id",
            "instance",
            "node",
            "node_assignment",
            "local_node",
            "inherited",
            "instance_name",
            "db_type",
            "host",
            "port",
            "workflow_enabled",
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


class AgentNodeAssignmentSerializer(serializers.ModelSerializer):
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


class AgentAssignmentReplaceItemSerializer(serializers.Serializer):
    instance = serializers.PrimaryKeyRelatedField(queryset=Instance.objects.none())
    enabled = serializers.BooleanField(default=True)
    modules = serializers.ListField(
        child=serializers.CharField(), required=False, default=list
    )
    capabilities = serializers.ListField(
        child=serializers.CharField(), required=False, default=list
    )
    command_enabled = serializers.BooleanField(default=False)
    metrics_enabled = serializers.BooleanField(default=True)
    online_schema_enabled = serializers.BooleanField(default=False)
    logs_enabled = serializers.BooleanField(default=False)


class AgentAssignmentReplaceSerializer(serializers.Serializer):
    assignments = AgentAssignmentReplaceItemSerializer(many=True)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        request = self.context.get("request")
        if request is not None:
            queryset = user_instances(request.user)
        else:
            queryset = Instance.objects.none()
        self.fields["assignments"].child.fields["instance"].queryset = queryset

    def validate_assignments(self, value):
        seen_instances = set()
        command_instances = set()
        for item in value:
            instance_id = item["instance"].pk
            if instance_id in seen_instances:
                raise serializers.ValidationError(
                    f"Instance {instance_id} appears more than once."
                )
            seen_instances.add(instance_id)
            if item.get("enabled", True) and item.get("command_enabled", False):
                if instance_id in command_instances:
                    raise serializers.ValidationError(
                        f"Instance {instance_id} has duplicate command assignments."
                    )
                command_instances.add(instance_id)

        conflicting = AgentInstanceAssignment.objects.filter(
            instance_id__in=command_instances,
            enabled=True,
            command_enabled=True,
        )
        agent = self.context["agent"]
        conflicting = conflicting.exclude(agent=agent)
        if conflicting.exists():
            instance_ids = sorted(conflicting.values_list("instance_id", flat=True))
            raise serializers.ValidationError(
                f"Instances already assigned to another command-capable agent: {instance_ids}."
            )
        return value


class AgentCommandEventSerializer(serializers.ModelSerializer):
    class Meta:
        model = AgentCommandEvent
        fields = ("id", "event_type", "message", "payload", "create_time")


class AgentCommandSummarySerializer(serializers.ModelSerializer):
    instance_name = serializers.CharField(
        source="instance.instance_name", read_only=True
    )

    class Meta:
        model = AgentCommand
        fields = (
            "id",
            "instance",
            "instance_name",
            "workflow_type",
            "workflow_id",
            "command_type",
            "status",
            "queued_at",
            "started_at",
            "finished_at",
            "cancel_requested_at",
            "create_time",
        )


class AgentCommandDetailSerializer(AgentCommandSummarySerializer):
    events = AgentCommandEventSerializer(many=True, read_only=True)

    class Meta(AgentCommandSummarySerializer.Meta):
        fields = AgentCommandSummarySerializer.Meta.fields + (
            "payload",
            "result",
            "error",
            "lease_owner",
            "lease_expires_at",
            "events",
        )


class AgentToolArtifactSerializer(serializers.ModelSerializer):
    class Meta:
        model = AgentToolArtifact
        fields = (
            "id",
            "tool_name",
            "version",
            "platform",
            "architecture",
            "download_url",
            "sha256",
            "size_bytes",
            "enabled",
            "notes",
            "create_time",
            "update_time",
        )
