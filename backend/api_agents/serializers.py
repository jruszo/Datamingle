from rest_framework import serializers

from api_agents.models import (
    Agent,
    AgentCommand,
    AgentCommandEvent,
    AgentInstanceAssignment,
    AgentToolArtifact,
)
from sql.models import Instance


class AgentListSerializer(serializers.ModelSerializer):
    assignment_count = serializers.SerializerMethodField()

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
            "assignment_count",
            "create_time",
            "update_time",
        )


class AgentCreateSerializer(serializers.ModelSerializer):
    class Meta:
        model = Agent
        fields = ("id", "organization_id", "name", "display_name", "metadata")
        read_only_fields = ("id",)

    def validate_name(self, value):
        value = value.strip()
        if not value:
            raise serializers.ValidationError("Agent name cannot be blank.")
        return value


class AgentUpdateSerializer(serializers.ModelSerializer):
    class Meta:
        model = Agent
        fields = ("display_name", "enabled", "metadata")


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
            obj.assignments.select_related("instance"), many=True
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

    class Meta:
        model = AgentInstanceAssignment
        fields = (
            "id",
            "instance",
            "instance_name",
            "db_type",
            "host",
            "port",
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
    instance = serializers.PrimaryKeyRelatedField(queryset=Instance.objects.all())
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
            "cancel_requested_at",
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
