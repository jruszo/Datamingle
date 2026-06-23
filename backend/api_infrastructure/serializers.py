import re

from django.db import transaction
from rest_framework import serializers

from api_agents.models import Agent, AgentNodeAssignment
from api_agents.services import notify_node_config_changed
from api_agents.time import agent_datetime_to_utc_iso
from sql.models import (
    DEFAULT_NODE_EXPORTER_COLLECTORS,
    InfrastructureNode,
    Instance,
    InstanceTag,
    Team,
    ServiceRecommendation,
    normalize_service_monitoring_collectors,
    service_exporter_collectors_for_engine,
)

NODE_EXPORTER_COLLECTOR_SET = set(DEFAULT_NODE_EXPORTER_COLLECTORS)
MONITORING_LABEL_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
MAX_MONITORING_LABELS = 32
MAX_MONITORING_LABEL_NAME_LENGTH = 64
MAX_MONITORING_LABEL_VALUE_LENGTH = 256


def normalize_monitoring_labels(value):
    if value in (None, ""):
        return {}
    if not isinstance(value, dict):
        raise serializers.ValidationError("Monitoring labels must be an object.")
    if len(value) > MAX_MONITORING_LABELS:
        raise serializers.ValidationError(
            f"At most {MAX_MONITORING_LABELS} monitoring labels are allowed."
        )

    normalized = {}
    for raw_name, raw_value in value.items():
        name = str(raw_name).strip()
        if not isinstance(raw_value, str):
            raise serializers.ValidationError(
                f'Monitoring label "{name}" requires a string value.'
            )
        label_value = raw_value.strip()
        if (
            not name
            or len(name) > MAX_MONITORING_LABEL_NAME_LENGTH
            or not MONITORING_LABEL_NAME_RE.fullmatch(name)
        ):
            raise serializers.ValidationError(
                f'"{name}" is not a valid monitoring label name.'
            )
        if not label_value:
            raise serializers.ValidationError(
                f'Monitoring label "{name}" requires a value.'
            )
        if len(label_value) > MAX_MONITORING_LABEL_VALUE_LENGTH:
            raise serializers.ValidationError(
                f'Monitoring label "{name}" must be '
                f"{MAX_MONITORING_LABEL_VALUE_LENGTH} characters or fewer."
            )
        normalized[name] = label_value
    return normalized


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


def primary_node_agent(node):
    local_agent = (
        node.local_agents.filter(enabled=True)
        .exclude(status="revoked")
        .order_by("-last_seen_at", "name", "id")
        .first()
    )
    if local_agent is not None:
        return local_agent
    assignment = (
        node.agent_assignments.filter(enabled=True)
        .select_related("agent")
        .order_by("-command_enabled", "id")
        .first()
    )
    return assignment.agent if assignment is not None else None


class DatabaseServiceSerializer(serializers.ModelSerializer):
    node_id = serializers.IntegerField(read_only=True)
    service_name = serializers.CharField(source="instance_name", read_only=True)
    role = serializers.CharField(source="type", read_only=True)
    engine = serializers.CharField(source="db_type", read_only=True)
    team_ids = serializers.SerializerMethodField()
    service_tag_ids = serializers.SerializerMethodField()
    monitoring_collectors = serializers.SerializerMethodField()
    effective_monitoring_labels = serializers.SerializerMethodField()
    inventory_last_refresh_at = serializers.DateTimeField(
        source="inventory_last_success_at", read_only=True
    )

    def get_team_ids(self, obj):
        return list(
            obj.resource_group.values_list("team_id", flat=True).order_by("team_id")
        )

    def get_service_tag_ids(self, obj):
        return list(obj.instance_tag.values_list("id", flat=True).order_by("id"))

    def get_monitoring_collectors(self, obj):
        return normalize_service_monitoring_collectors(
            obj.db_type, obj.monitoring_collectors
        )

    def get_effective_monitoring_labels(self, obj):
        labels = dict(obj.node.monitoring_labels or {}) if obj.node_id else {}
        labels.update(obj.monitoring_labels or {})
        return labels

    class Meta:
        model = Instance
        fields = (
            "id",
            "node_id",
            "service_name",
            "role",
            "engine",
            "host",
            "port",
            "user",
            "monitoring_enabled",
            "monitoring_collectors",
            "monitoring_labels",
            "effective_monitoring_labels",
            "is_ssl",
            "verify_ssl",
            "db_name",
            "show_db_name_regex",
            "denied_db_name_regex",
            "charset",
            "team_ids",
            "service_tag_ids",
            "inventory_status",
            "inventory_detected_hostname",
            "inventory_detected_version",
            "inventory_last_refresh_at",
            "create_time",
            "update_time",
        )


class ServiceRecommendationSerializer(serializers.ModelSerializer):
    node_id = serializers.IntegerField(read_only=True)

    class Meta:
        model = ServiceRecommendation
        fields = (
            "id",
            "node_id",
            "service_name",
            "engine",
            "host",
            "port",
            "source",
            "confidence",
            "status",
            "last_seen_at",
        )


class InfrastructureNodeSerializer(serializers.ModelSerializer):
    team_ids = serializers.SerializerMethodField()
    agent = serializers.SerializerMethodField()
    agent_id = serializers.SerializerMethodField()
    agent_status = serializers.SerializerMethodField()
    service_count = serializers.SerializerMethodField()
    recommendation_count = serializers.SerializerMethodField()
    services = serializers.SerializerMethodField()
    recommendations = serializers.SerializerMethodField()

    def get_team_ids(self, obj):
        return list(
            obj.resource_group.values_list("team_id", flat=True).order_by("team_id")
        )

    def get_agent_id(self, obj):
        agent = primary_node_agent(obj)
        return agent.id if agent is not None else None

    def get_agent_status(self, obj):
        agent = primary_node_agent(obj)
        return agent.status if agent is not None else None

    def get_agent(self, obj):
        agent = primary_node_agent(obj)
        if agent is None:
            return None
        return {
            "id": agent.id,
            "status": agent.status,
            "hostname": agent.hostname,
            "platform": agent.platform,
            "architecture": agent.architecture,
            "agent_version": agent.agent_version,
            "last_seen_at": agent_datetime_to_utc_iso(agent.last_seen_at),
            "last_websocket_pong_at": agent_datetime_to_utc_iso(
                agent.last_websocket_pong_at
            ),
            "last_connected_at": agent_datetime_to_utc_iso(agent.last_connected_at),
            "last_disconnected_at": agent_datetime_to_utc_iso(
                agent.last_disconnected_at
            ),
            "last_config_revision": agent.last_config_revision,
            "desired_config_revision": agent.desired_config_revision,
            "enabled": agent.enabled,
        }

    def get_service_count(self, obj):
        if hasattr(obj, "service_count"):
            return obj.service_count
        visible_service_ids = self.context.get("visible_service_ids")
        queryset = obj.services
        if visible_service_ids is not None:
            queryset = queryset.filter(id__in=visible_service_ids)
        return queryset.count()

    def get_recommendation_count(self, obj):
        if hasattr(obj, "recommendation_count"):
            return obj.recommendation_count
        return obj.service_recommendations.filter(
            status=ServiceRecommendation.STATUS_RECOMMENDED
        ).count()

    def get_services(self, obj):
        services = getattr(obj, "visible_services", None)
        if services is None:
            services = obj.services.prefetch_related("resource_group", "instance_tag")
            visible_service_ids = self.context.get("visible_service_ids")
            if visible_service_ids is not None:
                services = services.filter(id__in=visible_service_ids)
            services = services.order_by("instance_name", "id")
        return DatabaseServiceSerializer(services, many=True).data

    def get_recommendations(self, obj):
        recommendations = obj.service_recommendations.order_by("-last_seen_at", "id")
        return ServiceRecommendationSerializer(recommendations, many=True).data

    class Meta:
        model = InfrastructureNode
        fields = (
            "id",
            "name",
            "address",
            "description",
            "metadata",
            "monitoring_enabled",
            "monitoring_collectors",
            "monitoring_labels",
            "team_ids",
            "agent",
            "agent_id",
            "agent_status",
            "service_count",
            "recommendation_count",
            "services",
            "recommendations",
            "create_time",
            "update_time",
        )


class InfrastructureNodeWriteSerializer(serializers.ModelSerializer):
    team_ids = serializers.PrimaryKeyRelatedField(
        source="resource_group",
        queryset=Team.objects.filter(is_deleted=0),
        many=True,
        required=False,
    )

    def validate_name(self, value):
        name = value.strip()
        if not name:
            raise serializers.ValidationError("Node name cannot be blank.")
        return name

    def validate_address(self, value):
        address = value.strip()
        return address

    def validate_description(self, value):
        return value.strip()

    def validate_monitoring_collectors(self, value):
        return normalize_node_exporter_collectors(value)

    def validate_monitoring_labels(self, value):
        return normalize_monitoring_labels(value)

    def create(self, validated_data):
        teams = validated_data.pop("resource_group", [])
        with transaction.atomic():
            node = InfrastructureNode.objects.create(**validated_data)
            node.resource_group.set(teams)
        return node

    def update(self, instance, validated_data):
        teams = validated_data.pop("resource_group", None)
        previous_monitoring_enabled = instance.monitoring_enabled
        previous_monitoring_collectors = list(instance.monitoring_collectors or [])
        previous_monitoring_labels = dict(instance.monitoring_labels or {})
        with transaction.atomic():
            for field, value in validated_data.items():
                setattr(instance, field, value)
            instance.save()
            if teams is not None:
                instance.resource_group.set(teams)
            monitoring_changed = (
                "monitoring_enabled" in validated_data
                and previous_monitoring_enabled != instance.monitoring_enabled
            )
            collectors_changed = (
                "monitoring_collectors" in validated_data
                and previous_monitoring_collectors
                != list(instance.monitoring_collectors or [])
            )
            labels_changed = (
                "monitoring_labels" in validated_data
                and previous_monitoring_labels != dict(instance.monitoring_labels or {})
            )
            if monitoring_changed or collectors_changed or labels_changed:
                notify_node_config_changed(
                    instance,
                    summary={
                        "action": "node.monitoring_changed",
                        "node_id": instance.id,
                        "monitoring_enabled": instance.monitoring_enabled,
                        "monitoring_collectors": instance.monitoring_collectors,
                        "monitoring_labels": instance.monitoring_labels,
                    },
                    reason="node.monitoring_changed",
                )
        return instance

    class Meta:
        model = InfrastructureNode
        fields = (
            "name",
            "address",
            "description",
            "metadata",
            "monitoring_enabled",
            "monitoring_collectors",
            "monitoring_labels",
            "team_ids",
        )


class DatabaseServiceWriteSerializer(serializers.ModelSerializer):
    node_id = serializers.PrimaryKeyRelatedField(
        source="node",
        queryset=InfrastructureNode.objects.filter(enabled=True),
    )
    service_name = serializers.CharField(source="instance_name", max_length=50)
    role = serializers.ChoiceField(
        source="type", choices=Instance._meta.get_field("type").choices
    )
    engine = serializers.ChoiceField(
        source="db_type",
        choices=(("mysql", "MySQL"), ("pgsql", "PostgreSQL")),
    )
    team_ids = serializers.PrimaryKeyRelatedField(
        source="resource_group",
        queryset=Team.objects.filter(is_deleted=0),
        many=True,
        required=False,
    )
    service_tag_ids = serializers.PrimaryKeyRelatedField(
        source="instance_tag",
        queryset=InstanceTag.objects.filter(active=True),
        many=True,
        required=False,
    )
    recommendation_id = serializers.PrimaryKeyRelatedField(
        source="recommendation",
        queryset=ServiceRecommendation.objects.filter(
            status=ServiceRecommendation.STATUS_RECOMMENDED
        ),
        required=False,
        allow_null=True,
        write_only=True,
    )
    monitoring_collectors = serializers.ListField(
        child=serializers.CharField(),
        required=False,
        allow_empty=True,
    )

    def validate_service_name(self, value):
        instance_name = value.strip()
        if not instance_name:
            raise serializers.ValidationError("Service name cannot be blank.")
        return instance_name

    def validate_host(self, value):
        host = value.strip()
        if not host:
            raise serializers.ValidationError("Host cannot be blank.")
        return host

    def validate_user(self, value):
        return value.strip()

    def validate_db_name(self, value):
        return value.strip()

    def validate_show_db_name_regex(self, value):
        return value.strip()

    def validate_denied_db_name_regex(self, value):
        return value.strip()

    def validate_charset(self, value):
        return value.strip()

    def validate_monitoring_labels(self, value):
        return normalize_monitoring_labels(value)

    def validate(self, attrs):
        attrs = super().validate(attrs)
        engine = attrs.get("db_type") or getattr(self.instance, "db_type", "")
        if "monitoring_collectors" in attrs:
            collectors = attrs["monitoring_collectors"]
            allowed = set(service_exporter_collectors_for_engine(engine))
            invalid = sorted(
                {
                    str(collector).strip()
                    for collector in collectors
                    if str(collector).strip() not in allowed
                }
            )
            if invalid:
                raise serializers.ValidationError(
                    {
                        "monitoring_collectors": (
                            "Unknown exporter collectors: " + ", ".join(invalid)
                        )
                    }
                )
            attrs["monitoring_collectors"] = normalize_service_monitoring_collectors(
                engine, collectors
            )
        else:
            attrs["monitoring_collectors"] = normalize_service_monitoring_collectors(
                engine,
                (
                    getattr(self.instance, "monitoring_collectors", None)
                    if self.instance is not None
                    else None
                ),
            )
        return attrs

    def create(self, validated_data):
        teams = validated_data.pop("resource_group", [])
        instance_tags = validated_data.pop("instance_tag", [])
        recommendation = validated_data.pop("recommendation", None)
        with transaction.atomic():
            instance = Instance.objects.create(**validated_data)
            instance.resource_group.set(teams)
            instance.instance_tag.set(instance_tags)
            if recommendation is not None:
                recommendation.status = ServiceRecommendation.STATUS_ACCEPTED
                recommendation.save(update_fields=["status", "update_time"])
        return instance

    def update(self, instance, validated_data):
        teams = validated_data.pop("resource_group", None)
        instance_tags = validated_data.pop("instance_tag", None)
        recommendation = validated_data.pop("recommendation", None)
        password = validated_data.pop("password", None)
        with transaction.atomic():
            for field, value in validated_data.items():
                setattr(instance, field, value)
            if password not in (None, ""):
                instance.password = password
            instance.save()
            if teams is not None:
                instance.resource_group.set(teams)
            if instance_tags is not None:
                instance.instance_tag.set(instance_tags)
            if recommendation is not None:
                recommendation.status = ServiceRecommendation.STATUS_ACCEPTED
                recommendation.save(update_fields=["status", "update_time"])
        return instance

    class Meta:
        model = Instance
        fields = (
            "node_id",
            "service_name",
            "role",
            "engine",
            "host",
            "port",
            "user",
            "password",
            "monitoring_enabled",
            "queryable",
            "monitoring_collectors",
            "monitoring_labels",
            "is_ssl",
            "verify_ssl",
            "db_name",
            "show_db_name_regex",
            "denied_db_name_regex",
            "charset",
            "team_ids",
            "service_tag_ids",
            "recommendation_id",
        )
        extra_kwargs = {"password": {"write_only": True, "required": False}}


class RecommendationStatusSerializer(serializers.ModelSerializer):
    status = serializers.ChoiceField(choices=ServiceRecommendation.STATUS_CHOICES)

    class Meta:
        model = ServiceRecommendation
        fields = ("status",)


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
