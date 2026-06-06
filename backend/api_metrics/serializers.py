import uuid

from rest_framework import serializers

from api_metrics.models import MetricsDashboard
from api_metrics.views import (
    DEFAULT_MAX_QUERY_LENGTH,
    DEFAULT_MIN_STEP_SECONDS,
    LABEL_NAME_RE,
)

MAX_PANELS = 100
MAX_LEGEND_LABELS = 10
MAX_QUERIES_PER_PANEL = 10
MAX_VARIABLES = 20
TIME_RANGE_OPTIONS = {
    60 * 60,
    6 * 60 * 60,
    24 * 60 * 60,
    7 * 24 * 60 * 60,
}
REFRESH_INTERVAL_OPTIONS = {0, 30, 60, 5 * 60}


class DashboardPanelListSerializer(serializers.ListSerializer):
    def to_internal_value(self, data):
        if not isinstance(data, list):
            raise serializers.ValidationError("Panels must be a list.")
        if len(data) > MAX_PANELS:
            raise serializers.ValidationError(
                f"A dashboard can contain at most {MAX_PANELS} panels."
            )
        return super().to_internal_value(data)


class DashboardQuerySerializer(serializers.Serializer):
    ref_id = serializers.RegexField(r"^[A-Z][A-Z0-9]{0,2}$", max_length=3)
    query = serializers.CharField(
        max_length=DEFAULT_MAX_QUERY_LENGTH, trim_whitespace=True
    )
    editor_mode = serializers.ChoiceField(choices=("builder", "code"), default="code")
    disabled = serializers.BooleanField(default=False)
    legend = serializers.CharField(max_length=160, allow_blank=True, default="")


class DashboardVisualizationSerializer(serializers.Serializer):
    type = serializers.ChoiceField(
        choices=("time_series", "bar", "stat", "gauge", "table"),
        default="time_series",
    )
    unit = serializers.CharField(max_length=40, allow_blank=True, default="")
    decimals = serializers.IntegerField(
        min_value=0, max_value=10, allow_null=True, default=None
    )
    min = serializers.FloatField(allow_null=True, default=None)
    max = serializers.FloatField(allow_null=True, default=None)
    color_scheme = serializers.ChoiceField(
        choices=("classic", "cool", "warm", "status"),
        default="classic",
    )
    thresholds = serializers.ListField(
        child=serializers.DictField(),
        max_length=10,
        default=list,
    )
    legend_placement = serializers.ChoiceField(
        choices=("bottom", "right", "hidden"),
        default="bottom",
    )
    tooltip_mode = serializers.ChoiceField(
        choices=("single", "all"),
        default="all",
    )
    line_width = serializers.IntegerField(min_value=0, max_value=8, default=2)
    fill_opacity = serializers.IntegerField(min_value=0, max_value=100, default=10)
    stack = serializers.BooleanField(default=False)

    def validate_thresholds(self, value):
        normalized = []
        for item in value:
            threshold_value = item.get("value")
            color = str(item.get("color") or "").strip()
            if isinstance(threshold_value, bool) or not isinstance(
                threshold_value, (int, float)
            ):
                raise serializers.ValidationError(
                    "Each threshold requires a numeric value."
                )
            if not color or len(color) > 30:
                raise serializers.ValidationError(
                    "Each threshold requires a valid color."
                )
            normalized.append({"value": threshold_value, "color": color})
        return sorted(normalized, key=lambda item: item["value"])

    def validate(self, attrs):
        minimum = attrs.get("min")
        maximum = attrs.get("max")
        if minimum is not None and maximum is not None and maximum <= minimum:
            raise serializers.ValidationError("Maximum must be greater than minimum.")
        return attrs


class DashboardVariableSerializer(serializers.Serializer):
    name = serializers.RegexField(r"^[a-zA-Z_][a-zA-Z0-9_]*$", max_length=64)
    label = serializers.CharField(max_length=120)
    metric = serializers.CharField(max_length=255, allow_blank=True, default="")
    label_name = serializers.RegexField(r"^[a-zA-Z_][a-zA-Z0-9_]*$", max_length=128)
    multi = serializers.BooleanField(default=False)
    include_all = serializers.BooleanField(default=False)


class DashboardPanelSerializer(serializers.Serializer):
    id = serializers.CharField(max_length=36)
    title = serializers.CharField(max_length=120)
    description = serializers.CharField(max_length=500, allow_blank=True, default="")
    queries = DashboardQuerySerializer(
        many=True,
        min_length=1,
        max_length=MAX_QUERIES_PER_PANEL,
        required=False,
    )
    step_seconds = serializers.IntegerField(
        min_value=DEFAULT_MIN_STEP_SECONDS,
        max_value=24 * 60 * 60,
        default=60,
    )
    visualization = DashboardVisualizationSerializer(required=False)
    layout = serializers.DictField()
    query = serializers.CharField(
        max_length=DEFAULT_MAX_QUERY_LENGTH,
        trim_whitespace=True,
        required=False,
        write_only=True,
    )
    legend_labels = serializers.ListField(
        child=serializers.CharField(max_length=128),
        required=False,
        write_only=True,
    )

    class Meta:
        list_serializer_class = DashboardPanelListSerializer

    def to_internal_value(self, data):
        if isinstance(data, dict):
            data = dict(data)
            if isinstance(data.get("visualization"), str):
                data["visualization"] = {
                    "type": (
                        "time_series"
                        if data["visualization"] == "line"
                        else data["visualization"]
                    )
                }
        return super().to_internal_value(data)

    def to_representation(self, instance):
        if isinstance(instance, dict):
            instance = dict(instance)
            if not instance.get("queries") and instance.get("query"):
                legacy_labels = instance.get("legend_labels") or []
                instance["queries"] = [
                    {
                        "ref_id": "A",
                        "query": instance["query"],
                        "editor_mode": "code",
                        "disabled": False,
                        "legend": " · ".join(
                            f"{{{{{label}}}}}" for label in legacy_labels
                        ),
                    }
                ]
            if isinstance(instance.get("visualization"), str):
                instance["visualization"] = {
                    "type": (
                        "time_series"
                        if instance["visualization"] == "line"
                        else instance["visualization"]
                    )
                }
        return super().to_representation(instance)

    def validate_id(self, value):
        try:
            return str(uuid.UUID(value))
        except (TypeError, ValueError, AttributeError) as exc:
            raise serializers.ValidationError("Must be a valid UUID.") from exc

    def validate_layout(self, value):
        required = ("x", "y", "w", "h")
        if any(key not in value for key in required):
            raise serializers.ValidationError("Layout requires x, y, w, and h.")
        if any(
            isinstance(value[key], bool) or not isinstance(value[key], int)
            for key in required
        ):
            raise serializers.ValidationError("Layout values must be integers.")

        x, y, width, height = (value[key] for key in required)
        if x < 0 or x > 11:
            raise serializers.ValidationError("x must be between 0 and 11.")
        if y < 0:
            raise serializers.ValidationError("y must be zero or greater.")
        if width < 1 or width > 12 or x + width > 12:
            raise serializers.ValidationError("w must fit within the 12-column grid.")
        if height < 2 or height > 12:
            raise serializers.ValidationError("h must be between 2 and 12.")
        return {"x": x, "y": y, "w": width, "h": height}

    def validate(self, attrs):
        queries = attrs.get("queries")
        legacy_query = attrs.pop("query", "")
        legacy_labels = attrs.pop("legend_labels", [])
        if not queries and legacy_query:
            legend = " · ".join(
                f"{{{{{label}}}}}"
                for label in legacy_labels
                if LABEL_NAME_RE.match(label)
            )
            queries = [
                {
                    "ref_id": "A",
                    "query": legacy_query,
                    "editor_mode": "code",
                    "disabled": False,
                    "legend": legend,
                }
            ]
        if not queries:
            raise serializers.ValidationError(
                {"queries": "At least one query is required."}
            )
        ref_ids = [query["ref_id"] for query in queries]
        if len(ref_ids) != len(set(ref_ids)):
            raise serializers.ValidationError(
                {"queries": "Query reference IDs must be unique."}
            )
        attrs["queries"] = queries
        visualization = attrs.get("visualization")
        if not isinstance(visualization, dict):
            attrs["visualization"] = {
                "type": "time_series",
                "unit": "",
                "decimals": None,
                "min": None,
                "max": None,
                "color_scheme": "classic",
                "thresholds": [],
                "legend_placement": "bottom",
                "tooltip_mode": "all",
                "line_width": 2,
                "fill_opacity": 10,
                "stack": False,
            }
        return attrs


class MetricsDashboardSerializer(serializers.ModelSerializer):
    panels = DashboardPanelSerializer(many=True)
    variables = DashboardVariableSerializer(many=True, required=False, default=list)
    created_by = serializers.SerializerMethodField()

    class Meta:
        model = MetricsDashboard
        fields = (
            "id",
            "name",
            "description",
            "created_by",
            "revision",
            "time_range_seconds",
            "refresh_interval_seconds",
            "variables",
            "panels",
            "create_time",
            "update_time",
        )
        read_only_fields = (
            "id",
            "created_by",
            "revision",
            "create_time",
            "update_time",
        )

    def get_created_by(self, obj):
        if obj.created_by is None:
            return None
        return {
            "id": obj.created_by_id,
            "username": obj.created_by.username,
            "display": obj.created_by.display or obj.created_by.username,
        }

    def validate_name(self, value):
        value = value.strip()
        if not value:
            raise serializers.ValidationError("This field may not be blank.")
        return value

    def validate_time_range_seconds(self, value):
        if value not in TIME_RANGE_OPTIONS:
            raise serializers.ValidationError("Unsupported dashboard time range.")
        return value

    def validate_refresh_interval_seconds(self, value):
        if value not in REFRESH_INTERVAL_OPTIONS:
            raise serializers.ValidationError("Unsupported refresh interval.")
        return value

    def validate_panels(self, value):
        panel_ids = [panel["id"] for panel in value]
        if len(panel_ids) != len(set(panel_ids)):
            raise serializers.ValidationError("Panel IDs must be unique.")
        return value

    def validate_variables(self, value):
        if len(value) > MAX_VARIABLES:
            raise serializers.ValidationError(
                f"A dashboard can contain at most {MAX_VARIABLES} variables."
            )
        names = [variable["name"] for variable in value]
        if len(names) != len(set(names)):
            raise serializers.ValidationError("Variable names must be unique.")
        return value
