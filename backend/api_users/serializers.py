from django.contrib.auth.models import Group
from django.db import transaction
from rest_framework import serializers

from sql.models import InfrastructureNode, Instance, Team, TeamMembership, Users
from sql.utils.team import set_team_memberships, set_user_resource_memberships


class UserManagementGroupSerializer(serializers.Serializer):
    id = serializers.IntegerField()
    name = serializers.CharField()


class UserManagementTeamSerializer(serializers.Serializer):
    team_id = serializers.IntegerField()
    team_name = serializers.CharField()
    permission_group_id = serializers.IntegerField()
    permission_group_name = serializers.CharField()


class ResourceAccessAssignmentSerializer(serializers.Serializer):
    team_id = serializers.PrimaryKeyRelatedField(
        source="team", queryset=Team.objects.filter(is_deleted=0)
    )
    permission_group_id = serializers.PrimaryKeyRelatedField(
        source="permission_group",
        queryset=Group.objects.exclude(name="superadmin"),
    )


class TeamUserAccessSerializer(serializers.Serializer):
    user_id = serializers.PrimaryKeyRelatedField(
        source="user", queryset=Users.objects.all()
    )
    username = serializers.CharField(required=False, read_only=True)
    display = serializers.CharField(required=False, read_only=True)
    permission_group_id = serializers.PrimaryKeyRelatedField(
        source="permission_group",
        queryset=Group.objects.exclude(name="superadmin"),
    )
    permission_group_name = serializers.CharField(required=False, read_only=True)


class PermissionGroupSerializer(serializers.Serializer):
    id = serializers.IntegerField()
    name = serializers.CharField()
    permissions = serializers.ListField(child=serializers.CharField())


class UserManagementReadSerializer(serializers.ModelSerializer):
    groups = serializers.SerializerMethodField()
    group_ids = serializers.SerializerMethodField()
    teams = serializers.SerializerMethodField()
    team_access = serializers.SerializerMethodField()
    team_ids = serializers.SerializerMethodField()
    is_workos_managed = serializers.SerializerMethodField()

    def _memberships(self, obj):
        cached = getattr(obj, "_prefetched_objects_cache", {})
        if "team_memberships" in cached:
            return [
                membership
                for membership in cached["team_memberships"]
                if membership.team.is_deleted == 0
            ]
        return list(
            obj.team_memberships.select_related("team", "permission_group")
            .filter(team__is_deleted=0)
            .order_by("team__team_name", "team_id")
        )

    def get_groups(self, obj):
        return list(obj.groups.order_by("id").values("id", "name"))

    def get_group_ids(self, obj):
        return list(obj.groups.order_by("id").values_list("id", flat=True))

    def get_teams(self, obj):
        return [
            {
                "team_id": membership.team_id,
                "team_name": membership.team.team_name,
                "permission_group_id": membership.permission_group_id,
                "permission_group_name": membership.permission_group.name,
            }
            for membership in self._memberships(obj)
        ]

    def get_team_access(self, obj):
        return self.get_teams(obj)

    def get_team_ids(self, obj):
        return [membership.team_id for membership in self._memberships(obj)]

    def get_is_workos_managed(self, obj):
        return bool(obj.workos_user_id)

    class Meta:
        model = Users
        fields = (
            "id",
            "username",
            "display",
            "email",
            "is_workos_managed",
            "is_active",
            "is_superuser",
            "is_staff",
            "groups",
            "group_ids",
            "teams",
            "team_access",
            "team_ids",
        )


class UserManagementUpdateSerializer(serializers.ModelSerializer):
    group_ids = serializers.PrimaryKeyRelatedField(
        source="groups",
        queryset=Group.objects.filter(name="superadmin"),
        many=True,
        required=False,
    )
    team_access = ResourceAccessAssignmentSerializer(many=True, required=False)

    def update(self, instance, validated_data):
        groups = validated_data.pop("groups", None)
        team_access = validated_data.pop("team_access", None)
        with transaction.atomic():
            for attr, value in validated_data.items():
                setattr(instance, attr, value)
            instance.save()
            if groups is not None:
                instance.groups.set(groups)
            if team_access is not None:
                set_user_resource_memberships(instance, team_access)
        return instance

    class Meta:
        model = Users
        fields = ("group_ids", "team_access", "is_active")


class WorkOSUserInvitationSerializer(serializers.Serializer):
    email = serializers.EmailField()
    display = serializers.CharField(
        allow_blank=True, max_length=50, required=False, trim_whitespace=True
    )
    group_ids = serializers.PrimaryKeyRelatedField(
        source="groups",
        queryset=Group.objects.filter(name="superadmin"),
        many=True,
        required=False,
    )
    team_access = ResourceAccessAssignmentSerializer(many=True, required=False)

    def validate_email(self, value):
        return value.strip().lower()


class TeamListSerializer(serializers.ModelSerializer):
    user_count = serializers.IntegerField(read_only=True)
    node_count = serializers.IntegerField(read_only=True)
    service_count = serializers.IntegerField(read_only=True)

    class Meta:
        model = Team
        fields = (
            "team_id",
            "team_name",
            "user_count",
            "node_count",
            "service_count",
        )


class TeamDetailSerializer(serializers.ModelSerializer):
    user_access = TeamUserAccessSerializer(many=True, required=False)
    node_ids = serializers.PrimaryKeyRelatedField(
        source="infrastructurenode_set",
        queryset=InfrastructureNode.objects.all(),
        many=True,
        required=False,
    )
    service_ids = serializers.PrimaryKeyRelatedField(
        source="instance_set",
        queryset=Instance.objects.all(),
        many=True,
        required=False,
    )
    user_count = serializers.SerializerMethodField()
    node_count = serializers.SerializerMethodField()
    service_count = serializers.SerializerMethodField()

    def validate_team_name(self, value):
        team_name = value.strip()
        if not team_name:
            raise serializers.ValidationError("Team name cannot be blank.")
        return team_name

    def get_user_count(self, obj):
        return obj.memberships.count()

    def get_node_count(self, obj):
        return obj.infrastructurenode_set.count()

    def get_service_count(self, obj):
        return obj.instance_set.count()

    def to_representation(self, instance):
        representation = super().to_representation(instance)
        memberships = instance.memberships.select_related(
            "user", "permission_group"
        ).order_by("user__display", "user__username", "user_id")
        representation["user_access"] = [
            {
                "user_id": membership.user_id,
                "username": membership.user.username,
                "display": membership.user.display,
                "permission_group_id": membership.permission_group_id,
                "permission_group_name": membership.permission_group.name,
            }
            for membership in memberships
        ]
        return representation

    def create(self, validated_data):
        user_access = validated_data.pop("user_access", [])
        nodes = validated_data.pop("infrastructurenode_set", [])
        services = validated_data.pop("instance_set", [])
        with transaction.atomic():
            team = Team.objects.create(**validated_data)
            set_team_memberships(team, user_access)
            team.infrastructurenode_set.set(nodes)
            team.instance_set.set(services)
        return team

    def update(self, instance, validated_data):
        user_access = validated_data.pop("user_access", None)
        nodes = validated_data.pop("infrastructurenode_set", None)
        services = validated_data.pop("instance_set", None)
        with transaction.atomic():
            for attr, value in validated_data.items():
                setattr(instance, attr, value)
            instance.save()
            if user_access is not None:
                set_team_memberships(instance, user_access)
            if nodes is not None:
                instance.infrastructurenode_set.set(nodes)
            if services is not None:
                instance.instance_set.set(services)
        return instance

    class Meta:
        model = Team
        fields = (
            "team_id",
            "team_name",
            "user_access",
            "node_ids",
            "service_ids",
            "user_count",
            "node_count",
            "service_count",
        )


class TeamUserLookupSerializer(serializers.ModelSerializer):
    label = serializers.SerializerMethodField()

    def get_label(self, obj):
        return obj.display or obj.username

    class Meta:
        model = Users
        fields = ("id", "username", "display", "label")


class TeamNodeLookupSerializer(serializers.ModelSerializer):
    label = serializers.CharField(source="name", read_only=True)

    class Meta:
        model = InfrastructureNode
        fields = ("id", "name", "address", "label")


class TeamServiceLookupSerializer(serializers.ModelSerializer):
    label = serializers.SerializerMethodField()

    def get_label(self, obj):
        return f"{obj.instance_name} | {obj.db_type} | {obj.host}"

    class Meta:
        model = Instance
        fields = ("id", "instance_name", "db_type", "host", "label")


class CurrentUserGroupSerializer(serializers.Serializer):
    id = serializers.IntegerField()
    name = serializers.CharField()


class CurrentUserTeamSerializer(serializers.Serializer):
    team_id = serializers.IntegerField()
    team_name = serializers.CharField()


class CurrentUserSerializer(serializers.Serializer):
    id = serializers.IntegerField()
    username = serializers.CharField()
    display = serializers.CharField(allow_blank=True)
    email = serializers.CharField(allow_blank=True)
    avatar_url = serializers.CharField(allow_blank=True)
    is_workos_managed = serializers.BooleanField()
    is_superuser = serializers.BooleanField()
    is_staff = serializers.BooleanField()
    is_active = serializers.BooleanField()
    groups = CurrentUserGroupSerializer(many=True)
    teams = CurrentUserTeamSerializer(many=True)
    permissions = serializers.ListField(child=serializers.CharField())
