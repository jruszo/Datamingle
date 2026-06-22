from django.contrib.auth.models import Group
from django.contrib.auth.password_validation import validate_password
from django.db import transaction
from rest_framework import serializers
from allauth.account.models import EmailAddress

from common.team_permissions import (
    assignable_team_permissions,
    normalize_permission_codes,
)
from sql.models import InfrastructureNode, Instance, Team, TeamMembership, Users
from sql.utils.team import set_team_memberships


class TeamUserAccessSerializer(serializers.Serializer):
    user_id = serializers.PrimaryKeyRelatedField(
        source="user", queryset=Users.objects.all()
    )
    username = serializers.CharField(required=False, read_only=True)
    display = serializers.CharField(required=False, read_only=True)
    permission_level_id = serializers.PrimaryKeyRelatedField(
        source="permission_level",
        queryset=Group.objects.exclude(name="superadmin"),
    )
    permission_level_name = serializers.CharField(required=False, read_only=True)


class PermissionLevelSerializer(serializers.Serializer):
    id = serializers.IntegerField()
    name = serializers.CharField()
    permissions = serializers.ListField(child=serializers.CharField())
    membership_count = serializers.IntegerField()


class PermissionLevelWriteSerializer(serializers.Serializer):
    name = serializers.CharField(max_length=150)
    permission_codes = serializers.ListField(
        child=serializers.CharField(), allow_empty=True
    )

    def validate_name(self, value):
        name = value.strip()
        if not name:
            raise serializers.ValidationError("Permission level name cannot be blank.")
        if name.lower() == "superadmin":
            raise serializers.ValidationError("The superadmin group is protected.")
        queryset = Group.objects.filter(name__iexact=name)
        if self.instance:
            queryset = queryset.exclude(pk=self.instance.pk)
        if queryset.exists():
            raise serializers.ValidationError(
                "A permission level with this name already exists."
            )
        return name

    def validate_permission_codes(self, value):
        try:
            return sorted(normalize_permission_codes(value))
        except ValueError as exc:
            raise serializers.ValidationError(str(exc)) from exc

    def _save(self, group, validated_data):
        group.name = validated_data["name"]
        group.save()
        group.permissions.set(
            assignable_team_permissions().filter(
                codename__in=validated_data["permission_codes"]
            )
        )
        return group

    def create(self, validated_data):
        return self._save(Group(), validated_data)

    def update(self, instance, validated_data):
        return self._save(instance, validated_data)


class UserManagementReadSerializer(serializers.ModelSerializer):
    teams = serializers.SerializerMethodField()
    team_ids = serializers.SerializerMethodField()

    def _memberships(self, obj):
        cached = getattr(obj, "_prefetched_objects_cache", {})
        if "team_memberships" in cached:
            return [
                membership
                for membership in cached["team_memberships"]
                if membership.team.is_deleted == 0
            ]
        return list(
            obj.team_memberships.select_related("team", "permission_level")
            .filter(team__is_deleted=0)
            .order_by("team__team_name", "team_id")
        )

    def get_teams(self, obj):
        return [
            {
                "team_id": membership.team_id,
                "team_name": membership.team.team_name,
                "permission_level_id": membership.permission_level_id,
                "permission_level_name": membership.permission_level.name,
            }
            for membership in self._memberships(obj)
        ]

    def get_team_ids(self, obj):
        return [membership.team_id for membership in self._memberships(obj)]

    class Meta:
        model = Users
        fields = (
            "id",
            "username",
            "display",
            "email",
            "is_active",
            "is_superuser",
            "is_staff",
            "teams",
            "team_ids",
        )


class UserManagementUpdateSerializer(serializers.ModelSerializer):
    def update(self, instance, validated_data):
        with transaction.atomic():
            for attr, value in validated_data.items():
                setattr(instance, attr, value)
            instance.save()
        return instance

    class Meta:
        model = Users
        fields = ("is_active",)


class UserManagementCreateSerializer(serializers.Serializer):
    email = serializers.EmailField()
    display = serializers.CharField(
        allow_blank=True, max_length=50, required=False, trim_whitespace=True
    )
    password = serializers.CharField(write_only=True, trim_whitespace=False)
    is_active = serializers.BooleanField(required=False, default=True)

    def validate_email(self, value):
        email = value.strip().lower()
        if Users.objects.filter(email__iexact=email).exists():
            raise serializers.ValidationError(
                "A Datamingle user already uses that email address."
            )
        if Users.objects.filter(username__iexact=email).exists():
            raise serializers.ValidationError(
                "A Datamingle username already exists for that email address."
            )
        return email

    def validate_password(self, value):
        validate_password(value)
        return value

    @staticmethod
    def _default_display(email):
        local_part = email.split("@", 1)[0].replace(".", " ").replace("_", " ").strip()
        return local_part.title()[:50] or email[:50]

    def create(self, validated_data):
        email = validated_data["email"]
        display = validated_data.get("display") or self._default_display(email)
        with transaction.atomic():
            user = Users.objects.create_user(
                username=email,
                email=email,
                password=validated_data["password"],
                display=display,
                is_active=validated_data.get("is_active", True),
            )
            EmailAddress.objects.create(
                user=user,
                email=email,
                primary=True,
                verified=True,
            )
        return user


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
            "user", "permission_level"
        ).order_by("user__display", "user__username", "user_id")
        representation["user_access"] = [
            {
                "user_id": membership.user_id,
                "username": membership.user.username,
                "display": membership.user.display,
                "permission_level_id": membership.permission_level_id,
                "permission_level_name": membership.permission_level.name,
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
    is_superuser = serializers.BooleanField()
    is_staff = serializers.BooleanField()
    is_active = serializers.BooleanField()
    groups = CurrentUserGroupSerializer(many=True)
    teams = CurrentUserTeamSerializer(many=True)
    permissions = serializers.ListField(child=serializers.CharField())
