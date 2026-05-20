from django.contrib.auth.models import Group, Permission
from django.db import transaction
from rest_framework import serializers

from sql.models import Instance, ResourceGroup, Users


class UserManagementGroupSerializer(serializers.Serializer):
    id = serializers.IntegerField()
    name = serializers.CharField()
    membership_source = serializers.CharField(default="datamingle")


class UserManagementResourceGroupSerializer(serializers.Serializer):
    group_id = serializers.IntegerField()
    group_name = serializers.CharField()
    membership_source = serializers.CharField(default="datamingle")


class UserManagementReadSerializer(serializers.ModelSerializer):
    groups = serializers.SerializerMethodField()
    group_ids = serializers.SerializerMethodField()
    resource_groups = serializers.SerializerMethodField()
    resource_group_ids = serializers.SerializerMethodField()
    is_workos_managed = serializers.SerializerMethodField()
    is_directory_managed = serializers.SerializerMethodField()

    def _prefetched_groups(self, obj):
        cached_relations = getattr(obj, "_prefetched_objects_cache", {})
        if "groups" in cached_relations:
            return list(cached_relations["groups"])
        return list(obj.groups.order_by("id"))

    def _prefetched_resource_groups(self, obj):
        cached_relations = getattr(obj, "_prefetched_objects_cache", {})
        if "resource_group" in cached_relations:
            return [
                resource_group
                for resource_group in cached_relations["resource_group"]
                if resource_group.is_deleted == 0
            ]
        return list(obj.resource_group.filter(is_deleted=0).order_by("group_id"))

    def _directory_resource_group_ids(self, obj):
        memberships = getattr(obj, "active_workos_directory_memberships", None)
        if memberships is not None:
            return {
                membership.directory_group.resource_group_id
                for membership in memberships
            }

        return set(
            obj.workos_directory_memberships.filter(
                directory_group__is_deleted=False,
                directory_group__resource_group__is_deleted=0,
            )
            .values_list("directory_group__resource_group_id", flat=True)
            .distinct()
        )

    def get_groups(self, obj):
        return [
            {
                "id": group.id,
                "name": group.name,
                "membership_source": "datamingle",
            }
            for group in self._prefetched_groups(obj)
        ]

    def get_group_ids(self, obj):
        return [group.id for group in self._prefetched_groups(obj)]

    def get_resource_groups(self, obj):
        directory_resource_group_ids = self._directory_resource_group_ids(obj)
        return [
            {
                "group_id": group.group_id,
                "group_name": group.group_name,
                "membership_source": (
                    "workos_directory"
                    if group.group_id in directory_resource_group_ids
                    else "datamingle"
                ),
            }
            for group in self._prefetched_resource_groups(obj)
        ]

    def get_resource_group_ids(self, obj):
        return [group.group_id for group in self._prefetched_resource_groups(obj)]

    def get_is_workos_managed(self, obj):
        return bool(obj.workos_user_id)

    def get_is_directory_managed(self, obj):
        return bool(obj.workos_directory_managed)

    class Meta:
        model = Users
        fields = (
            "id",
            "username",
            "display",
            "email",
            "is_workos_managed",
            "is_directory_managed",
            "is_active",
            "is_superuser",
            "is_staff",
            "groups",
            "group_ids",
            "resource_groups",
            "resource_group_ids",
        )


class UserManagementUpdateSerializer(serializers.ModelSerializer):
    group_ids = serializers.PrimaryKeyRelatedField(
        source="groups", queryset=Group.objects.all(), many=True, required=False
    )
    resource_group_ids = serializers.PrimaryKeyRelatedField(
        source="resource_group",
        queryset=ResourceGroup.objects.filter(is_deleted=0),
        many=True,
        required=False,
    )

    def update(self, instance, validated_data):
        groups = validated_data.pop("groups", None)
        resource_groups = validated_data.pop("resource_group", None)
        if resource_groups is not None and instance.workos_directory_managed:
            raise serializers.ValidationError(
                {
                    "resource_group_ids": [
                        "Resource group membership for this user is managed by WorkOS Directory Sync."
                    ]
                }
            )

        for attr, value in validated_data.items():
            setattr(instance, attr, value)

        with transaction.atomic():
            instance.save()
            if groups is not None:
                instance.groups.set(groups)
            if resource_groups is not None:
                instance.resource_group.set(resource_groups)

        return instance

    class Meta:
        model = Users
        fields = ("group_ids", "resource_group_ids", "is_active")


class WorkOSUserInvitationSerializer(serializers.Serializer):
    email = serializers.EmailField()
    display = serializers.CharField(
        allow_blank=True, max_length=50, required=False, trim_whitespace=True
    )
    group_ids = serializers.PrimaryKeyRelatedField(
        source="groups", queryset=Group.objects.all(), many=True, required=False
    )
    resource_group_ids = serializers.PrimaryKeyRelatedField(
        source="resource_groups",
        queryset=ResourceGroup.objects.filter(is_deleted=0),
        many=True,
        required=False,
    )

    def validate_email(self, value):
        return value.strip().lower()


class GroupSerializer(serializers.ModelSerializer):
    class Meta:
        model = Group
        fields = "__all__"


class PermissionSerializer(serializers.ModelSerializer):
    app_label = serializers.CharField(source="content_type.app_label", read_only=True)
    model = serializers.CharField(source="content_type.model", read_only=True)

    class Meta:
        model = Permission
        fields = ("id", "name", "codename", "app_label", "model")


class ResourceGroupListSerializer(serializers.ModelSerializer):
    user_count = serializers.SerializerMethodField()
    instance_count = serializers.SerializerMethodField()

    def get_user_count(self, obj):
        return obj.users_set.count()

    def get_instance_count(self, obj):
        return obj.instance_set.count()

    class Meta:
        model = ResourceGroup
        fields = ("group_id", "group_name", "user_count", "instance_count")


class ResourceGroupDetailSerializer(serializers.ModelSerializer):
    user_ids = serializers.PrimaryKeyRelatedField(
        source="users_set", queryset=Users.objects.all(), many=True, required=False
    )
    instance_ids = serializers.PrimaryKeyRelatedField(
        source="instance_set",
        queryset=Instance.objects.all(),
        many=True,
        required=False,
    )
    user_count = serializers.SerializerMethodField()
    instance_count = serializers.SerializerMethodField()

    def validate_group_name(self, value):
        group_name = value.strip()
        if not group_name:
            raise serializers.ValidationError("Group name cannot be blank.")
        return group_name

    def _validate_directory_managed_user_memberships(self, users, instance=None):
        if users is None:
            return

        requested_ids = {user.id for user in users}
        existing_directory_user_ids = set()
        if instance is not None:
            existing_directory_user_ids = set(
                instance.users_set.filter(workos_directory_managed=True).values_list(
                    "id", flat=True
                )
            )

        added_directory_users = [
            user
            for user in users
            if user.workos_directory_managed
            and user.id not in existing_directory_user_ids
        ]
        removed_directory_user_ids = existing_directory_user_ids - requested_ids

        if added_directory_users or removed_directory_user_ids:
            raise serializers.ValidationError(
                {
                    "user_ids": [
                        "Resource group membership for WorkOS Directory Sync users is managed by WorkOS."
                    ]
                }
            )

    def get_user_count(self, obj):
        return obj.users_set.count()

    def get_instance_count(self, obj):
        return obj.instance_set.count()

    def create(self, validated_data):
        users = validated_data.pop("users_set", [])
        instances = validated_data.pop("instance_set", [])
        self._validate_directory_managed_user_memberships(users)
        with transaction.atomic():
            group = ResourceGroup.objects.create(**validated_data)
            group.users_set.set(users)
            group.instance_set.set(instances)
        return group

    def update(self, instance, validated_data):
        users = validated_data.pop("users_set", None)
        instances = validated_data.pop("instance_set", None)
        self._validate_directory_managed_user_memberships(users, instance)
        for attr, value in validated_data.items():
            setattr(instance, attr, value)
        with transaction.atomic():
            instance.save()
            if users is not None:
                instance.users_set.set(users)
            if instances is not None:
                instance.instance_set.set(instances)
        return instance

    class Meta:
        model = ResourceGroup
        fields = (
            "group_id",
            "group_name",
            "user_ids",
            "instance_ids",
            "user_count",
            "instance_count",
        )


class ResourceGroupUserLookupSerializer(serializers.ModelSerializer):
    label = serializers.SerializerMethodField()

    def get_label(self, obj):
        return obj.display or obj.username

    class Meta:
        model = Users
        fields = ("id", "username", "display", "label")


class ResourceGroupInstanceLookupSerializer(serializers.ModelSerializer):
    label = serializers.SerializerMethodField()

    def get_label(self, obj):
        return f"{obj.instance_name} | {obj.db_type} | {obj.host}"

    class Meta:
        model = Instance
        fields = ("id", "instance_name", "db_type", "host", "label")


class CurrentUserGroupSerializer(serializers.Serializer):
    id = serializers.IntegerField()
    name = serializers.CharField()


class CurrentUserResourceGroupSerializer(serializers.Serializer):
    group_id = serializers.IntegerField()
    group_name = serializers.CharField()


class CurrentUserSerializer(serializers.Serializer):
    id = serializers.IntegerField()
    username = serializers.CharField()
    display = serializers.CharField(allow_blank=True)
    email = serializers.CharField(allow_blank=True)
    avatar_url = serializers.CharField(allow_blank=True)
    is_workos_managed = serializers.BooleanField()
    is_directory_managed = serializers.BooleanField()
    is_superuser = serializers.BooleanField()
    is_staff = serializers.BooleanField()
    is_active = serializers.BooleanField()
    groups = CurrentUserGroupSerializer(many=True)
    resource_groups = CurrentUserResourceGroupSerializer(many=True)
    permissions = serializers.ListField(child=serializers.CharField())
