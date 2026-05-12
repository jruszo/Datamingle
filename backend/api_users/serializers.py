from django.contrib.auth.models import Group, Permission
from django.db import transaction
from rest_framework import serializers

from sql.models import Instance, ResourceGroup, Users


class UserManagementGroupSerializer(serializers.ModelSerializer):
    class Meta:
        model = Group
        fields = ("id", "name")


class UserManagementReadSerializer(serializers.ModelSerializer):
    groups = serializers.SerializerMethodField()
    group_ids = serializers.SerializerMethodField()
    is_workos_managed = serializers.SerializerMethodField()

    def get_groups(self, obj):
        groups = obj.groups.order_by("id")
        return UserManagementGroupSerializer(groups, many=True).data

    def get_group_ids(self, obj):
        return list(obj.groups.order_by("id").values_list("id", flat=True))

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
        )


class UserManagementUpdateSerializer(serializers.ModelSerializer):
    group_ids = serializers.PrimaryKeyRelatedField(
        source="groups", queryset=Group.objects.all(), many=True, required=False
    )

    def update(self, instance, validated_data):
        groups = validated_data.pop("groups", None)

        for attr, value in validated_data.items():
            setattr(instance, attr, value)

        with transaction.atomic():
            instance.save()
            if groups is not None:
                instance.groups.set(groups)

        return instance

    class Meta:
        model = Users
        fields = ("group_ids", "is_active")


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

    def get_user_count(self, obj):
        return obj.users_set.count()

    def get_instance_count(self, obj):
        return obj.instance_set.count()

    def create(self, validated_data):
        users = validated_data.pop("users_set", [])
        instances = validated_data.pop("instance_set", [])
        with transaction.atomic():
            group = ResourceGroup.objects.create(**validated_data)
            group.users_set.set(users)
            group.instance_set.set(instances)
        return group

    def update(self, instance, validated_data):
        users = validated_data.pop("users_set", None)
        instances = validated_data.pop("instance_set", None)
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
    is_superuser = serializers.BooleanField()
    is_staff = serializers.BooleanField()
    is_active = serializers.BooleanField()
    groups = CurrentUserGroupSerializer(many=True)
    resource_groups = CurrentUserResourceGroupSerializer(many=True)
    permissions = serializers.ListField(child=serializers.CharField())
