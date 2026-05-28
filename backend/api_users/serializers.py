from django.contrib.auth.models import Group
from django.db import transaction
from rest_framework import serializers

from sql.models import (
    Instance,
    ResourceAccessRole,
    ResourceGroup,
    Users,
)
from sql.utils.resource_group import (
    access_role_label,
    set_resource_group_memberships,
    set_user_resource_memberships,
)


class UserManagementGroupSerializer(serializers.Serializer):
    id = serializers.IntegerField()
    name = serializers.CharField()
    membership_source = serializers.CharField(default="datamingle")


class UserManagementResourceGroupSerializer(serializers.Serializer):
    group_id = serializers.IntegerField()
    group_name = serializers.CharField()
    access_role = serializers.CharField(default=ResourceAccessRole.QUERY)
    access_role_label = serializers.CharField(default="Query")
    membership_source = serializers.CharField(default="datamingle")


class ResourceAccessAssignmentSerializer(serializers.Serializer):
    resource_group_id = serializers.IntegerField()
    access_role = serializers.ChoiceField(choices=ResourceAccessRole.choices)


class ResourceGroupUserAccessSerializer(serializers.Serializer):
    user_id = serializers.IntegerField()
    username = serializers.CharField(required=False)
    display = serializers.CharField(required=False, allow_blank=True)
    access_role = serializers.ChoiceField(
        choices=ResourceAccessRole.choices, required=False
    )
    access_role_label = serializers.CharField(required=False)
    membership_source = serializers.CharField(required=False)


class AccessRoleSerializer(serializers.Serializer):
    code = serializers.CharField()
    label = serializers.CharField()
    description = serializers.CharField()
    rank = serializers.IntegerField()


class UserManagementReadSerializer(serializers.ModelSerializer):
    groups = serializers.SerializerMethodField()
    group_ids = serializers.SerializerMethodField()
    resource_groups = serializers.SerializerMethodField()
    resource_access = serializers.SerializerMethodField()
    resource_group_ids = serializers.SerializerMethodField()
    is_workos_managed = serializers.SerializerMethodField()
    is_directory_managed = serializers.SerializerMethodField()

    def _prefetched_groups(self, obj):
        cached_relations = getattr(obj, "_prefetched_objects_cache", {})
        if "groups" in cached_relations:
            return list(cached_relations["groups"])
        return list(obj.groups.order_by("id"))

    def _prefetched_resource_groups(self, obj):
        memberships = self._prefetched_resource_memberships(obj)
        if memberships:
            return [membership.resource_group for membership in memberships]
        cached_relations = getattr(obj, "_prefetched_objects_cache", {})
        if "resource_group" in cached_relations:
            return [
                resource_group
                for resource_group in cached_relations["resource_group"]
                if resource_group.is_deleted == 0
            ]
        return list(obj.resource_group.filter(is_deleted=0).order_by("group_id"))

    def _prefetched_resource_memberships(self, obj):
        cached_relations = getattr(obj, "_prefetched_objects_cache", {})
        if "resource_group_memberships" in cached_relations:
            return [
                membership
                for membership in cached_relations["resource_group_memberships"]
                if membership.resource_group.is_deleted == 0
            ]
        return list(
            obj.resource_group_memberships.select_related("resource_group")
            .filter(resource_group__is_deleted=0)
            .order_by("resource_group__group_name", "resource_group_id")
        )

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
        memberships = self._prefetched_resource_memberships(obj)
        if memberships:
            return [
                {
                    "group_id": membership.resource_group.group_id,
                    "group_name": membership.resource_group.group_name,
                    "access_role": membership.access_role,
                    "access_role_label": access_role_label(membership.access_role),
                    "membership_source": membership.membership_source,
                }
                for membership in memberships
            ]

        directory_resource_group_ids = self._directory_resource_group_ids(obj)
        return [
            {
                "group_id": group.group_id,
                "group_name": group.group_name,
                "access_role": ResourceAccessRole.QUERY,
                "access_role_label": access_role_label(ResourceAccessRole.QUERY),
                "membership_source": (
                    "workos_directory"
                    if group.group_id in directory_resource_group_ids
                    else "datamingle"
                ),
            }
            for group in self._prefetched_resource_groups(obj)
        ]

    def get_resource_group_ids(self, obj):
        return [
            membership.resource_group_id
            for membership in self._prefetched_resource_memberships(obj)
        ] or [group.group_id for group in self._prefetched_resource_groups(obj)]

    def get_resource_access(self, obj):
        return self.get_resource_groups(obj)

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
            "resource_access",
            "resource_group_ids",
        )


class UserManagementUpdateSerializer(serializers.ModelSerializer):
    group_ids = serializers.PrimaryKeyRelatedField(
        source="groups", queryset=Group.objects.all(), many=True, required=False
    )
    resource_group_ids = serializers.PrimaryKeyRelatedField(
        source="legacy_resource_groups",
        queryset=ResourceGroup.objects.filter(is_deleted=0),
        many=True,
        required=False,
    )
    resource_access = ResourceAccessAssignmentSerializer(many=True, required=False)

    def update(self, instance, validated_data):
        groups = validated_data.pop("groups", None)
        resource_groups = validated_data.pop("legacy_resource_groups", None)
        resource_access = validated_data.pop("resource_access", None)
        if (
            resource_groups is not None or resource_access is not None
        ) and instance.workos_directory_managed:
            raise serializers.ValidationError(
                {
                    "resource_access": [
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
            if resource_access is not None:
                set_user_resource_memberships(
                    instance,
                    [
                        {
                            "resource_group_id": row["resource_group_id"],
                            "access_role": row["access_role"],
                        }
                        for row in resource_access
                    ],
                )
            elif resource_groups is not None:
                set_user_resource_memberships(
                    instance,
                    [
                        {
                            "resource_group": resource_group,
                            "access_role": ResourceAccessRole.QUERY,
                        }
                        for resource_group in resource_groups
                    ],
                )

        return instance

    class Meta:
        model = Users
        fields = ("group_ids", "resource_group_ids", "resource_access", "is_active")


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
    resource_access = ResourceAccessAssignmentSerializer(many=True, required=False)

    def validate_email(self, value):
        return value.strip().lower()


class ResourceGroupListSerializer(serializers.ModelSerializer):
    user_count = serializers.SerializerMethodField()
    instance_count = serializers.SerializerMethodField()

    def get_user_count(self, obj):
        return obj.memberships.filter(resource_group__is_deleted=0).count()

    def get_instance_count(self, obj):
        return obj.instance_set.count()

    class Meta:
        model = ResourceGroup
        fields = ("group_id", "group_name", "user_count", "instance_count")


class ResourceGroupDetailSerializer(serializers.ModelSerializer):
    user_ids = serializers.PrimaryKeyRelatedField(
        source="users_set", queryset=Users.objects.all(), many=True, required=False
    )
    user_access = ResourceGroupUserAccessSerializer(many=True, required=False)
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
        return obj.memberships.filter(resource_group__is_deleted=0).count()

    def get_instance_count(self, obj):
        return obj.instance_set.count()

    def to_representation(self, instance):
        representation = super().to_representation(instance)
        memberships = (
            instance.memberships.select_related("user")
            .filter(resource_group__is_deleted=0)
            .order_by("user__display", "user__username", "user_id")
        )
        representation["user_ids"] = [membership.user_id for membership in memberships]
        representation["user_access"] = [
            {
                "user_id": membership.user_id,
                "username": membership.user.username,
                "display": membership.user.display,
                "access_role": membership.access_role,
                "access_role_label": access_role_label(membership.access_role),
                "membership_source": membership.membership_source,
            }
            for membership in memberships
        ]
        return representation

    def create(self, validated_data):
        users = validated_data.pop("users_set", [])
        user_access = validated_data.pop("user_access", None)
        instances = validated_data.pop("instance_set", [])
        self._validate_directory_managed_user_memberships(users)
        with transaction.atomic():
            group = ResourceGroup.objects.create(**validated_data)
            if user_access is not None:
                user_ids = [row["user_id"] for row in user_access]
                access_users = list(Users.objects.filter(id__in=user_ids))
                self._validate_directory_managed_user_memberships(access_users)
                set_resource_group_memberships(
                    group,
                    [
                        {
                            "user_id": row["user_id"],
                            "access_role": row.get(
                                "access_role", ResourceAccessRole.QUERY
                            ),
                        }
                        for row in user_access
                    ],
                )
            else:
                set_resource_group_memberships(
                    group,
                    [
                        {"user": user, "access_role": ResourceAccessRole.QUERY}
                        for user in users
                    ],
                )
            group.instance_set.set(instances)
        return group

    def update(self, instance, validated_data):
        users = validated_data.pop("users_set", None)
        user_access = validated_data.pop("user_access", None)
        instances = validated_data.pop("instance_set", None)
        self._validate_directory_managed_user_memberships(users, instance)
        for attr, value in validated_data.items():
            setattr(instance, attr, value)
        with transaction.atomic():
            instance.save()
            if user_access is not None:
                user_ids = [row["user_id"] for row in user_access]
                access_users = list(Users.objects.filter(id__in=user_ids))
                self._validate_directory_managed_user_memberships(
                    access_users, instance
                )
                set_resource_group_memberships(instance, user_access)
            elif users is not None:
                set_resource_group_memberships(
                    instance,
                    [
                        {"user": user, "access_role": ResourceAccessRole.QUERY}
                        for user in users
                    ],
                )
            if instances is not None:
                instance.instance_set.set(instances)
        return instance

    class Meta:
        model = ResourceGroup
        fields = (
            "group_id",
            "group_name",
            "user_ids",
            "user_access",
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
