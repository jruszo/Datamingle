from django.contrib.auth.models import Group, Permission
from django.contrib.auth.password_validation import validate_password
from django.core.exceptions import ValidationError
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


class UserManagementCreateSerializer(serializers.ModelSerializer):
    group_ids = serializers.PrimaryKeyRelatedField(
        source="groups", queryset=Group.objects.all(), many=True, required=False
    )
    email = serializers.EmailField(required=False, allow_blank=True)
    password = serializers.CharField(write_only=True, trim_whitespace=False)

    def validate_display(self, value):
        display = value.strip()
        if not display:
            raise serializers.ValidationError("Display name cannot be blank.")
        return display

    def validate_email(self, value):
        return value.strip()

    def validate_password(self, password):
        try:
            validate_password(password)
        except ValidationError as msg:
            raise serializers.ValidationError(msg.messages)
        return password

    def create(self, validated_data):
        groups = validated_data.pop("groups", [])
        password = validated_data.pop("password")
        with transaction.atomic():
            user = Users(
                is_active=True,
                is_staff=False,
                is_superuser=False,
                **validated_data,
            )
            user.set_password(password)
            user.save()
            user.groups.set(groups)
        return user

    class Meta:
        model = Users
        fields = ("username", "display", "email", "password", "group_ids")


class UserManagementUpdateSerializer(serializers.ModelSerializer):
    group_ids = serializers.PrimaryKeyRelatedField(
        source="groups", queryset=Group.objects.all(), many=True, required=False
    )
    password = serializers.CharField(
        write_only=True, required=False, allow_blank=True, trim_whitespace=False
    )
    email = serializers.EmailField(required=False, allow_blank=True)

    def validate_display(self, value):
        display = value.strip()
        if not display:
            raise serializers.ValidationError("Display name cannot be blank.")
        return display

    def validate_email(self, value):
        return value.strip()

    def validate(self, attrs):
        if self.instance and self.instance.workos_user_id:
            current_display = (self.instance.display or "").strip()
            current_email = (self.instance.email or "").strip()
            next_display = attrs.get("display", current_display)
            next_email = attrs.get("email", current_email)

            if next_display != current_display:
                raise serializers.ValidationError(
                    {"display": "Display name is managed by WorkOS for this user."}
                )
            if next_email != current_email:
                raise serializers.ValidationError(
                    {"email": "Email is managed by WorkOS for this user."}
                )
            if attrs.get("password") not in (None, ""):
                raise serializers.ValidationError(
                    {"password": "Password is managed by WorkOS for this user."}
                )

        password = attrs.get("password")
        if password == "":
            attrs.pop("password")
            return attrs

        if password is not None:
            try:
                validate_password(password, user=self.instance)
            except ValidationError as msg:
                raise serializers.ValidationError({"password": msg.messages})

        return attrs

    def update(self, instance, validated_data):
        groups = validated_data.pop("groups", None)
        password = validated_data.pop("password", None)

        for attr, value in validated_data.items():
            setattr(instance, attr, value)

        if password:
            instance.set_password(password)

        with transaction.atomic():
            instance.save()
            if groups is not None:
                instance.groups.set(groups)

        return instance

    class Meta:
        model = Users
        fields = ("display", "email", "password", "group_ids", "is_active")


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


class UserAuthSerializer(serializers.Serializer):
    password = serializers.CharField(label="Password", trim_whitespace=False)


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
    two_factor_auth_types = serializers.ListField(child=serializers.CharField())


class CurrentUserProfileUpdateSerializer(serializers.Serializer):
    display = serializers.CharField(max_length=50)

    def validate_display(self, value):
        display = value.strip()
        if not display:
            raise serializers.ValidationError("Display name cannot be blank.")
        return display


class CurrentUserPasswordChangeSerializer(serializers.Serializer):
    current_password = serializers.CharField(write_only=True, trim_whitespace=False)
    new_password = serializers.CharField(write_only=True, trim_whitespace=False)
    new_password_confirm = serializers.CharField(write_only=True, trim_whitespace=False)

    def validate_current_password(self, value):
        user = self.context["request"].user
        if not user.check_password(value):
            raise serializers.ValidationError("Incorrect current password.")
        return value

    def validate(self, attrs):
        if attrs["new_password"] != attrs["new_password_confirm"]:
            raise serializers.ValidationError(
                {"new_password_confirm": "Passwords do not match."}
            )

        if attrs["current_password"] == attrs["new_password"]:
            raise serializers.ValidationError(
                {
                    "new_password": (
                        "New password must be different from the current password."
                    )
                }
            )

        try:
            validate_password(attrs["new_password"], user=self.context["request"].user)
        except ValidationError as msg:
            raise serializers.ValidationError({"new_password": msg.messages})

        return attrs


class TwoFASerializer(serializers.Serializer):
    enable = serializers.ChoiceField(
        choices=["true", "false"], label="Enable or disable"
    )
    phone = serializers.CharField(required=False, label="Phone number")
    auth_type = serializers.ChoiceField(
        choices=["totp", "sms"],
        label="Verification type: totp-Google Authenticator, sms-SMS code",
    )

    def validate(self, attrs):
        auth_type = attrs.get("auth_type")
        enable = attrs.get("enable")

        if auth_type == "sms" and enable == "true" and not attrs.get("phone"):
            raise serializers.ValidationError({"errors": "Missing phone."})

        return attrs


class TwoFAStateSerializer(serializers.Serializer):
    pass


class TwoFASaveSerializer(serializers.Serializer):
    key = serializers.CharField(required=False, label="Secret key")
    phone = serializers.CharField(required=False, label="Phone number")
    auth_type = serializers.ChoiceField(
        choices=["disabled", "totp", "sms"],
        label="Verification type: disabled-off, totp-Google Authenticator, sms-SMS code",
    )

    def validate(self, attrs):
        auth_type = attrs.get("auth_type")
        key = attrs.get("key")
        phone = attrs.get("phone")

        if auth_type == "sms" and not phone:
            raise serializers.ValidationError({"errors": "Missing phone."})

        if auth_type == "totp" and not key:
            raise serializers.ValidationError({"errors": "Missing key."})

        return attrs


class TwoFAVerifySerializer(serializers.Serializer):
    otp = serializers.RegexField(
        r"^\d{6}$", label="One-time password / code", max_length=6
    )
    key = serializers.CharField(required=False, label="Secret key")
    phone = serializers.CharField(required=False, label="Phone number")
    auth_type = serializers.ChoiceField(
        choices=["totp", "sms"], label="Verification method"
    )

    def validate(self, attrs):
        auth_type = attrs.get("auth_type")

        if auth_type == "sms" and not attrs.get("phone"):
            raise serializers.ValidationError({"errors": "Missing phone."})

        return attrs
