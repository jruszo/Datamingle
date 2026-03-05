from rest_framework import views, generics, status
from rest_framework.response import Response
from rest_framework.exceptions import PermissionDenied
from drf_spectacular.utils import extend_schema
from .serializers import (
    UserSerializer,
    UserDetailSerializer,
    GroupSerializer,
    ResourceGroupSerializer,
    TwoFASerializer,
    UserAuthSerializer,
    TwoFAVerifySerializer,
    TwoFASaveSerializer,
    TwoFAStateSerializer,
)
from .pagination import CustomizedPagination
from .permissions import IsOwner
from .filters import UserFilter
from django_redis import get_redis_connection
from django.contrib.auth.models import Group
from django.contrib.auth import authenticate
from django.http import Http404
from sql.models import Users, ResourceGroup, TwoFactorAuthConfig
from common.twofa import get_authenticator
import random
import json
import time


def _require_any_permission(request, *perm_list):
    if request.user.is_superuser:
        return
    if any(request.user.has_perm(perm) for perm in perm_list):
        return
    raise PermissionDenied(
        f"Missing required permission. Need one of: {', '.join(perm_list)}"
    )


class UserList(generics.ListAPIView):
    """
    List all users or create a new user.
    """

    filterset_class = UserFilter
    pagination_class = CustomizedPagination
    serializer_class = UserSerializer
    queryset = Users.objects.all().order_by("id")

    @extend_schema(
        summary="User List",
        request=UserSerializer,
        responses={200: UserSerializer},
        description="List all users (filtering, pagination).",
    )
    def get(self, request):
        _require_any_permission(request, "sql.menu_system", "sql.view_users")
        users = self.filter_queryset(self.queryset)
        page_user = self.paginate_queryset(queryset=users)
        serializer_obj = self.get_serializer(page_user, many=True)
        return self.get_paginated_response(serializer_obj.data)

    @extend_schema(
        summary="Create User",
        request=UserSerializer,
        responses={201: UserSerializer},
        description="Create a user.",
    )
    def post(self, request):
        _require_any_permission(request, "sql.menu_system", "sql.add_users")
        serializer = UserSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class UserDetail(views.APIView):
    """
    User operations.
    """

    serializer_class = UserDetailSerializer

    def get_object(self, pk):
        try:
            return Users.objects.get(pk=pk)
        except Users.DoesNotExist:
            raise Http404

    @extend_schema(
        summary="Update User",
        request=UserDetailSerializer,
        responses={200: UserDetailSerializer},
        description="Update a user.",
    )
    def put(self, request, pk):
        _require_any_permission(request, "sql.menu_system", "sql.change_users")
        user = self.get_object(pk)
        serializer = UserDetailSerializer(user, data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    @extend_schema(summary="Delete User", description="Delete a user.")
    def delete(self, request, pk):
        _require_any_permission(request, "sql.menu_system", "sql.delete_users")
        user = self.get_object(pk)
        user.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)


class GroupList(generics.ListAPIView):
    """
    List all groups or create a new group.
    """

    pagination_class = CustomizedPagination
    serializer_class = GroupSerializer
    queryset = Group.objects.all().order_by("id")

    @extend_schema(
        summary="Group List",
        request=GroupSerializer,
        responses={200: GroupSerializer},
        description="List all groups (filtering, pagination).",
    )
    def get(self, request):
        _require_any_permission(request, "sql.menu_system", "auth.view_group")
        groups = self.filter_queryset(self.queryset)
        page_groups = self.paginate_queryset(queryset=groups)
        serializer_obj = self.get_serializer(page_groups, many=True)
        return self.get_paginated_response(serializer_obj.data)

    @extend_schema(
        summary="Create Group",
        request=GroupSerializer,
        responses={201: GroupSerializer},
        description="Create a group.",
    )
    def post(self, request):
        _require_any_permission(request, "sql.menu_system", "auth.add_group")
        serializer = GroupSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class GroupDetail(views.APIView):
    """
    Group operations.
    """

    serializer_class = GroupSerializer

    def get_object(self, pk):
        try:
            return Group.objects.get(pk=pk)
        except Group.DoesNotExist:
            raise Http404

    @extend_schema(
        summary="Update Group",
        request=GroupSerializer,
        responses={200: GroupSerializer},
        description="Update a group.",
    )
    def put(self, request, pk):
        _require_any_permission(request, "sql.menu_system", "auth.change_group")
        group = self.get_object(pk)
        serializer = GroupSerializer(group, data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    @extend_schema(summary="Delete Group", description="Delete a group.")
    def delete(self, request, pk):
        _require_any_permission(request, "sql.menu_system", "auth.delete_group")
        group = self.get_object(pk)
        group.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)


class ResourceGroupList(generics.ListAPIView):
    """
    List all resource groups or create a new resource group.
    """

    pagination_class = CustomizedPagination
    serializer_class = ResourceGroupSerializer
    queryset = ResourceGroup.objects.all().order_by("group_id")

    @extend_schema(
        summary="Resource Group List",
        request=ResourceGroupSerializer,
        responses={200: ResourceGroupSerializer},
        description="List all resource groups (filtering, pagination).",
    )
    def get(self, request):
        _require_any_permission(request, "sql.menu_system", "sql.view_resourcegroup")
        groups = self.filter_queryset(self.queryset)
        page_groups = self.paginate_queryset(queryset=groups)
        serializer_obj = self.get_serializer(page_groups, many=True)
        return self.get_paginated_response(serializer_obj.data)

    @extend_schema(
        summary="Create Resource Group",
        request=ResourceGroupSerializer,
        responses={201: ResourceGroupSerializer},
        description="Create a resource group.",
    )
    def post(self, request):
        _require_any_permission(request, "sql.menu_system", "sql.add_resourcegroup")
        serializer = ResourceGroupSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class ResourceGroupDetail(views.APIView):
    """
    Resource group operations.
    """

    serializer_class = ResourceGroupSerializer

    def get_object(self, pk):
        try:
            return ResourceGroup.objects.get(pk=pk)
        except ResourceGroup.DoesNotExist:
            raise Http404

    @extend_schema(
        summary="Update Resource Group",
        request=ResourceGroupSerializer,
        responses={200: ResourceGroupSerializer},
        description="Update a resource group.",
    )
    def put(self, request, pk):
        _require_any_permission(request, "sql.menu_system", "sql.change_resourcegroup")
        group = self.get_object(pk)
        serializer = ResourceGroupSerializer(group, data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    @extend_schema(
        summary="Delete Resource Group", description="Delete a resource group."
    )
    def delete(self, request, pk):
        _require_any_permission(request, "sql.menu_system", "sql.delete_resourcegroup")
        group = self.get_object(pk)
        group.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)


class UserAuth(views.APIView):
    """
    User authentication check.
    """

    permission_classes = [IsOwner]

    @extend_schema(
        summary="User Authentication Check",
        request=UserAuthSerializer,
        description="User authentication check.",
    )
    def post(self, request):
        # Parameter validation
        serializer = UserAuthSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        result = {"status": 0, "msg": "Authentication successful."}
        engineer = request.data["engineer"]
        password = request.data["password"]

        user = authenticate(username=engineer, password=password)
        if not user:
            result = {"status": 1, "msg": "Incorrect username or password."}

        return Response(result)


class TwoFA(views.APIView):
    """
    Configure 2FA.
    """

    permission_classes = [IsOwner]

    @extend_schema(
        summary="Configure 2FA", request=TwoFASerializer, description="Configure 2FA."
    )
    def post(self, request):
        # Parameter validation
        serializer = TwoFASerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        engineer = request.data["engineer"]
        enable = request.data["enable"]
        auth_type = request.data["auth_type"]
        user = Users.objects.get(username=engineer)

        authenticator = get_authenticator(user=user, auth_type=auth_type)
        if enable == "true":
            if auth_type == "totp":
                # Enable 2FA - generate secret key first
                result = authenticator.generate_key()
            elif auth_type == "sms":
                # Enable 2FA - send SMS verification code first
                phone = request.data["phone"]
                otp = "{:06d}".format(random.randint(0, 999999))
                result = authenticator.get_captcha(phone=phone, otp=otp)
                if result["status"] == 0:
                    r = get_redis_connection("default")
                    data = {"otp": otp, "update_time": int(time.time())}
                    r.set(f"captcha-{phone}", json.dumps(data), 300)
            else:
                # Enable 2FA
                result = authenticator.enable()
        else:
            result = authenticator.disable(auth_type)

        return Response(result)


class TwoFAState(views.APIView):
    """
    Query user 2FA configuration status.
    """

    permission_classes = [IsOwner]

    @extend_schema(
        summary="Query 2FA Configuration",
        request=TwoFAStateSerializer,
        description="Query 2FA configuration status.",
    )
    def post(self, request):
        # Parameter validation
        serializer = TwoFAStateSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        result = {"status": 0, "msg": "ok", "data": {}}
        engineer = request.data["engineer"]
        user = Users.objects.get(username=engineer)
        configs = TwoFactorAuthConfig.objects.filter(user=user)
        result["data"]["totp"] = (
            "enabled" if configs.filter(auth_type="totp") else "disabled"
        )
        result["data"]["sms"] = (
            "enabled" if configs.filter(auth_type="sms") else "disabled"
        )

        return Response(result)


class TwoFASave(views.APIView):
    """
    Save 2FA configuration (TOTP).
    """

    permission_classes = [IsOwner]

    @extend_schema(
        summary="Save 2FA Configuration",
        request=TwoFASaveSerializer,
        description="Save 2FA configuration.",
    )
    def post(self, request):
        # Parameter validation
        serializer = TwoFASaveSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        engineer = request.data["engineer"]
        auth_type = request.data["auth_type"]
        key = request.data["key"] if "key" in request.data.keys() else None
        phone = request.data["phone"] if "phone" in request.data.keys() else None
        user = Users.objects.get(username=engineer)

        authenticator = get_authenticator(user=user, auth_type=auth_type)
        if auth_type == "sms":
            result = authenticator.save(phone)
        else:
            result = authenticator.save(key)

        return Response(result)


class TwoFAVerify(views.APIView):
    """
    Verify 2FA code.
    """

    permission_classes = [IsOwner]

    @extend_schema(
        summary="Verify 2FA Code",
        request=TwoFAVerifySerializer,
        description="Verify 2FA code.",
    )
    def post(self, request):
        # Parameter validation
        serializer = TwoFAVerifySerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        engineer = request.data["engineer"]
        otp = request.data["otp"]
        key = request.data["key"] if "key" in request.data.keys() else None
        phone = request.data["phone"] if "phone" in request.data.keys() else None
        user = Users.objects.get(username=engineer)
        twofa_config = TwoFactorAuthConfig.objects.filter(user=user)
        if not twofa_config and not key:
            return Response({"status": 1, "msg": "User has not configured 2FA."})

        auth_type = request.data["auth_type"]
        authenticator = get_authenticator(user=user, auth_type=auth_type)
        if auth_type == "sms":
            result = authenticator.verify(otp, phone)
        else:
            result = authenticator.verify(otp, key)

        return Response(result)
