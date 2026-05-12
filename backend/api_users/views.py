from rest_framework import views, generics, status, permissions
from rest_framework.response import Response
from rest_framework.exceptions import PermissionDenied, ValidationError
from drf_spectacular.utils import extend_schema
from django.db.models import Count, Q
from api_users.serializers import (
    UserManagementReadSerializer,
    UserManagementUpdateSerializer,
    GroupSerializer,
    PermissionSerializer,
    ResourceGroupListSerializer,
    ResourceGroupDetailSerializer,
    ResourceGroupUserLookupSerializer,
    ResourceGroupInstanceLookupSerializer,
    CurrentUserSerializer,
)
from api_core.pagination import CustomizedPagination
from api_users.filters import UserFilter
from api_core.response import success_response
from django.contrib.auth.models import Group, Permission
from django.http import Http404
from sql.models import Users, ResourceGroup, Instance
from sql.utils.resource_group import user_groups, active_instance_grants


def _require_any_permission(request, *perm_list):
    if request.user.is_superuser:
        return
    if any(request.user.has_perm(perm) for perm in perm_list):
        return
    raise PermissionDenied(
        f"Missing required permission. Need one of: {', '.join(perm_list)}"
    )


def _require_superuser(request):
    if request.user.is_superuser:
        return
    raise PermissionDenied("Only superusers can access user management.")


def _validate_user_management_lifecycle(request_user, target_user, action):
    if request_user.pk == target_user.pk:
        if action == "delete":
            raise ValidationError("You cannot delete your own account.")
        raise ValidationError("You cannot deactivate your own account.")

    if (
        target_user.is_superuser
        and not Users.objects.filter(is_superuser=True, is_active=True)
        .exclude(pk=target_user.pk)
        .exists()
    ):
        if action == "delete":
            raise ValidationError("You cannot delete the last active superuser.")
        raise ValidationError("You cannot deactivate the last active superuser.")


class CurrentUser(views.APIView):
    """Get bootstrap context for the authenticated user."""

    permission_classes = [permissions.IsAuthenticated]

    @staticmethod
    def _serialize_user(user):
        permissions = set(user.get_all_permissions())
        active_instance_access = active_instance_grants(user)
        if active_instance_access.exists():
            permissions.update(
                {"sql.menu_query", "sql.menu_sqlquery", "sql.query_submit"}
            )
        if active_instance_access.filter(
            access_level__in=["query_dml", "query_dml_ddl"]
        ).exists():
            permissions.update({"sql.menu_sqlworkflow", "sql.sql_submit"})

        payload = {
            "id": user.id,
            "username": user.username,
            "display": user.display,
            "email": user.email or "",
            "avatar_url": user.avatar_url or "",
            "is_workos_managed": bool(user.workos_user_id),
            "is_superuser": user.is_superuser,
            "is_staff": user.is_staff,
            "is_active": user.is_active,
            "groups": list(user.groups.values("id", "name").order_by("id")),
            "resource_groups": list(
                ResourceGroup.objects.filter(
                    group_id__in=[group.group_id for group in user_groups(user)]
                )
                .values("group_id", "group_name")
                .order_by("group_id")
            ),
            "permissions": sorted(permissions),
        }
        serializer = CurrentUserSerializer(payload)
        return serializer.data

    @extend_schema(
        summary="Current User Context",
        responses={200: CurrentUserSerializer},
        description="Get current user profile, groups, resource groups, permissions, and 2FA methods.",
    )
    def get(self, request):
        return success_response(data=self._serialize_user(request.user))


class UserList(generics.ListAPIView):
    """
    List all users or create a new user.
    """

    filterset_class = UserFilter
    pagination_class = CustomizedPagination
    serializer_class = UserManagementReadSerializer
    queryset = Users.objects.prefetch_related("groups").all().order_by("id")

    def get_queryset(self):
        queryset = super().get_queryset()
        search = self.request.query_params.get("search", "").strip()
        ordering = self.request.query_params.get("ordering", "").strip()

        if search:
            search_filter = (
                Q(username__icontains=search)
                | Q(display__icontains=search)
                | Q(email__icontains=search)
            )
            if search.isdigit():
                search_filter |= Q(id=int(search))
            queryset = queryset.filter(search_filter)

        if ordering in {
            "id",
            "-id",
            "username",
            "-username",
            "display",
            "-display",
            "email",
            "-email",
            "is_active",
            "-is_active",
        }:
            queryset = queryset.order_by(ordering, "id")

        return queryset

    @extend_schema(
        summary="User List",
        responses={200: UserManagementReadSerializer},
        description="List all users (filtering, pagination).",
    )
    def get(self, request):
        _require_superuser(request)
        users = self.filter_queryset(self.get_queryset())
        page_user = self.paginate_queryset(queryset=users)
        serializer_obj = self.get_serializer(page_user, many=True)
        return self.get_paginated_response(serializer_obj.data)


class UserDetail(views.APIView):
    """
    User operations.
    """

    serializer_class = UserManagementReadSerializer

    def get_object(self, pk):
        try:
            return Users.objects.prefetch_related("groups").get(pk=pk)
        except Users.DoesNotExist:
            raise Http404

    @extend_schema(
        summary="User Detail",
        responses={200: UserManagementReadSerializer},
        description="Get a single user.",
    )
    def get(self, request, pk):
        _require_superuser(request)
        user = self.get_object(pk)
        return success_response(data=UserManagementReadSerializer(user).data)

    @extend_schema(
        summary="Update User",
        request=UserManagementUpdateSerializer,
        responses={200: UserManagementReadSerializer},
        description="Update a user.",
    )
    def put(self, request, pk):
        _require_superuser(request)
        user = self.get_object(pk)
        serializer = UserManagementUpdateSerializer(
            user, data=request.data, partial=True
        )
        if serializer.is_valid():
            if serializer.validated_data.get("is_active") is False:
                _validate_user_management_lifecycle(request.user, user, "deactivate")
            serializer.save()
            return success_response(
                data=UserManagementReadSerializer(user).data,
                detail="User updated successfully.",
            )
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    @extend_schema(summary="Delete User", description="Delete a user.")
    def delete(self, request, pk):
        _require_superuser(request)
        user = self.get_object(pk)
        _validate_user_management_lifecycle(request.user, user, "delete")
        user.delete()
        return success_response(detail="User deleted successfully.")


class GroupList(generics.ListAPIView):
    """
    List all groups or create a new group.
    """

    pagination_class = CustomizedPagination
    serializer_class = GroupSerializer
    queryset = Group.objects.all().order_by("id")

    def get_queryset(self):
        queryset = super().get_queryset()
        search = self.request.query_params.get("search", "").strip()
        ordering = self.request.query_params.get("ordering", "").strip()
        if search:
            queryset = queryset.filter(name__icontains=search)
        if ordering in {"id", "-id", "name", "-name"}:
            queryset = queryset.order_by(ordering)
        return queryset

    @extend_schema(
        summary="Group List",
        request=GroupSerializer,
        responses={200: GroupSerializer},
        description="List all groups (filtering, pagination).",
    )
    def get(self, request):
        _require_any_permission(
            request,
            "sql.menu_system",
            "auth.view_group",
            "auth.add_group",
            "auth.change_group",
        )
        groups = self.filter_queryset(self.get_queryset())
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
            return success_response(
                data=serializer.data, status_code=status.HTTP_201_CREATED
            )
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
        summary="Group Detail",
        responses={200: GroupSerializer},
        description="Get a group and its assigned permissions.",
    )
    def get(self, request, pk):
        _require_any_permission(
            request,
            "sql.menu_system",
            "auth.view_group",
            "auth.change_group",
            "auth.delete_group",
        )
        group = self.get_object(pk)
        serializer = GroupSerializer(group)
        return success_response(data=serializer.data)

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
            return success_response(data=serializer.data)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    @extend_schema(summary="Delete Group", description="Delete a group.")
    def delete(self, request, pk):
        _require_any_permission(request, "sql.menu_system", "auth.delete_group")
        group = self.get_object(pk)
        group.delete()
        return success_response()


class PermissionList(views.APIView):
    """
    List assignable Django permissions.
    """

    serializer_class = PermissionSerializer

    @extend_schema(
        summary="Permission List",
        responses={200: PermissionSerializer(many=True)},
        description="List all assignable Django permissions.",
    )
    def get(self, request):
        _require_any_permission(
            request,
            "sql.menu_system",
            "auth.view_group",
            "auth.add_group",
            "auth.change_group",
        )
        permissions = Permission.objects.select_related("content_type").order_by(
            "content_type__app_label", "content_type__model", "name"
        )
        serializer = PermissionSerializer(permissions, many=True)
        return success_response(data=serializer.data)


class ResourceGroupList(generics.ListAPIView):
    """
    List all resource groups or create a new resource group.
    """

    pagination_class = CustomizedPagination
    serializer_class = ResourceGroupListSerializer
    queryset = ResourceGroup.objects.filter(is_deleted=0).order_by("group_id")

    def get_queryset(self):
        queryset = (
            super()
            .get_queryset()
            .annotate(
                user_count=Count("users", distinct=True),
                instance_count=Count("instance", distinct=True),
            )
        )
        search = self.request.query_params.get("search", "").strip()
        ordering = self.request.query_params.get("ordering", "").strip()

        if search:
            search_filter = Q(group_name__icontains=search)
            if search.isdigit():
                search_filter |= Q(group_id=int(search))
            queryset = queryset.filter(search_filter)

        if ordering in {
            "group_id",
            "-group_id",
            "group_name",
            "-group_name",
            "user_count",
            "-user_count",
            "instance_count",
            "-instance_count",
        }:
            queryset = queryset.order_by(ordering, "group_id")

        return queryset

    @extend_schema(
        summary="Resource Group List",
        request=ResourceGroupDetailSerializer,
        responses={200: ResourceGroupListSerializer},
        description="List all resource groups (filtering, pagination).",
    )
    def get(self, request):
        _require_any_permission(
            request,
            "sql.menu_system",
            "sql.view_resourcegroup",
            "sql.add_resourcegroup",
            "sql.change_resourcegroup",
        )
        groups = self.filter_queryset(self.get_queryset())
        page_groups = self.paginate_queryset(queryset=groups)
        serializer_obj = self.get_serializer(page_groups, many=True)
        return self.get_paginated_response(serializer_obj.data)

    @extend_schema(
        summary="Create Resource Group",
        request=ResourceGroupDetailSerializer,
        responses={201: ResourceGroupDetailSerializer},
        description="Create a resource group.",
    )
    def post(self, request):
        _require_any_permission(request, "sql.menu_system", "sql.add_resourcegroup")
        serializer = ResourceGroupDetailSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return success_response(
                data=serializer.data, status_code=status.HTTP_201_CREATED
            )
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class ResourceGroupDetail(views.APIView):
    """
    Resource group operations.
    """

    serializer_class = ResourceGroupDetailSerializer

    def get_object(self, pk):
        try:
            return ResourceGroup.objects.get(pk=pk, is_deleted=0)
        except ResourceGroup.DoesNotExist:
            raise Http404

    @extend_schema(
        summary="Resource Group Detail",
        responses={200: ResourceGroupDetailSerializer},
        description="Get a resource group with its assigned users and instances.",
    )
    def get(self, request, pk):
        _require_any_permission(
            request,
            "sql.menu_system",
            "sql.view_resourcegroup",
            "sql.change_resourcegroup",
            "sql.delete_resourcegroup",
        )
        group = self.get_object(pk)
        serializer = self.serializer_class(group)
        return success_response(data=serializer.data)

    @extend_schema(
        summary="Update Resource Group",
        request=ResourceGroupDetailSerializer,
        responses={200: ResourceGroupDetailSerializer},
        description="Update a resource group.",
    )
    def put(self, request, pk):
        _require_any_permission(request, "sql.menu_system", "sql.change_resourcegroup")
        group = self.get_object(pk)
        serializer = self.serializer_class(group, data=request.data)
        if serializer.is_valid():
            serializer.save()
            return success_response(data=serializer.data)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    @extend_schema(
        summary="Delete Resource Group", description="Delete a resource group."
    )
    def delete(self, request, pk):
        _require_any_permission(request, "sql.menu_system", "sql.delete_resourcegroup")
        group = self.get_object(pk)
        group.delete()
        return success_response()


class ResourceGroupUserLookup(views.APIView):
    """List assignable users for resource-group membership."""

    serializer_class = ResourceGroupUserLookupSerializer

    @extend_schema(
        summary="Resource Group User Lookup",
        responses={200: ResourceGroupUserLookupSerializer(many=True)},
        description="List lightweight user records for resource-group membership selection.",
    )
    def get(self, request):
        _require_any_permission(
            request,
            "sql.menu_system",
            "sql.view_resourcegroup",
            "sql.add_resourcegroup",
            "sql.change_resourcegroup",
        )
        search = request.query_params.get("search", "").strip()
        users = Users.objects.all().order_by("display", "username", "id")
        if search:
            users = users.filter(
                Q(display__icontains=search) | Q(username__icontains=search)
            )
        serializer = self.serializer_class(users, many=True)
        return success_response(data=serializer.data)


class ResourceGroupInstanceLookup(views.APIView):
    """List assignable instances for resource-group membership."""

    serializer_class = ResourceGroupInstanceLookupSerializer

    @extend_schema(
        summary="Resource Group Instance Lookup",
        responses={200: ResourceGroupInstanceLookupSerializer(many=True)},
        description="List lightweight instance records for resource-group membership selection.",
    )
    def get(self, request):
        _require_any_permission(
            request,
            "sql.menu_system",
            "sql.view_resourcegroup",
            "sql.add_resourcegroup",
            "sql.change_resourcegroup",
        )
        search = request.query_params.get("search", "").strip()
        instances = Instance.objects.all().order_by("instance_name", "id")
        if search:
            instances = instances.filter(
                Q(instance_name__icontains=search)
                | Q(db_type__icontains=search)
                | Q(host__icontains=search)
            )
        serializer = self.serializer_class(instances, many=True)
        return success_response(data=serializer.data)
