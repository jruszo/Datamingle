from rest_framework import views, generics, status, permissions
from rest_framework.response import Response
from rest_framework.exceptions import PermissionDenied, ValidationError
from drf_spectacular.utils import extend_schema
from django.db.models import Count, Prefetch, Q
from django.db.models.deletion import ProtectedError
from django.contrib.auth.models import Group
from django.http import Http404
from api_users.serializers import (
    UserManagementReadSerializer,
    UserManagementUpdateSerializer,
    UserManagementCreateSerializer,
    PermissionLevelSerializer,
    PermissionLevelWriteSerializer,
    TeamListSerializer,
    TeamDetailSerializer,
    TeamUserLookupSerializer,
    TeamNodeLookupSerializer,
    TeamServiceLookupSerializer,
    CurrentUserSerializer,
)
from common.auth import init_user
from common.team_permissions import permission_catalog
from api_core.pagination import CustomizedPagination
from api_users.filters import UserFilter
from api_core.response import success_response
from sql.models import (
    InfrastructureNode,
    Instance,
    Team,
    TeamMembership,
    TeamPermissionGroup,
    Users,
    WorkflowAuditSetting,
)
from sql.utils.team import (
    active_instance_grants,
    teams_for_role,
    user_groups,
    user_has_resource_role,
)


def _user_management_prefetches():
    return (
        Prefetch(
            "team_memberships",
            queryset=TeamMembership.objects.select_related("team", "permission_level")
            .filter(team__is_deleted=0)
            .order_by("team__team_name", "team_id"),
        ),
    )


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


def _user_can_access_teams(user):
    return (
        user.is_superuser
        or user.has_perm("sql.menu_system")
        or user.has_perm("sql.view_team")
        or teams_for_role(user, TeamPermissionGroup.RESOURCE_OWNER).exists()
    )


def _require_team_manager(request, team):
    if request.user.is_superuser or request.user.has_perm("sql.menu_system"):
        return
    if user_has_resource_role(request.user, team, TeamPermissionGroup.RESOURCE_OWNER):
        return
    raise PermissionDenied("You do not own this team.")


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
            "is_superuser": user.is_superuser,
            "is_staff": user.is_staff,
            "is_active": user.is_active,
            "groups": list(
                user.groups.filter(name="superadmin")
                .values("id", "name")
                .order_by("id")
            ),
            "teams": list(
                Team.objects.filter(
                    team_id__in=[team.team_id for team in user_groups(user)]
                )
                .values("team_id", "team_name")
                .order_by("team_id")
            ),
            "permissions": sorted(permissions),
        }
        serializer = CurrentUserSerializer(payload)
        return serializer.data

    @extend_schema(
        summary="Current User Context",
        responses={200: CurrentUserSerializer},
        description="Get current user profile, groups, teams, permissions, and 2FA methods.",
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
    queryset = (
        Users.objects.prefetch_related(*_user_management_prefetches())
        .all()
        .order_by("id")
    )

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

    @extend_schema(
        summary="Create User",
        request=UserManagementCreateSerializer,
        responses={201: UserManagementReadSerializer},
        description="Create a local Datamingle email/password user.",
    )
    def post(self, request):
        _require_superuser(request)
        serializer = UserManagementCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        user = serializer.save()
        init_user(user)
        return success_response(
            data=UserManagementReadSerializer(user).data,
            detail="User created.",
            status_code=status.HTTP_201_CREATED,
        )


def _serialize_permission_level(group):
    permissions = sorted(
        f"{permission.content_type.app_label}.{permission.codename}"
        for permission in group.permissions.filter(
            content_type__app_label="sql"
        ).select_related("content_type")
    )
    return {
        "id": group.id,
        "name": group.name,
        "permissions": permissions,
        "membership_count": group.team_memberships.count(),
    }


def _approval_level_is_referenced(group):
    group_id = str(group.id)
    return any(
        group_id
        in {
            token.strip()
            for token in setting.audit_auth_groups.split(",")
            if token.strip()
        }
        for setting in WorkflowAuditSetting.objects.exclude(audit_auth_groups="")
    )


class PermissionLevelList(views.APIView):
    permission_classes = [permissions.IsAuthenticated]
    serializer_class = PermissionLevelSerializer

    @extend_schema(
        summary="Permission Levels",
        responses={200: PermissionLevelSerializer(many=True)},
    )
    def get(self, request):
        levels = (
            Group.objects.exclude(name="superadmin")
            .prefetch_related("permissions__content_type")
            .order_by("name", "id")
        )
        return success_response(
            data=[_serialize_permission_level(level) for level in levels]
        )

    @extend_schema(request=PermissionLevelWriteSerializer)
    def post(self, request):
        _require_superuser(request)
        serializer = PermissionLevelWriteSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        level = serializer.save()
        return success_response(
            data=_serialize_permission_level(level),
            status_code=status.HTTP_201_CREATED,
        )


class PermissionLevelDetail(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    def get_object(self, pk):
        try:
            return (
                Group.objects.exclude(name="superadmin")
                .prefetch_related("permissions__content_type")
                .get(pk=pk)
            )
        except Group.DoesNotExist as exc:
            raise Http404 from exc

    def get(self, request, pk):
        return success_response(data=_serialize_permission_level(self.get_object(pk)))

    @extend_schema(request=PermissionLevelWriteSerializer)
    def put(self, request, pk):
        _require_superuser(request)
        level = self.get_object(pk)
        serializer = PermissionLevelWriteSerializer(
            level, data=request.data, partial=False
        )
        serializer.is_valid(raise_exception=True)
        return success_response(data=_serialize_permission_level(serializer.save()))

    def delete(self, request, pk):
        _require_superuser(request)
        level = self.get_object(pk)
        if _approval_level_is_referenced(level):
            raise ValidationError(
                "Reassign approval flows before deleting this permission level."
            )
        try:
            level.delete()
        except ProtectedError as exc:
            raise ValidationError(
                "Reassign memberships, requests, and grants before deleting this permission level."
            ) from exc
        return success_response()


class AvailableTeamPermissions(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request):
        _require_superuser(request)
        return success_response(data=permission_catalog())


class UserDetail(views.APIView):
    """
    User operations.
    """

    serializer_class = UserManagementReadSerializer

    def get_object(self, pk):
        try:
            return Users.objects.prefetch_related(*_user_management_prefetches()).get(
                pk=pk
            )
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


class TeamList(generics.ListAPIView):
    """
    List all teams or create a new team.
    """

    pagination_class = CustomizedPagination
    serializer_class = TeamListSerializer
    queryset = Team.objects.filter(is_deleted=0).order_by("team_id")

    def get_queryset(self):
        queryset = (
            super()
            .get_queryset()
            .annotate(
                user_count=Count("memberships", distinct=True),
                node_count=Count("infrastructurenode", distinct=True),
                service_count=Count("instance", distinct=True),
            )
        )
        search = self.request.query_params.get("search", "").strip()
        ordering = self.request.query_params.get("ordering", "").strip()

        if search:
            search_filter = Q(team_name__icontains=search)
            if search.isdigit():
                search_filter |= Q(team_id=int(search))
            queryset = queryset.filter(search_filter)

        if ordering in {
            "team_id",
            "-team_id",
            "team_name",
            "-team_name",
            "user_count",
            "-user_count",
            "node_count",
            "-node_count",
            "service_count",
            "-service_count",
        }:
            queryset = queryset.order_by(ordering, "team_id")

        return queryset

    @extend_schema(
        summary="Team List",
        request=TeamDetailSerializer,
        responses={200: TeamListSerializer},
        description="List all teams (filtering, pagination).",
    )
    def get(self, request):
        if not request.user.is_superuser and not _user_can_access_teams(request.user):
            raise PermissionDenied("You do not have permission to access teams.")
        groups = self.filter_queryset(self.get_queryset())
        if not request.user.is_superuser and not request.user.has_perm(
            "sql.menu_system"
        ):
            groups = groups.filter(
                team_id__in=[
                    team.team_id
                    for team in teams_for_role(
                        request.user, TeamPermissionGroup.RESOURCE_OWNER
                    )
                ]
            )
        page_groups = self.paginate_queryset(queryset=groups)
        serializer_obj = self.get_serializer(page_groups, many=True)
        return self.get_paginated_response(serializer_obj.data)

    @extend_schema(
        summary="Create Team",
        request=TeamDetailSerializer,
        responses={201: TeamDetailSerializer},
        description="Create a team.",
    )
    def post(self, request):
        _require_any_permission(request, "sql.menu_system", "sql.add_team")
        serializer = TeamDetailSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return success_response(
                data=serializer.data, status_code=status.HTTP_201_CREATED
            )
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class TeamDetail(views.APIView):
    """
    Team operations.
    """

    serializer_class = TeamDetailSerializer

    def get_object(self, pk):
        try:
            return Team.objects.get(pk=pk, is_deleted=0)
        except Team.DoesNotExist:
            raise Http404

    @extend_schema(
        summary="Team Detail",
        responses={200: TeamDetailSerializer},
        description="Get a team with its assigned users and instances.",
    )
    def get(self, request, pk):
        group = self.get_object(pk)
        if not _user_can_access_teams(request.user):
            raise PermissionDenied("You do not have permission to access teams.")
        if not request.user.is_superuser and not request.user.has_perm(
            "sql.menu_system"
        ):
            _require_team_manager(request, group)
        serializer = self.serializer_class(group)
        return success_response(data=serializer.data)

    @extend_schema(
        summary="Update Team",
        request=TeamDetailSerializer,
        responses={200: TeamDetailSerializer},
        description="Update a team.",
    )
    def put(self, request, pk):
        group = self.get_object(pk)
        _require_team_manager(request, group)
        serializer = self.serializer_class(group, data=request.data)
        if serializer.is_valid():
            serializer.save()
            return success_response(data=serializer.data)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    @extend_schema(summary="Delete Team", description="Delete a team.")
    def delete(self, request, pk):
        _require_any_permission(request, "sql.menu_system", "sql.delete_team")
        group = self.get_object(pk)
        group.delete()
        return success_response()


class TeamUserLookup(views.APIView):
    """List assignable users for team membership."""

    serializer_class = TeamUserLookupSerializer

    @extend_schema(
        summary="Team User Lookup",
        responses={200: TeamUserLookupSerializer(many=True)},
        description="List lightweight user records for team membership selection.",
    )
    def get(self, request):
        if not _user_can_access_teams(request.user):
            raise PermissionDenied("You do not have permission to access teams.")
        search = request.query_params.get("search", "").strip()
        users = Users.objects.all().order_by("display", "username", "id")
        if search:
            users = users.filter(
                Q(display__icontains=search) | Q(username__icontains=search)
            )
        serializer = self.serializer_class(users, many=True)
        return success_response(data=serializer.data)


class TeamNodeLookup(views.APIView):
    """List assignable infrastructure nodes for Team assignment."""

    serializer_class = TeamNodeLookupSerializer

    @extend_schema(
        summary="Team Node Lookup",
        responses={200: TeamNodeLookupSerializer(many=True)},
    )
    def get(self, request):
        if not _user_can_access_teams(request.user):
            raise PermissionDenied("You do not have permission to access teams.")
        search = request.query_params.get("search", "").strip()
        nodes = InfrastructureNode.objects.all().order_by("name", "id")
        if search:
            nodes = nodes.filter(
                Q(name__icontains=search) | Q(address__icontains=search)
            )
        serializer = self.serializer_class(nodes, many=True)
        return success_response(data=serializer.data)


class TeamServiceLookup(views.APIView):
    """List assignable database services for Team assignment."""

    serializer_class = TeamServiceLookupSerializer

    def get(self, request):
        if not _user_can_access_teams(request.user):
            raise PermissionDenied("You do not have permission to access teams.")
        search = request.query_params.get("search", "").strip()
        services = Instance.objects.all().order_by("instance_name", "id")
        if search:
            services = services.filter(
                Q(instance_name__icontains=search)
                | Q(db_type__icontains=search)
                | Q(host__icontains=search)
            )
        serializer = self.serializer_class(services, many=True)
        return success_response(data=serializer.data)
