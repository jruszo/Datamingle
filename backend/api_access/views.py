import datetime
import logging

from django.db import DatabaseError, IntegrityError, transaction
from django.db.models import Q
from django.contrib.auth.models import Group
from common.task_queue import async_task
from drf_spectacular.utils import extend_schema
from rest_framework import permissions, serializers, status, views
from rest_framework.exceptions import PermissionDenied

from common.utils.const import WorkflowAction, WorkflowStatus, WorkflowType
from sql.mailbox import sync_approval_notifications
from sql.models import (
    Instance,
    PermanentTeamGrant,
    PermissionRequest,
    PermissionRequestDuration,
    PermissionRequestSubject,
    PermissionRequestTarget,
    Team,
    TeamMembership,
    TemporaryInstanceGrant,
    TemporaryTeamGrant,
    Users,
    WorkflowAudit,
    WorkflowLog,
)
from sql.notify import notify_for_audit
from sql.utils.team import (
    teams_for_role,
    user_groups,
    user_member_groups,
    user_has_instance_query_access,
    user_has_instance_workflow_access,
)
from sql.utils.workflow_audit import AuditException, get_auditor, reviewable_audit_ids

from api_core.pagination import CustomizedPagination
from api_core.response import success_response

logger = logging.getLogger("default")


def _require_permission(request, permission):
    if request.user.is_superuser or request.user.has_perm(permission):
        return
    raise PermissionDenied(f"Missing required permission: {permission}")


def _today():
    return datetime.date.today()


def _sync_permission_request_approval_notifications(permission_request):
    if not isinstance(permission_request, PermissionRequest):
        logger.warning(
            "Skipping permission request mailbox sync for unsupported workflow type %s",
            type(permission_request).__name__,
        )
        return
    sync_approval_notifications(permission_request)


def _reviewable_request_ids(user):
    if user.is_superuser:
        return list(
            PermissionRequest.objects.values_list("request_id", flat=True).order_by()
        )
    return list(
        WorkflowAudit.objects.filter(
            audit_id__in=reviewable_audit_ids(
                user, workflow_type=WorkflowType.ACCESS_REQUEST
            )
        ).values_list("workflow_id", flat=True)
    )


def _base_request_queryset():
    return PermissionRequest.objects.select_related("team", "instance")


def _request_queryset_for_user(user):
    queryset = _base_request_queryset()
    if user.is_superuser:
        return queryset

    own_ids = list(
        queryset.filter(user_name=user.username).values_list("request_id", flat=True)
    )
    reviewable_ids = _reviewable_request_ids(user)
    visible_ids = sorted(set(own_ids) | set(reviewable_ids))
    return queryset.filter(request_id__in=visible_ids)


def _grant_queryset_for_user(user, model):
    select_related_fields = ["user", "team"]
    if model in {TemporaryInstanceGrant, PermanentTeamGrant}:
        select_related_fields.append("instance")
    queryset = model.objects.select_related(*select_related_fields)
    if user.is_superuser:
        return queryset
    if user.has_perm("sql.query_mgtpriv"):
        group_ids = [group.team_id for group in user_member_groups(user)]
        return queryset.filter(Q(team_id__in=group_ids) | Q(user=user))
    return queryset.filter(
        Q(user=user) | Q(user__isnull=True, team__in=user_member_groups(user))
    )


def _subject_filter(permission_request, user):
    return (
        {"user": user}
        if permission_request.subject_type == PermissionRequestSubject.USER
        else {"user": None}
    )


def _request_subject_is_self(permission_request):
    return permission_request.subject_type == PermissionRequestSubject.USER


def _subject_has_team_access(team, user, access_duration):
    include_temporary = access_duration == PermissionRequestDuration.TEMPORARY
    if not include_temporary:
        return any(group.team_id == team.team_id for group in user_member_groups(user))
    return any(group.team_id == team.team_id for group in user_groups(user))


def _subject_has_instance_access(instance, access_level, subject_type, user, team):
    if subject_type == PermissionRequestSubject.USER:
        return _request_grants_enough(user, instance, access_level)

    if instance.resource_group.filter(team_id=team.team_id).exists():
        return True
    return TemporaryInstanceGrant.objects.filter(
        user__isnull=True,
        team=team,
        instance=instance,
        access_level=access_level,
        is_revoked=False,
        valid_date__gte=_today(),
    ).exists()


def _request_grants_enough(user, instance, access_level):
    if access_level == "query":
        return user_has_instance_query_access(user, instance)
    if access_level == "query_dml":
        return user_has_instance_workflow_access(user, instance, syntax_type=2)
    if access_level == "query_dml_ddl":
        return user_has_instance_workflow_access(user, instance, syntax_type=1)
    return False


def _permission_request_audit_callback(request_id, workflow_status):
    permission_request = PermissionRequest.objects.select_related(
        "team", "instance"
    ).get(request_id=request_id)
    permission_request.status = workflow_status
    permission_request.save(update_fields=["status"])

    if workflow_status != WorkflowStatus.PASSED:
        return

    user = Users.objects.get(username=permission_request.user_name)
    subject_filter = _subject_filter(permission_request, user)

    if permission_request.access_duration == PermissionRequestDuration.PERMANENT:
        if permission_request.target_type == PermissionRequestTarget.TEAM:
            TeamMembership.objects.update_or_create(
                user=user,
                team=permission_request.team,
                defaults={"permission_group": permission_request.permission_group},
            )
            PermanentTeamGrant.objects.get_or_create(
                source_request=permission_request,
                defaults={
                    "user": user,
                    "team": permission_request.team,
                    "permission_group": permission_request.permission_group,
                },
            )
            return

        permission_request.instance.resource_group.add(permission_request.team)
        PermanentTeamGrant.objects.get_or_create(
            source_request=permission_request,
            defaults={
                "user": None,
                "team": permission_request.team,
                "instance": permission_request.instance,
            },
        )
        return

    if permission_request.target_type == PermissionRequestTarget.TEAM:
        if not TemporaryTeamGrant.objects.filter(
            source_request=permission_request
        ).exists():
            TemporaryTeamGrant.objects.create(
                **subject_filter,
                team=permission_request.team,
                permission_group=permission_request.permission_group,
                source_request=permission_request,
                valid_date=permission_request.valid_date,
            )
        return

    if not TemporaryInstanceGrant.objects.filter(
        source_request=permission_request
    ).exists():
        TemporaryInstanceGrant.objects.create(
            **subject_filter,
            team=permission_request.team,
            instance=permission_request.instance,
            access_level=permission_request.access_level,
            source_request=permission_request,
            valid_date=permission_request.valid_date,
        )


class TeamLookupSerializer(serializers.ModelSerializer):
    label = serializers.SerializerMethodField()

    def get_label(self, obj):
        return obj.team_name

    class Meta:
        model = Team
        fields = ("team_id", "team_name", "label")


class PermissionInstanceLookupSerializer(serializers.ModelSerializer):
    teams = serializers.SerializerMethodField()
    label = serializers.SerializerMethodField()

    def get_label(self, obj):
        return f"{obj.instance_name} | {obj.db_type} | {obj.host}"

    def get_teams(self, obj):
        return TeamLookupSerializer(
            obj.resource_group.filter(is_deleted=0).order_by("team_id"),
            many=True,
        ).data

    class Meta:
        model = Instance
        fields = (
            "id",
            "instance_name",
            "db_type",
            "type",
            "host",
            "label",
            "teams",
        )


class PermissionRequestListSerializer(serializers.ModelSerializer):
    team_id = serializers.IntegerField(source="team.team_id", read_only=True)
    team_name = serializers.CharField(source="team.team_name", read_only=True)
    instance_id = serializers.SerializerMethodField()
    instance_name = serializers.SerializerMethodField()

    def get_instance_id(self, obj):
        return obj.instance_id

    def get_instance_name(self, obj):
        return obj.instance.instance_name if obj.instance_id else ""

    class Meta:
        model = PermissionRequest
        fields = (
            "request_id",
            "title",
            "reason",
            "target_type",
            "team_id",
            "team_name",
            "instance_id",
            "instance_name",
            "access_level",
            "subject_type",
            "access_duration",
            "valid_date",
            "user_name",
            "user_display",
            "status",
            "create_time",
        )


class PermissionRequestCreateSerializer(serializers.Serializer):
    title = serializers.CharField(max_length=50)
    reason = serializers.CharField(required=False, allow_blank=True, max_length=255)
    target_type = serializers.ChoiceField(choices=PermissionRequestTarget.choices)
    subject_type = serializers.ChoiceField(
        choices=PermissionRequestSubject.choices,
        default=PermissionRequestSubject.USER,
    )
    access_duration = serializers.ChoiceField(
        choices=PermissionRequestDuration.choices,
        default=PermissionRequestDuration.TEMPORARY,
    )
    team_id = serializers.IntegerField()
    permission_group_id = serializers.IntegerField(required=False)
    instance_id = serializers.IntegerField(required=False)
    access_level = serializers.ChoiceField(
        choices=["query", "query_dml", "query_dml_ddl"],
        required=False,
    )
    valid_date = serializers.DateField()

    def validate(self, attrs):
        target_type = attrs["target_type"]
        subject_type = attrs["subject_type"]
        access_duration = attrs["access_duration"]
        valid_date = attrs["valid_date"]
        request_user = self.context["request"].user

        if valid_date < _today():
            raise serializers.ValidationError(
                {"errors": "valid_date cannot be in the past."}
            )
        if (
            access_duration == PermissionRequestDuration.PERMANENT
            and target_type == PermissionRequestTarget.INSTANCE
            and subject_type == PermissionRequestSubject.USER
        ):
            raise serializers.ValidationError(
                {"errors": ("Permanent instance requests must be made for a team.")}
            )
        if (
            target_type == PermissionRequestTarget.TEAM
            and subject_type != PermissionRequestSubject.USER
        ):
            raise serializers.ValidationError(
                {"errors": "Team membership requests must be for yourself."}
            )

        try:
            team = Team.objects.get(team_id=attrs["team_id"], is_deleted=0)
        except Team.DoesNotExist:
            raise serializers.ValidationError({"errors": "Team does not exist."})
        attrs["team"] = team
        if target_type == PermissionRequestTarget.TEAM:
            permission_group_id = attrs.get("permission_group_id")
            permission_group = (
                Group.objects.exclude(name="superadmin")
                .filter(id=permission_group_id)
                .first()
            )
            if permission_group is None:
                raise serializers.ValidationError(
                    {"permission_group_id": "Select a valid permission group."}
                )
            attrs["permission_group"] = permission_group
        if (
            subject_type == PermissionRequestSubject.TEAM
            and not request_user.is_superuser
            and team not in user_member_groups(request_user)
        ):
            raise serializers.ValidationError(
                {"errors": "You can only request access for your own teams."}
            )

        if target_type == PermissionRequestTarget.TEAM:
            attrs["instance"] = None
            attrs["access_level"] = ""
            return attrs

        instance_id = attrs.get("instance_id")
        access_level = attrs.get("access_level")
        if not instance_id or not access_level:
            raise serializers.ValidationError(
                {
                    "errors": (
                        "instance_id and access_level are required for instance requests."
                    )
                }
            )

        try:
            instance = Instance.objects.get(id=instance_id)
        except Instance.DoesNotExist:
            raise serializers.ValidationError({"errors": "Instance does not exist."})

        if (
            subject_type == PermissionRequestSubject.USER
            and not instance.resource_group.filter(team_id=team.team_id).exists()
        ):
            raise serializers.ValidationError(
                {
                    "errors": (
                        "The selected instance is not associated with the selected team."
                    )
                }
            )
        attrs["instance"] = instance
        return attrs


class PermissionRequestReviewSerializer(serializers.Serializer):
    audit_status = serializers.IntegerField()
    audit_remark = serializers.CharField(required=False, allow_blank=True, default="")


class ActiveGrantSerializer(serializers.Serializer):
    grant_type = serializers.CharField()
    grant_id = serializers.IntegerField()
    subject_type = serializers.CharField()
    user_name = serializers.CharField()
    user_display = serializers.CharField()
    team_id = serializers.IntegerField()
    team_name = serializers.CharField()
    instance_id = serializers.IntegerField(allow_null=True)
    instance_name = serializers.CharField(allow_blank=True)
    access_level = serializers.CharField(allow_blank=True)
    access_duration = serializers.CharField()
    valid_date = serializers.DateField(allow_null=True)
    source_request_id = serializers.IntegerField(allow_null=True)
    create_time = serializers.DateTimeField()


def _serialize_request_detail(permission_request, request_user):
    auditor = get_auditor(workflow=permission_request)
    review_info = auditor.get_review_info()
    logs = []
    audit = permission_request.get_audit()
    if audit:
        logs = [
            {
                "operation_type_desc": log.operation_type_desc,
                "operation_info": log.operation_info,
                "operator_display": log.operator_display,
                "operation_time": log.operation_time,
            }
            for log in WorkflowLog.objects.filter(audit_id=audit.audit_id).order_by(
                "-id"
            )
        ]

    is_can_review = False
    if audit and permission_request.status == WorkflowStatus.WAITING:
        try:
            auditor.can_operate(WorkflowAction.PASS, request_user)
            is_can_review = True
        except AuditException:
            is_can_review = False

    serializer_data = PermissionRequestListSerializer(permission_request).data
    serializer_data["review_info"] = [
        {
            "team_name": node.group.name if node.group else "Auto",
            "is_current_node": node.is_current_node,
            "is_passed_node": node.is_passed_node,
        }
        for node in review_info.nodes
    ]
    serializer_data["is_can_review"] = is_can_review
    serializer_data["logs"] = logs
    return serializer_data


def _grant_subject_payload(grant):
    if grant.user_id:
        return {
            "subject_type": PermissionRequestSubject.USER.value,
            "user_name": grant.user.username,
            "user_display": grant.user.display,
        }
    return {
        "subject_type": PermissionRequestSubject.TEAM.value,
        "user_name": "",
        "user_display": "",
    }


def _serialize_active_grant(grant, grant_type, access_duration):
    subject_payload = _grant_subject_payload(grant)
    if grant_type in {"team", "permanent_team"} and not getattr(
        grant, "instance_id", None
    ):
        return {
            "grant_type": grant_type,
            "grant_id": grant.grant_id,
            **subject_payload,
            "team_id": grant.team.team_id,
            "team_name": grant.team.team_name,
            "instance_id": None,
            "instance_name": "",
            "access_level": "",
            "access_duration": access_duration,
            "valid_date": getattr(grant, "valid_date", None),
            "source_request_id": grant.source_request_id,
            "create_time": grant.create_time,
        }

    return {
        "grant_type": grant_type,
        "grant_id": grant.grant_id,
        **subject_payload,
        "team_id": grant.team.team_id,
        "team_name": grant.team.team_name,
        "instance_id": grant.instance_id,
        "instance_name": grant.instance.instance_name,
        "access_level": getattr(grant, "access_level", ""),
        "access_duration": access_duration,
        "valid_date": getattr(grant, "valid_date", None),
        "source_request_id": grant.source_request_id,
        "create_time": grant.create_time,
    }


class PermissionTeamLookup(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(responses={200: TeamLookupSerializer(many=True)})
    def get(self, request):
        _require_permission(request, "sql.query_applypriv")
        queryset = Team.objects.filter(is_deleted=0).order_by("team_name")
        serializer = TeamLookupSerializer(queryset, many=True)
        return success_response(data=serializer.data)


class PermissionInstanceLookup(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(responses={200: PermissionInstanceLookupSerializer(many=True)})
    def get(self, request):
        _require_permission(request, "sql.query_applypriv")
        queryset = Instance.objects.prefetch_related("resource_group").order_by(
            "instance_name"
        )
        serializer = PermissionInstanceLookupSerializer(queryset, many=True)
        return success_response(data=serializer.data)


class PermissionRequestListCreate(views.APIView):
    permission_classes = [permissions.IsAuthenticated]
    pagination_class = CustomizedPagination

    @extend_schema(responses={200: PermissionRequestListSerializer(many=True)})
    def get(self, request):
        _require_permission(request, "sql.menu_queryapplylist")
        search = request.query_params.get("search", "").strip()
        queryset = _request_queryset_for_user(request.user)
        if search:
            queryset = queryset.filter(
                Q(title__icontains=search)
                | Q(user_display__icontains=search)
                | Q(team__team_name__icontains=search)
                | Q(instance__instance_name__icontains=search)
            )
        queryset = queryset.order_by("-request_id")
        paginator = self.pagination_class()
        page_obj = paginator.paginate_queryset(queryset, request, view=self)
        serializer = PermissionRequestListSerializer(page_obj, many=True)
        return paginator.get_paginated_response(serializer.data)

    @extend_schema(request=PermissionRequestCreateSerializer)
    def post(self, request):
        _require_permission(request, "sql.query_applypriv")
        serializer = PermissionRequestCreateSerializer(
            data=request.data, context={"request": request}
        )
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data
        user = request.user
        subject_type = data["subject_type"]
        access_duration = data["access_duration"]

        if data["target_type"] == PermissionRequestTarget.TEAM:
            if _subject_has_team_access(
                data["team"],
                user,
                access_duration,
            ):
                raise serializers.ValidationError(
                    {
                        "errors": (
                            "The selected subject already has access to this team."
                        )
                    }
                )
            duplicate_qs = PermissionRequest.objects.filter(
                user_name=user.username,
                target_type=PermissionRequestTarget.TEAM,
                team=data["team"],
                subject_type=subject_type,
                access_duration=access_duration,
                status=WorkflowStatus.WAITING,
            )
        else:
            if _subject_has_instance_access(
                data["instance"],
                data["access_level"],
                subject_type,
                user,
                data["team"],
            ):
                raise serializers.ValidationError(
                    {
                        "errors": (
                            "The selected subject already has sufficient access to this instance."
                        )
                    }
                )
            duplicate_qs = PermissionRequest.objects.filter(
                user_name=user.username,
                target_type=PermissionRequestTarget.INSTANCE,
                team=data["team"],
                instance=data["instance"],
                access_level=data["access_level"],
                subject_type=subject_type,
                access_duration=access_duration,
                status=WorkflowStatus.WAITING,
            )

        if duplicate_qs.exists():
            raise serializers.ValidationError(
                {"errors": "A pending request for the same target already exists."}
            )

        permission_request = PermissionRequest(
            team=data["team"],
            permission_group=data.get("permission_group"),
            target_type=data["target_type"],
            instance=data["instance"],
            access_level=data.get("access_level", ""),
            title=data["title"],
            reason=data.get("reason", ""),
            subject_type=subject_type,
            access_duration=access_duration,
            user_name=user.username,
            user_display=user.display,
            valid_date=data["valid_date"],
            status=WorkflowStatus.WAITING,
            audit_auth_groups="",
        )

        auditor = get_auditor(workflow=permission_request)
        try:
            with transaction.atomic():
                auditor.create_audit()
                _permission_request_audit_callback(
                    auditor.workflow.request_id, auditor.audit.current_status
                )
                _sync_permission_request_approval_notifications(auditor.workflow)
                transaction.on_commit(
                    lambda workflow_audit=auditor.audit, request_id=auditor.workflow.request_id: async_task(
                        notify_for_audit,
                        workflow_audit=workflow_audit,
                        timeout=60,
                        task_name=f"permission-request-{request_id}",
                    )
                )
        except AuditException:
            raise serializers.ValidationError(
                {"errors": "Failed to create approval flow, please contact admin."}
            )
        except serializers.ValidationError:
            raise
        except (DatabaseError, IntegrityError, ValueError) as exc:
            logger.exception(
                "Error while creating permission request approval flow "
                "for request_id=%s",
                getattr(permission_request, "request_id", None),
            )
            raise serializers.ValidationError(
                {"errors": "Failed to create approval flow, please contact admin."}
            ) from exc
        except Exception as exc:
            logger.exception(
                "Unexpected error while creating permission request approval flow "
                "for request_id=%s",
                getattr(permission_request, "request_id", None),
            )
            raise serializers.ValidationError(
                {"errors": "Failed to create approval flow, please contact admin."}
            ) from exc
        return success_response(
            data={"request_id": auditor.workflow.request_id},
            status_code=status.HTTP_201_CREATED,
        )


class PermissionRequestDetail(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(responses={200: serializers.JSONField()})
    def get(self, request, request_id):
        _require_permission(request, "sql.menu_queryapplylist")
        try:
            permission_request = _request_queryset_for_user(request.user).get(
                request_id=request_id
            )
        except PermissionRequest.DoesNotExist:
            raise PermissionDenied("You do not have permission to view this request.")

        return success_response(
            data=_serialize_request_detail(permission_request, request.user)
        )


class PermissionRequestReviewCreate(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(request=PermissionRequestReviewSerializer)
    def post(self, request, request_id):
        if not (
            request.user.is_superuser
            or teams_for_role(
                request.user, TeamPermissionGroup.WORKFLOW_APPROVER
            ).exists()
            or request.user.has_perm("sql.query_review")
        ):
            raise PermissionDenied("You do not have permission to review requests.")
        serializer = PermissionRequestReviewSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data

        try:
            permission_request = PermissionRequest.objects.get(request_id=request_id)
        except PermissionRequest.DoesNotExist:
            raise serializers.ValidationError({"errors": "Workflow does not exist."})

        try:
            action = WorkflowAction(int(data["audit_status"]))
        except ValueError as exc:
            raise serializers.ValidationError(
                {"errors": "Invalid audit_status parameter."}
            )

        auditor = get_auditor(workflow=permission_request)
        with transaction.atomic():
            try:
                workflow_audit_detail = auditor.operate(
                    action, request.user, data.get("audit_remark", "")
                )
            except AuditException as exc:
                logger.exception(
                    "Permission request audit failed for request_id=%s user=%s action=%s",
                    request_id,
                    request.user,
                    action,
                )
                raise serializers.ValidationError({"errors": "Audit failed."})
            _permission_request_audit_callback(
                auditor.audit.workflow_id, auditor.audit.current_status
            )
            _sync_permission_request_approval_notifications(auditor.workflow)

        async_task(
            notify_for_audit,
            workflow_audit=auditor.audit,
            workflow_audit_detail=workflow_audit_detail,
            timeout=60,
            task_name=f"permission-request-review-{request_id}",
        )
        return success_response()


class ActiveGrantList(views.APIView):
    permission_classes = [permissions.IsAuthenticated]
    pagination_class = CustomizedPagination

    @extend_schema(responses={200: ActiveGrantSerializer(many=True)})
    def get(self, request):
        _require_permission(request, "sql.menu_queryapplylist")
        search = request.query_params.get("search", "").strip().lower()
        group_grants = _grant_queryset_for_user(
            request.user, TemporaryTeamGrant
        ).filter(is_revoked=False, valid_date__gte=_today())
        instance_grants = _grant_queryset_for_user(
            request.user, TemporaryInstanceGrant
        ).filter(is_revoked=False, valid_date__gte=_today())
        permanent_group_grants = _grant_queryset_for_user(
            request.user, PermanentTeamGrant
        ).filter(is_revoked=False)

        rows = (
            [
                _serialize_active_grant(grant, "team", "temporary")
                for grant in group_grants.order_by("-grant_id")
            ]
            + [
                _serialize_active_grant(grant, "instance", "temporary")
                for grant in instance_grants.order_by("-grant_id")
            ]
            + [
                _serialize_active_grant(grant, "permanent_team", "permanent")
                for grant in permanent_group_grants.order_by("-grant_id")
            ]
        )

        if search:
            rows = [
                row
                for row in rows
                if search
                in " ".join(
                    [
                        row["user_display"],
                        row["user_name"],
                        row["team_name"],
                        row["instance_name"],
                        row["access_level"],
                        row["access_duration"],
                    ]
                ).lower()
            ]

        rows.sort(key=lambda row: row["create_time"], reverse=True)
        paginator = self.pagination_class()
        page_obj = paginator.paginate_queryset(rows, request, view=self)
        serializer = ActiveGrantSerializer(page_obj, many=True)
        return paginator.get_paginated_response(serializer.data)


class ActiveGrantDetail(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    def delete(self, request, grant_type, grant_id):
        _require_permission(request, "sql.query_mgtpriv")

        if grant_type == "team":
            queryset = _grant_queryset_for_user(request.user, TemporaryTeamGrant)
        elif grant_type == "instance":
            queryset = _grant_queryset_for_user(request.user, TemporaryInstanceGrant)
        elif grant_type == "permanent_team":
            queryset = _grant_queryset_for_user(request.user, PermanentTeamGrant)
        else:
            raise serializers.ValidationError({"errors": "Unsupported grant type."})

        try:
            grant = queryset.get(grant_id=grant_id)
        except queryset.model.DoesNotExist:
            raise serializers.ValidationError({"errors": "Grant does not exist."})

        grant.is_revoked = True
        grant.save(update_fields=["is_revoked"])
        if grant_type == "permanent_team":
            if grant.user_id:
                TeamMembership.objects.filter(user=grant.user, team=grant.team).delete()
            elif grant.instance_id:
                grant.instance.resource_group.remove(grant.team)
        return success_response()
