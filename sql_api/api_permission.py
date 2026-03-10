import datetime

from django.contrib.auth.models import Group
from django.db import transaction
from django.db.models import Q
from django_q.tasks import async_task
from drf_spectacular.utils import extend_schema
from rest_framework import permissions, serializers, status, views
from rest_framework.exceptions import PermissionDenied

from common.utils.const import WorkflowAction, WorkflowStatus, WorkflowType
from sql.models import (
    Instance,
    PermissionRequest,
    PermissionRequestTarget,
    ResourceGroup,
    TemporaryInstanceGrant,
    TemporaryResourceGroupGrant,
    Users,
    WorkflowAudit,
    WorkflowLog,
)
from sql.notify import notify_for_audit
from sql.utils.resource_group import (
    user_groups,
    user_member_groups,
    user_has_group_instance_access,
    user_has_instance_query_access,
    user_has_instance_workflow_access,
)
from sql.utils.workflow_audit import AuditException, get_auditor

from .pagination import CustomizedPagination
from .response import success_response


def _require_permission(request, permission):
    if request.user.is_superuser or request.user.has_perm(permission):
        return
    raise PermissionDenied(f"Missing required permission: {permission}")


def _today():
    return datetime.date.today()


def _user_auth_group_ids(user):
    if user.is_superuser:
        return list(Group.objects.values_list("id", flat=True))
    return list(user.groups.values_list("id", flat=True))


def _reviewable_request_ids(user):
    if user.is_superuser:
        return list(
            PermissionRequest.objects.values_list("request_id", flat=True).order_by()
        )
    if not user.has_perm("sql.query_review"):
        return []
    group_ids = [group.group_id for group in user_member_groups(user)]
    auth_group_ids = _user_auth_group_ids(user)
    return list(
        WorkflowAudit.objects.filter(
            workflow_type=WorkflowType.ACCESS_REQUEST,
            group_id__in=group_ids,
            current_status=WorkflowStatus.WAITING,
            current_audit__in=auth_group_ids,
        ).values_list("workflow_id", flat=True)
    )


def _base_request_queryset():
    return PermissionRequest.objects.select_related("resource_group", "instance")


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
    select_related_fields = ["user", "resource_group"]
    if model is TemporaryInstanceGrant:
        select_related_fields.append("instance")
    queryset = model.objects.select_related(*select_related_fields)
    if user.is_superuser:
        return queryset
    if user.has_perm("sql.query_mgtpriv"):
        group_ids = [group.group_id for group in user_member_groups(user)]
        return queryset.filter(Q(resource_group_id__in=group_ids) | Q(user=user))
    return queryset.filter(user=user)


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
        "resource_group", "instance"
    ).get(request_id=request_id)
    permission_request.status = workflow_status
    permission_request.save(update_fields=["status"])

    if workflow_status != WorkflowStatus.PASSED:
        return

    user = Users.objects.get(username=permission_request.user_name)
    if permission_request.target_type == PermissionRequestTarget.RESOURCE_GROUP:
        if not TemporaryResourceGroupGrant.objects.filter(
            source_request=permission_request
        ).exists():
            TemporaryResourceGroupGrant.objects.create(
                user=user,
                resource_group=permission_request.resource_group,
                source_request=permission_request,
                valid_date=permission_request.valid_date,
            )
        return

    if not TemporaryInstanceGrant.objects.filter(
        source_request=permission_request
    ).exists():
        TemporaryInstanceGrant.objects.create(
            user=user,
            resource_group=permission_request.resource_group,
            instance=permission_request.instance,
            access_level=permission_request.access_level,
            source_request=permission_request,
            valid_date=permission_request.valid_date,
        )


class ResourceGroupLookupSerializer(serializers.ModelSerializer):
    label = serializers.SerializerMethodField()

    def get_label(self, obj):
        return obj.group_name

    class Meta:
        model = ResourceGroup
        fields = ("group_id", "group_name", "label")


class PermissionInstanceLookupSerializer(serializers.ModelSerializer):
    resource_groups = serializers.SerializerMethodField()
    label = serializers.SerializerMethodField()

    def get_label(self, obj):
        return f"{obj.instance_name} | {obj.db_type} | {obj.host}"

    def get_resource_groups(self, obj):
        return ResourceGroupLookupSerializer(
            obj.resource_group.filter(is_deleted=0).order_by("group_id"),
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
            "resource_groups",
        )


class PermissionRequestListSerializer(serializers.ModelSerializer):
    resource_group_id = serializers.IntegerField(
        source="resource_group.group_id", read_only=True
    )
    resource_group_name = serializers.CharField(
        source="resource_group.group_name", read_only=True
    )
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
            "resource_group_id",
            "resource_group_name",
            "instance_id",
            "instance_name",
            "access_level",
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
    resource_group_id = serializers.IntegerField()
    instance_id = serializers.IntegerField(required=False)
    access_level = serializers.ChoiceField(
        choices=["query", "query_dml", "query_dml_ddl"],
        required=False,
    )
    valid_date = serializers.DateField()

    def validate(self, attrs):
        target_type = attrs["target_type"]
        valid_date = attrs["valid_date"]

        if valid_date < _today():
            raise serializers.ValidationError(
                {"errors": "valid_date cannot be in the past."}
            )

        try:
            resource_group = ResourceGroup.objects.get(
                group_id=attrs["resource_group_id"], is_deleted=0
            )
        except ResourceGroup.DoesNotExist:
            raise serializers.ValidationError(
                {"errors": "Resource group does not exist."}
            )
        attrs["resource_group"] = resource_group

        if target_type == PermissionRequestTarget.RESOURCE_GROUP:
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

        if not instance.resource_group.filter(
            group_id=resource_group.group_id
        ).exists():
            raise serializers.ValidationError(
                {
                    "errors": (
                        "The selected instance is not associated with the selected resource group."
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
    user_name = serializers.CharField()
    user_display = serializers.CharField()
    resource_group_id = serializers.IntegerField()
    resource_group_name = serializers.CharField()
    instance_id = serializers.IntegerField(allow_null=True)
    instance_name = serializers.CharField(allow_blank=True)
    access_level = serializers.CharField(allow_blank=True)
    valid_date = serializers.DateField()
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
            "group_name": node.group.name if node.group else "Auto",
            "is_current_node": node.is_current_node,
            "is_passed_node": node.is_passed_node,
        }
        for node in review_info.nodes
    ]
    serializer_data["is_can_review"] = is_can_review
    serializer_data["logs"] = logs
    return serializer_data


def _serialize_active_grant(grant, grant_type):
    if grant_type == "resource_group":
        return {
            "grant_type": grant_type,
            "grant_id": grant.grant_id,
            "user_name": grant.user.username,
            "user_display": grant.user.display,
            "resource_group_id": grant.resource_group.group_id,
            "resource_group_name": grant.resource_group.group_name,
            "instance_id": None,
            "instance_name": "",
            "access_level": "",
            "valid_date": grant.valid_date,
            "source_request_id": grant.source_request_id,
            "create_time": grant.create_time,
        }

    return {
        "grant_type": grant_type,
        "grant_id": grant.grant_id,
        "user_name": grant.user.username,
        "user_display": grant.user.display,
        "resource_group_id": grant.resource_group.group_id,
        "resource_group_name": grant.resource_group.group_name,
        "instance_id": grant.instance_id,
        "instance_name": grant.instance.instance_name,
        "access_level": grant.access_level,
        "valid_date": grant.valid_date,
        "source_request_id": grant.source_request_id,
        "create_time": grant.create_time,
    }


class PermissionResourceGroupLookup(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(responses={200: ResourceGroupLookupSerializer(many=True)})
    def get(self, request):
        _require_permission(request, "sql.query_applypriv")
        queryset = ResourceGroup.objects.filter(is_deleted=0).order_by("group_name")
        serializer = ResourceGroupLookupSerializer(queryset, many=True)
        return success_response(data=serializer.data)


class PermissionInstanceLookup(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(responses={200: PermissionInstanceLookupSerializer(many=True)})
    def get(self, request):
        _require_permission(request, "sql.query_applypriv")
        queryset = (
            Instance.objects.filter(resource_group__is_deleted=0)
            .prefetch_related("resource_group")
            .distinct()
            .order_by("instance_name")
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
                | Q(resource_group__group_name__icontains=search)
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
        serializer = PermissionRequestCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data
        user = request.user

        if data["target_type"] == PermissionRequestTarget.RESOURCE_GROUP:
            if any(
                group.group_id == data["resource_group"].group_id
                for group in user_groups(user)
            ):
                raise serializers.ValidationError(
                    {"errors": "You already have access to this resource group."}
                )
            duplicate_qs = PermissionRequest.objects.filter(
                user_name=user.username,
                target_type=PermissionRequestTarget.RESOURCE_GROUP,
                resource_group=data["resource_group"],
                status=WorkflowStatus.WAITING,
            )
        else:
            if _request_grants_enough(user, data["instance"], data["access_level"]):
                raise serializers.ValidationError(
                    {"errors": "You already have sufficient access to this instance."}
                )
            duplicate_qs = PermissionRequest.objects.filter(
                user_name=user.username,
                target_type=PermissionRequestTarget.INSTANCE,
                resource_group=data["resource_group"],
                instance=data["instance"],
                access_level=data["access_level"],
                status=WorkflowStatus.WAITING,
            )

        if duplicate_qs.exists():
            raise serializers.ValidationError(
                {"errors": "A pending request for the same target already exists."}
            )

        permission_request = PermissionRequest(
            resource_group=data["resource_group"],
            target_type=data["target_type"],
            instance=data["instance"],
            access_level=data.get("access_level", ""),
            title=data["title"],
            reason=data.get("reason", ""),
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
        except AuditException:
            raise serializers.ValidationError(
                {"errors": "Failed to create approval flow, please contact admin."}
            )

        _permission_request_audit_callback(
            auditor.workflow.request_id, auditor.audit.current_status
        )
        async_task(
            notify_for_audit,
            workflow_audit=auditor.audit,
            timeout=60,
            task_name=f"permission-request-{auditor.workflow.request_id}",
        )
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
        _require_permission(request, "sql.query_review")
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
                {"errors": f"Invalid audit_status parameter, {str(exc)}"}
            )

        auditor = get_auditor(workflow=permission_request)
        with transaction.atomic():
            try:
                workflow_audit_detail = auditor.operate(
                    action, request.user, data.get("audit_remark", "")
                )
            except AuditException as exc:
                raise serializers.ValidationError(
                    {"errors": f"Audit failed: {str(exc)}"}
                )
            _permission_request_audit_callback(
                auditor.audit.workflow_id, auditor.audit.current_status
            )

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
            request.user, TemporaryResourceGroupGrant
        ).filter(is_revoked=False, valid_date__gte=_today())
        instance_grants = _grant_queryset_for_user(
            request.user, TemporaryInstanceGrant
        ).filter(is_revoked=False, valid_date__gte=_today())

        rows = [
            _serialize_active_grant(grant, "resource_group")
            for grant in group_grants.order_by("-grant_id")
        ] + [
            _serialize_active_grant(grant, "instance")
            for grant in instance_grants.order_by("-grant_id")
        ]

        if search:
            rows = [
                row
                for row in rows
                if search
                in " ".join(
                    [
                        row["user_display"],
                        row["user_name"],
                        row["resource_group_name"],
                        row["instance_name"],
                        row["access_level"],
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

        if grant_type == "resource_group":
            queryset = _grant_queryset_for_user(
                request.user, TemporaryResourceGroupGrant
            )
        elif grant_type == "instance":
            queryset = _grant_queryset_for_user(request.user, TemporaryInstanceGrant)
        else:
            raise serializers.ValidationError({"errors": "Unsupported grant type."})

        try:
            grant = queryset.get(grant_id=grant_id)
        except queryset.model.DoesNotExist:
            raise serializers.ValidationError({"errors": "Grant does not exist."})

        grant.is_revoked = True
        grant.save(update_fields=["is_revoked"])
        return success_response()
