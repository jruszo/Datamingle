import simplejson as json
from django.http import HttpResponse

from common.utils.const import WorkflowStatus
from common.utils.extend_json_encoder import ExtendJSONEncoder, ExtendJSONEncoderFTime
from sql.models import WorkflowAudit, WorkflowLog
from sql.utils.workflow_audit import reviewable_audit_ids


# Get pending review list
def lists(request):
    # Get user info
    user = request.user

    limit = int(request.POST.get("limit"))
    offset = int(request.POST.get("offset"))
    workflow_type = int(request.POST.get("workflow_type"))
    limit = offset + limit
    search = request.POST.get("search", "")

    # Return only records in the user's resource groups waiting for this reviewer
    workflow_audit = WorkflowAudit.objects.filter(
        audit_id__in=reviewable_audit_ids(user, workflow_type=workflow_type or None),
        workflow_title__icontains=search,
        current_status=WorkflowStatus.WAITING,
    )
    # Filter by workflow type
    if workflow_type != 0:
        workflow_audit = workflow_audit.filter(workflow_type=workflow_type)

    audit_list_count = workflow_audit.count()
    audit_list = workflow_audit.order_by("-audit_id")[offset:limit].values(
        "audit_id",
        "workflow_type",
        "workflow_title",
        "create_user_display",
        "create_time",
        "current_status",
        "audit_auth_groups",
        "current_audit",
        "group_name",
    )

    # Serialize QuerySet
    rows = [row for row in audit_list]

    result = {"total": audit_list_count, "rows": rows}
    # Return query result
    return HttpResponse(
        json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
        content_type="application/json",
    )


# Get workflow logs
def log(request):
    workflow_id = request.POST.get("workflow_id")
    workflow_type = request.POST.get("workflow_type")
    try:
        audit_id = WorkflowAudit.objects.get(
            workflow_id=workflow_id, workflow_type=workflow_type
        ).audit_id
        workflow_logs = (
            WorkflowLog.objects.filter(audit_id=audit_id)
            .order_by("-id")
            .values(
                "operation_type_desc",
                "operation_info",
                "operator_display",
                "operation_time",
            )
        )
        count = WorkflowLog.objects.filter(audit_id=audit_id).count()
    except Exception:
        workflow_logs = []
        count = 0

    # Serialize QuerySet
    rows = [row for row in workflow_logs]
    result = {"total": count, "rows": rows}
    # Return query result
    return HttpResponse(
        json.dumps(result, cls=ExtendJSONEncoderFTime, bigint_as_string=True),
        content_type="application/json",
    )
