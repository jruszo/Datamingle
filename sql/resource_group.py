# -*- coding: UTF-8 -*-
import logging
import traceback
from itertools import chain

import simplejson as json
from django.contrib.auth.models import Group
from django.db.models import F, Value, IntegerField
from django.http import HttpResponse
from common.utils.extend_json_encoder import ExtendJSONEncoder
from common.utils.permission import superuser_required
from common.utils.convert import Convert
from sql.models import ResourceGroup, Users, Instance
from sql.utils.resource_group import user_instances
from sql.utils.workflow_audit import Audit

logger = logging.getLogger("default")


@superuser_required
def group(request):
    """Get resource group list."""
    limit = int(request.POST.get("limit"))
    offset = int(request.POST.get("offset"))
    limit = offset + limit
    search = request.POST.get("search", "")

    # Filter search conditions.
    group_obj = ResourceGroup.objects.filter(group_name__icontains=search, is_deleted=0)
    group_count = group_obj.count()
    group_list = group_obj[offset:limit].values(
        "group_id", "group_name", "ding_webhook"
    )

    # Serialize QuerySet.
    rows = [row for row in group_list]

    result = {"total": group_count, "rows": rows}
    # Return query result.
    return HttpResponse(
        json.dumps(result, cls=ExtendJSONEncoder, bigint_as_string=True),
        content_type="application/json",
    )


def associated_objects(request):
    """
    Get objects associated with the resource group.
    type: (0, 'User'), (1, 'Instance')
    """
    group_id = int(request.POST.get("group_id"))
    object_type = request.POST.get("type")
    limit = int(request.POST.get("limit"))
    offset = int(request.POST.get("offset"))
    limit = offset + limit
    search = request.POST.get("search")

    # Get associated data.
    resource_group = ResourceGroup.objects.get(group_id=group_id)
    rows_users = resource_group.users_set.all()
    rows_instances = resource_group.instance_set.all()
    # Apply search filter.
    if search:
        rows_users = rows_users.filter(display__contains=search)
        rows_instances = rows_instances.filter(instance_name__contains=search)
    rows_users = rows_users.annotate(
        object_id=F("id"),
        object_type=Value(0, output_field=IntegerField()),
        object_name=F("display"),
        group_id=F("resource_group__group_id"),
        group_name=F("resource_group__group_name"),
    ).values("object_type", "object_id", "object_name", "group_id", "group_name")
    rows_instances = rows_instances.annotate(
        object_id=F("id"),
        object_type=Value(1, output_field=IntegerField()),
        object_name=F("instance_name"),
        group_id=F("resource_group__group_id"),
        group_name=F("resource_group__group_name"),
    ).values("object_type", "object_id", "object_name", "group_id", "group_name")
    # Filter by object type.
    if object_type == "0":
        rows_obj = rows_users
        count = rows_obj.count()
        rows = [row for row in rows_obj][offset:limit]
    elif object_type == "1":
        rows_obj = rows_instances
        count = rows_obj.count()
        rows = [row for row in rows_obj][offset:limit]
    else:
        rows = list(chain(rows_users, rows_instances))
        count = len(rows)
        rows = rows[offset:limit]
    result = {"status": 0, "msg": "ok", "total": count, "rows": rows}
    return HttpResponse(
        json.dumps(result, cls=ExtendJSONEncoder), content_type="application/json"
    )


def unassociated_objects(request):
    """
    Get objects not associated with the resource group.
    type: (0, 'User'), (1, 'Instance')
    """
    group_id = int(request.POST.get("group_id"))
    object_type = int(request.POST.get("object_type"))
    # Get associated data.
    resource_group = ResourceGroup.objects.get(group_id=group_id)
    if object_type == 0:
        associated_user_ids = [user.id for user in resource_group.users_set.all()]
        rows = (
            Users.objects.exclude(pk__in=associated_user_ids)
            .annotate(object_id=F("pk"), object_name=F("display"))
            .values("object_id", "object_name")
        )
    elif object_type == 1:
        associated_instance_ids = [ins.id for ins in resource_group.instance_set.all()]
        rows = (
            Instance.objects.exclude(pk__in=associated_instance_ids)
            .annotate(object_id=F("pk"), object_name=F("instance_name"))
            .values("object_id", "object_name")
        )
    else:
        raise ValueError("Invalid associated object type")

    rows = [row for row in rows]
    result = {"status": 0, "msg": "ok", "rows": rows, "total": len(rows)}
    return HttpResponse(json.dumps(result), content_type="application/json")


def instances(request):
    """Get instances associated with a resource group."""
    group_name = request.POST.get("group_name")
    group_id = ResourceGroup.objects.get(group_name=group_name).group_id
    tag_code = request.POST.get("tag_code")
    db_type = request.POST.get("db_type")

    # First get all instances associated with the resource group.
    ins = ResourceGroup.objects.get(group_id=group_id).instance_set.all()

    # Filters
    filter_dict = dict()
    # db_type
    if db_type:
        filter_dict["db_type"] = db_type
    if tag_code:
        filter_dict["instance_tag__tag_code"] = tag_code
        filter_dict["instance_tag__active"] = True
    ins = (
        ins.filter(**filter_dict)
        .order_by(Convert("instance_name", "gbk").asc())
        .values("id", "type", "db_type", "instance_name")
    )
    rows = [row for row in ins]
    result = {"status": 0, "msg": "ok", "data": rows}
    return HttpResponse(json.dumps(result), content_type="application/json")


def user_all_instances(request):
    """Get all instances accessible by the user via resource groups."""
    user = request.user
    type = request.GET.get("type")
    db_type = request.GET.getlist("db_type[]")
    tag_codes = request.GET.getlist("tag_codes[]")
    instances = (
        user_instances(user, type, db_type, tag_codes)
        .order_by(Convert("instance_name", "gbk").asc())
        .values("id", "type", "db_type", "instance_name")
    )
    rows = [row for row in instances]
    result = {"status": 0, "msg": "ok", "data": rows}
    return HttpResponse(json.dumps(result), content_type="application/json")


@superuser_required
def addrelation(request):
    """
    Add objects to a resource group.
    type: (0, 'User'), (1, 'Instance')
    """
    group_id = int(request.POST.get("group_id"))
    object_type = request.POST.get("object_type")
    object_list = json.loads(request.POST.get("object_info"))
    try:
        resource_group = ResourceGroup.objects.get(group_id=group_id)
        obj_ids = [int(obj.split(",")[0]) for obj in object_list]
        if object_type == "0":  # User
            resource_group.users_set.add(*Users.objects.filter(pk__in=obj_ids))
        elif object_type == "1":  # Instance
            resource_group.instance_set.add(*Instance.objects.filter(pk__in=obj_ids))
        result = {"status": 0, "msg": "ok"}
    except Exception as e:
        logger.error(traceback.format_exc())
        result = {"status": 1, "msg": str(e)}
    return HttpResponse(json.dumps(result), content_type="application/json")


def auditors(request):
    """Get the approval flow configured for the resource group."""
    group_name = request.POST.get("group_name")
    workflow_type = request.POST["workflow_type"]
    result = {
        "status": 0,
        "msg": "ok",
        "data": {"auditors": "", "auditors_display": ""},
    }
    if group_name:
        group_id = ResourceGroup.objects.get(group_name=group_name).group_id
        audit_auth_groups = Audit.settings(
            group_id=group_id, workflow_type=workflow_type
        )
    else:
        result["status"] = 1
        result["msg"] = "Invalid parameters"
        return HttpResponse(json.dumps(result), content_type="application/json")

    # Get permission group names.
    if audit_auth_groups:
        # Validate configuration.
        for auth_group_id in audit_auth_groups.split(","):
            try:
                Group.objects.get(id=auth_group_id)
            except Exception:
                result["status"] = 1
                result["msg"] = (
                    "Approval flow permission group does not exist. "
                    "Please reconfigure it."
                )
                return HttpResponse(json.dumps(result), content_type="application/json")
        audit_auth_groups_name = "->".join(
            [
                Group.objects.get(id=auth_group_id).name
                for auth_group_id in audit_auth_groups.split(",")
            ]
        )
        result["data"]["auditors"] = audit_auth_groups
        result["data"]["auditors_display"] = audit_auth_groups_name

    return HttpResponse(json.dumps(result), content_type="application/json")


@superuser_required
def changeauditors(request):
    """Set the approval flow for the resource group."""
    auth_groups = request.POST.get("audit_auth_groups")
    group_name = request.POST.get("group_name")
    workflow_type = request.POST.get("workflow_type")
    result = {"status": 0, "msg": "ok", "data": []}

    # Update workflow approval settings.
    group_id = ResourceGroup.objects.get(group_name=group_name).group_id
    audit_auth_groups = [
        str(Group.objects.get(name=auth_group).id)
        for auth_group in auth_groups.split(",")
    ]
    try:
        Audit.change_settings(group_id, workflow_type, ",".join(audit_auth_groups))
    except Exception as msg:
        logger.error(traceback.format_exc())
        result["msg"] = str(msg)
        result["status"] = 1

    # Return result.
    return HttpResponse(json.dumps(result), content_type="application/json")
