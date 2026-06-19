# -*- coding: UTF-8 -*-
import logging
from itertools import chain

import simplejson as json
from django.db.models import F, Value, IntegerField
from django.http import HttpResponse
from common.utils.extend_json_encoder import ExtendJSONEncoder
from common.utils.permission import superuser_required
from common.utils.convert import Convert
from sql.models import Team, Users, Instance
from sql.utils.team import (
    permission_group_label,
    normalize_permission_group_sequence,
    user_instances,
)
from sql.utils.workflow_audit import Audit

logger = logging.getLogger("default")


@superuser_required
def group(request):
    """Get team list."""
    limit = int(request.POST.get("limit"))
    offset = int(request.POST.get("offset"))
    limit = offset + limit
    search = request.POST.get("search", "")

    # Filter search conditions.
    group_obj = Team.objects.filter(team_name__icontains=search, is_deleted=0)
    group_count = group_obj.count()
    group_list = group_obj[offset:limit].values("team_id", "team_name")

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
    Get objects associated with the team.
    type: (0, 'User'), (1, 'Instance')
    """
    team_id = int(request.POST.get("team_id"))
    object_type = request.POST.get("type")
    limit = int(request.POST.get("limit"))
    offset = int(request.POST.get("offset"))
    limit = offset + limit
    search = request.POST.get("search")

    # Get associated data.
    team = Team.objects.get(team_id=team_id)
    rows_users = team.users_set.all()
    rows_instances = team.instance_set.all()
    # Apply search filter.
    if search:
        rows_users = rows_users.filter(display__contains=search)
        rows_instances = rows_instances.filter(instance_name__contains=search)
    rows_users = rows_users.annotate(
        object_id=F("id"),
        object_type=Value(0, output_field=IntegerField()),
        object_name=F("display"),
        team_id=F("team__team_id"),
        team_name=F("team__team_name"),
    ).values("object_type", "object_id", "object_name", "team_id", "team_name")
    rows_instances = rows_instances.annotate(
        object_id=F("id"),
        object_type=Value(1, output_field=IntegerField()),
        object_name=F("instance_name"),
        team_id=F("team__team_id"),
        team_name=F("team__team_name"),
    ).values("object_type", "object_id", "object_name", "team_id", "team_name")
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
    Get objects not associated with the team.
    type: (0, 'User'), (1, 'Instance')
    """
    team_id = int(request.POST.get("team_id"))
    object_type = int(request.POST.get("object_type"))
    # Get associated data.
    team = Team.objects.get(team_id=team_id)
    if object_type == 0:
        associated_user_ids = [user.id for user in team.users_set.all()]
        rows = (
            Users.objects.exclude(pk__in=associated_user_ids)
            .annotate(object_id=F("pk"), object_name=F("display"))
            .values("object_id", "object_name")
        )
    elif object_type == 1:
        associated_instance_ids = [ins.id for ins in team.instance_set.all()]
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
    """Get instances associated with a team."""
    team_name = request.POST.get("team_name")
    team_id = Team.objects.get(team_name=team_name).team_id
    tag_code = request.POST.get("tag_code")
    db_type = request.POST.get("db_type")

    # First get all instances associated with the team.
    ins = Team.objects.get(team_id=team_id).instance_set.all()

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
    """Get all instances accessible by the user via teams."""
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
    Add objects to a team.
    type: (0, 'User'), (1, 'Instance')
    """
    team_id = int(request.POST.get("team_id"))
    object_type = request.POST.get("object_type")
    object_list = json.loads(request.POST.get("object_info"))
    try:
        team = Team.objects.get(team_id=team_id)
        obj_ids = [int(obj.split(",")[0]) for obj in object_list]
        if object_type == "0":  # User
            team.users_set.add(*Users.objects.filter(pk__in=obj_ids))
        elif object_type == "1":  # Instance
            team.instance_set.add(*Instance.objects.filter(pk__in=obj_ids))
        result = {"status": 0, "msg": "ok"}
    except Exception as e:
        logger.exception("Failed to save team objects")
        result = {"status": 1, "msg": "Failed to save team objects."}
    return HttpResponse(json.dumps(result), content_type="application/json")


def auditors(request):
    """Get the approval flow configured for the team."""
    team_name = request.POST.get("team_name")
    workflow_type = request.POST["workflow_type"]
    result = {
        "status": 0,
        "msg": "ok",
        "data": {"auditors": "", "auditors_display": ""},
    }
    if team_name:
        team_id = Team.objects.get(team_name=team_name).team_id
        audit_auth_groups = Audit.settings(team_id=team_id, workflow_type=workflow_type)
    else:
        result["status"] = 1
        result["msg"] = "Invalid parameters"
        return HttpResponse(json.dumps(result), content_type="application/json")

    if audit_auth_groups:
        audit_auth_groups_name = "->".join(
            permission_group_label(role)
            for role in normalize_permission_group_sequence(audit_auth_groups)
        )
        result["data"]["auditors"] = audit_auth_groups
        result["data"]["auditors_display"] = audit_auth_groups_name

    return HttpResponse(json.dumps(result), content_type="application/json")


@superuser_required
def changeauditors(request):
    """Set the approval flow for the team."""
    auth_groups = request.POST.get("audit_auth_groups")
    team_name = request.POST.get("team_name")
    workflow_type = request.POST.get("workflow_type")
    result = {"status": 0, "msg": "ok", "data": []}

    # Update workflow approval settings.
    team_id = Team.objects.get(team_name=team_name).team_id
    audit_auth_groups = normalize_permission_group_sequence(auth_groups)
    try:
        Audit.change_settings(team_id, workflow_type, ",".join(audit_auth_groups))
    except Exception as msg:
        logger.exception("Failed to update team audit settings")
        result["msg"] = "Failed to update team audit settings."
        result["status"] = 1

    # Return result.
    return HttpResponse(json.dumps(result), content_type="application/json")
