#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import requests
from django.http import JsonResponse
from django_redis import get_redis_connection
from common.config import SysConfig
from common.utils.permission import superuser_required
from sql.models import Users
from sql.utils.tasks import add_sync_ding_user_schedule

logger = logging.getLogger("default")
rs = get_redis_connection("default")


def get_access_token():
    """Get DingTalk access token: https://ding-doc.dingtalk.com/doc#/serverapi2/eev437"""
    # Read from cache first
    try:
        access_token = rs.execute_command(f"get ding_access_token")
    except Exception as e:
        logger.error(f"Failed to read DingTalk access_token from cache: {e}")
        access_token = None
    if access_token:
        return access_token.decode()
    # Request from DingTalk API
    sys_config = SysConfig()
    app_key = sys_config.get("ding_app_key")
    app_secret = sys_config.get("ding_app_secret")
    url = f"https://oapi.dingtalk.com/gettoken?appkey={app_key}&appsecret={app_secret}"
    resp = requests.get(url, timeout=3).json()
    if resp.get("errcode") == 0:
        access_token = resp.get("access_token")
        expires_in = resp.get("expires_in")
        rs.execute_command(f"SETEX ding_access_token {expires_in-60} {access_token}")
        return access_token
    else:
        logger.error(f"Failed to fetch DingTalk access_token: {resp}")
        return None


def get_ding_user_id(username):
    """Update user's ding_user_id."""
    try:
        ding_user_id = rs.execute_command("GET {}".format(username.lower()))
        if ding_user_id:
            user = Users.objects.get(username=username)
            if user.ding_user_id != str(ding_user_id, encoding="utf8"):
                user.ding_user_id = str(ding_user_id, encoding="utf8")
                user.save(update_fields=["ding_user_id"])
    except Exception as e:
        logger.error(f"Failed to update user ding_user_id: {e}")


def get_dept_list_id_fetch_child(token, parent_dept_id):
    """Get all child department IDs recursively."""
    ids = [int(parent_dept_id)]
    url = (
        "https://oapi.dingtalk.com/department/list_ids?id={0}&access_token={1}".format(
            parent_dept_id, token
        )
    )
    resp = requests.get(url, timeout=3).json()
    if resp.get("errcode") == 0:
        for dept_id in resp.get("sub_dept_id_list"):
            ids.extend(get_dept_list_id_fetch_child(token, dept_id))
    return list(set(ids))


def sync_ding_user_id():
    """
    Archery users log in with employee ID (`username`), which maps to DingTalk's
    `jobnumber` field. Use `jobnumber` to find and cache user `ding_user_id`.
    """
    sys_config = SysConfig()
    ding_dept_ids = sys_config.get("ding_dept_ids", "")
    username2ding = sys_config.get("ding_archery_username")
    token = get_access_token()
    if not token:
        return False
    # Fetch all department IDs
    sub_dept_id_list = []
    for dept_id in list(set(ding_dept_ids.split(","))):
        sub_dept_id_list.extend(get_dept_list_id_fetch_child(token, dept_id))
    # Iterate users in each department
    user_ids = []
    for sdi in sub_dept_id_list:
        url = f"https://oapi.dingtalk.com/user/getDeptMember?access_token={token}&deptId={sdi}"
        try:
            resp = requests.get(url, timeout=3).json()
            if resp.get("errcode") == 0:
                user_ids.extend(resp.get("userIds"))
            else:
                raise Exception(f"Failed to fetch department users: {resp}")
        except Exception as e:
            raise Exception(f"Failed to fetch department users: {e}")
    # Fetch user details and cache mappings
    for user_id in list(set(user_ids)):
        url = (
            f"https://oapi.dingtalk.com/user/get?access_token={token}&userid={user_id}"
        )
        try:
            resp = requests.get(url, timeout=3).json()
            if resp.get("errcode") == 0:
                if not resp.get(username2ding):
                    raise Exception(
                        f"DingTalk user payload does not include `{username2ding}`. "
                        f"Please check `ding_archery_username` config: {resp}"
                    )
                rs.execute_command(
                    f"SETEX {resp.get(username2ding).lower()} 86400 {resp.get('userid')}"
                )
            else:
                raise Exception(f"Failed to fetch user info: {resp}")
        except Exception as e:
            raise Exception(f"Failed to fetch user info: {e}")
    return True


@superuser_required
def sync_ding_user(request):
    """Trigger manual sync and also register daily schedule sync."""
    try:
        # Add schedule and trigger sync
        add_sync_ding_user_schedule()
        return JsonResponse({"status": 0, "msg": "Sync triggered successfully"})
    except Exception as e:
        return JsonResponse({"status": 1, "msg": f"Sync trigger failed: {e}"})
