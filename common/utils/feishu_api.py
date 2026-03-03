# coding: utf-8

import logging
import requests
from common.config import SysConfig
from django.core.cache import cache

logger = logging.getLogger("default")


def get_feishu_access_token():
    # Read from cache first
    try:
        token = cache.get("feishu_access_token")
    except Exception as e:
        logger.error(f"Failed to read Feishu token from cache: {e}")
        token = None
    if token:
        return token
    # Request token from Feishu API
    sys_config = SysConfig()
    app_id = sys_config.get("feishu_appid")
    app_secret = sys_config.get("feishu_app_secret")
    url = f"https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal/"
    data = {"app_id": app_id, "app_secret": app_secret}
    resp = requests.post(url, json=data, timeout=5).json()
    # resp = requests.get(url, timeout=3).json()
    logger.info(f"Fetched Feishu access_token response: {resp}")
    if resp.get("code") == 0:
        access_token = resp.get("app_access_token")
        expires_in = resp.get("expire")
        cache.set("feishu_access_token", access_token, timeout=expires_in - 60)
        return access_token
    else:
        logger.error(f"Failed to fetch Feishu access_token: {resp}")
        return None


def get_feishu_open_id(email):
    url = "https://open.feishu.cn/open-apis/user/v1/batch_get_id?"
    resp = requests.get(
        url,
        timeout=3,
        headers={"Authorization": "Bearer " + get_feishu_access_token()},
        params={"emails": email},
    ).json()
    result = []
    if resp.get("code") == 0:
        for key in resp.get("data").get("email_users").values():
            result.append(key[0][f"open_id"])
    return result
