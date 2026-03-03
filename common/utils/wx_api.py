# coding: utf-8

import logging
import requests
from common.config import SysConfig
from django.core.cache import cache

logger = logging.getLogger("default")


def get_wx_access_token():
    # Read from cache first
    try:
        token = cache.get("wx_access_token")
    except Exception as e:
        logger.error(f"Failed to read WeCom token from cache: {e}")
        token = None
    if token:
        return token
    # Request token from WeCom API
    sys_config = SysConfig()
    corp_id = sys_config.get("wx_corpid")
    corp_secret = sys_config.get("wx_app_secret")
    url = f"https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid={corp_id}&corpsecret={corp_secret}"
    resp = requests.get(url, timeout=3).json()
    if resp.get("errcode") == 0:
        access_token = resp.get("access_token")
        expires_in = resp.get("expires_in")
        cache.set("wx_access_token", access_token, timeout=expires_in - 60)
        return access_token
    else:
        logger.error(f"Failed to fetch WeCom access_token: {resp}")
        return None
