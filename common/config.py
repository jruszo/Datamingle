# -*- coding: UTF-8 -*-
import logging
import traceback

import simplejson as json
from django.http import HttpResponse

from common.utils.permission import superuser_required
from sql.models import Config
from django.db import transaction

logger = logging.getLogger("default")


class SysConfig(object):
    def __init__(self):
        self.sys_config = {}

    def get_all_config(self):
        try:
            # Load system configuration from DB
            all_config = Config.objects.all().values("item", "value")
            sys_config = {}
            for items in all_config:
                if items["value"] in ("true", "True"):
                    items["value"] = True
                elif items["value"] in ("false", "False"):
                    items["value"] = False
                sys_config[items["item"]] = items["value"]
            self.sys_config = sys_config
        except Exception as m:
            logger.error(f"Failed to load system configuration: {m}{traceback.format_exc()}")
            self.sys_config = {}

    def get(self, key, default_value=None):
        value = self.sys_config.get(key)
        if value:
            return value
        # Fallback to DB lookup
        config_entry = Config.objects.filter(item=key).last()
        if config_entry:
            # Normalize string bool into Python bool
            value = self.filter_bool(config_entry.value)
        # If it's an empty/blank string, return default
        if isinstance(value, str) and value.strip() == "":
            return default_value
        if value is not None:
            self.sys_config[key] = value
            return value
        return default_value

    @staticmethod
    def filter_bool(value: str):
        if value.lower() == "true":
            return True
        if value.lower() == "false":
            return False
        return value

    def set(self, key, value):
        if value is True:
            db_value = "true"
        elif value is False:
            db_value = "false"
        else:
            db_value = value
        obj, created = Config.objects.update_or_create(
            item=key, defaults={"value": db_value}
        )
        self.sys_config.update({key: value})

    def replace(self, configs):
        result = {"status": 0, "msg": "ok", "data": []}
        # Replace all existing configs
        try:
            with transaction.atomic():
                self.purge()
                Config.objects.bulk_create(
                    [
                        Config(
                            item=items["key"].strip(), value=str(items["value"]).strip()
                        )
                        for items in json.loads(configs)
                    ]
                )
        except Exception as e:
            logger.error(traceback.format_exc())
            result["status"] = 1
            result["msg"] = str(e)
        finally:
            self.get_all_config()
        return result

    def purge(self):
        """Clear all configs. Used by tests and `replace`."""
        try:
            with transaction.atomic():
                Config.objects.all().delete()
                self.sys_config = {}
        except Exception as m:
            logger.error(f"Failed to clear config cache: {m}{traceback.format_exc()}")


# Update system configuration
@superuser_required
def change_config(request):
    configs = request.POST.get("configs")
    archer_config = SysConfig()
    result = archer_config.replace(configs)
    # Return result
    return HttpResponse(json.dumps(result), content_type="application/json")
