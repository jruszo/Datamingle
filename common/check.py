# -*- coding: UTF-8 -*-
import logging
import traceback

import MySQLdb
import simplejson as json
from common.utils.sendmsg import MsgSender
from sql.storage import DynamicStorage

logger = logging.getLogger("default")
VALID_STORAGE_TYPES = {"sftp", "s3c", "azure"}


def _parse_bool(value):
    if isinstance(value, bool):
        return value
    return str(value).lower() == "true"


def validate_go_inception_payload(payload):
    result = {"status": 0, "msg": "ok", "data": []}
    go_inception_host = payload.get("go_inception_host", "")
    go_inception_port = payload.get("go_inception_port", "")
    go_inception_user = payload.get("go_inception_user", "")
    go_inception_password = payload.get("go_inception_password", "")
    inception_remote_backup_host = payload.get("inception_remote_backup_host", "")
    inception_remote_backup_port = payload.get("inception_remote_backup_port", "")
    inception_remote_backup_user = payload.get("inception_remote_backup_user", "")
    inception_remote_backup_password = payload.get(
        "inception_remote_backup_password", ""
    )

    try:
        conn = MySQLdb.connect(
            host=go_inception_host,
            port=int(go_inception_port),
            user=go_inception_user,
            password=go_inception_password,
            charset="utf8mb4",
            connect_timeout=5,
        )
        cur = conn.cursor()
    except Exception:
        logger.error(traceback.format_exc())
        result["status"] = 1
        result["msg"] = "Unable to connect to goInception"
        return result
    else:
        cur.close()
        conn.close()

    try:
        conn = MySQLdb.connect(
            host=inception_remote_backup_host,
            port=int(inception_remote_backup_port),
            user=inception_remote_backup_user,
            password=inception_remote_backup_password,
            charset="utf8mb4",
            connect_timeout=5,
        )
        cur = conn.cursor()
    except Exception:
        logger.error(traceback.format_exc())
        result["status"] = 1
        result["msg"] = "Unable to connect to goInception backup database"
    else:
        cur.close()
        conn.close()

    return result


def validate_email_payload(payload, user_email):
    result = {"status": 0, "msg": "ok", "data": []}
    mail = _parse_bool(payload.get("mail", ""))
    mail_ssl = _parse_bool(payload.get("mail_ssl", ""))
    mail_smtp_server = payload.get("mail_smtp_server", "")
    mail_smtp_port = payload.get("mail_smtp_port", "")
    mail_smtp_user = payload.get("mail_smtp_user", "")
    mail_smtp_password = payload.get("mail_smtp_password", "")
    if not mail:
        result["status"] = 1
        result["msg"] = "Please enable email notifications first."
        return result
    try:
        mail_smtp_port = int(mail_smtp_port)
        if mail_smtp_port < 0:
            raise ValueError
    except ValueError:
        result["status"] = 1
        result["msg"] = "Port must be a positive integer."
        return result
    if not user_email:
        result["status"] = 1
        result["msg"] = "Please complete the current user's email first."
        return result
    bd = "Archery email delivery test..."
    subj = "Archery email delivery test"
    sender = MsgSender(
        server=mail_smtp_server,
        port=mail_smtp_port,
        user=mail_smtp_user,
        password=mail_smtp_password,
        ssl=mail_ssl,
    )
    sender_response = sender.send_email(subj, bd, [user_email])
    if sender_response != "success":
        result["status"] = 1
        result["msg"] = sender_response
        logger.error("Email delivery test failed: %s", sender_response)
        return result
    return result


def validate_file_storage_payload(payload):
    result = {"status": 0, "msg": "ok", "data": []}
    storage_type = payload.get("storage_type")
    if storage_type not in VALID_STORAGE_TYPES:
        result["status"] = 1
        result["msg"] = "Invalid storage type."
        return result

    max_export_rows = payload.get("max_export_rows", "10000")
    max_export_rows = max_export_rows if max_export_rows else "10000"
    try:
        if not str(max_export_rows).isdigit():
            raise TypeError("max_export_rows must be an integer")
        max_export_rows = int(max_export_rows)
    except TypeError as e:
        result["status"] = 1
        result["msg"] = f"Invalid parameter type: {str(e)}"
        return result

    custom_param_key = f"{storage_type}_custom_params"
    custom_params_str = str(payload.get(custom_param_key, "")).strip()
    if custom_params_str:
        try:
            json.loads(custom_params_str)
        except json.JSONDecodeError:
            result["status"] = 1
            result["msg"] = "Invalid custom parameters. Please provide valid JSON."
            return result

    config_dict = {
        "storage_type": storage_type,
        "max_export_rows": max_export_rows,
        "sftp_host": payload.get("sftp_host", ""),
        "sftp_port": payload.get("sftp_port", 22),
        "sftp_user": payload.get("sftp_user", ""),
        "sftp_password": payload.get("sftp_password", ""),
        "sftp_path": payload.get("sftp_path", ""),
        "sftp_custom_params": payload.get("sftp_custom_params", ""),
        "s3c_access_key_id": payload.get("s3c_access_key_id", ""),
        "s3c_access_key_secret": payload.get("s3c_access_key_secret", ""),
        "s3c_endpoint": payload.get("s3c_endpoint", ""),
        "s3c_bucket_name": payload.get("s3c_bucket_name", ""),
        "s3c_region": payload.get("s3c_region", ""),
        "s3c_path": payload.get("s3c_path", ""),
        "s3c_custom_params": payload.get("s3c_custom_params", ""),
        "azure_account_name": payload.get("azure_account_name", ""),
        "azure_account_key": payload.get("azure_account_key", ""),
        "azure_container": payload.get("azure_container", ""),
        "azure_path": payload.get("azure_path", ""),
        "azure_custom_params": payload.get("azure_custom_params", ""),
    }

    try:
        storage = DynamicStorage(config_dict=config_dict)
        success, message = storage.check_connection()

        if not success:
            result["status"] = 1
            result["msg"] = "Storage connectivity test failed."
            logging.error("Storage connectivity test failed")
            logging.debug("Storage connectivity test returned an error.")
        else:
            logging.info("Storage connectivity test succeeded")
    except Exception:
        result["status"] = 1
        result["msg"] = "Storage connectivity test raised an exception."
        logger.error("Storage connectivity test failed", exc_info=True)

    return result
