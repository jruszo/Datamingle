# -*- coding: UTF-8 -*-

import re
import email
import smtplib
import requests
import logging
import traceback
from email import encoders
from email.header import Header
from email.utils import formataddr

from common.config import SysConfig
from common.utils.ding_api import get_access_token
from common.utils.wx_api import get_wx_access_token
from common.utils.feishu_api import *

logger = logging.getLogger("default")


class MsgSender(object):
    def __init__(self, **kwargs):
        if kwargs:
            self.MAIL_REVIEW_SMTP_SERVER = kwargs.get("server")
            self.MAIL_REVIEW_SMTP_PORT = kwargs.get("port", 0)
            self.MAIL_REVIEW_FROM_ADDR = kwargs.get("user")
            self.MAIL_REVIEW_FROM_PASSWORD = kwargs.get("password")
            self.MAIL_SSL = kwargs.get("ssl")
        else:
            sys_config = SysConfig()
            # Email settings
            self.MAIL_REVIEW_SMTP_SERVER = sys_config.get("mail_smtp_server")
            self.MAIL_REVIEW_SMTP_PORT = sys_config.get("mail_smtp_port", 0)
            self.MAIL_SSL = sys_config.get("mail_ssl")
            self.MAIL_REVIEW_FROM_ADDR = sys_config.get("mail_smtp_user")
            self.MAIL_REVIEW_FROM_PASSWORD = sys_config.get("mail_smtp_password")
            # DingTalk settings
            self.ding_agent_id = sys_config.get("ding_agent_id")
            # WeCom settings
            self.wx_agent_id = sys_config.get("wx_agent_id")
            # Feishu settings
            self.feishu_appid = sys_config.get("feishu_appid")
            self.feishu_app_secret = sys_config.get("feishu_app_secret")

        if self.MAIL_REVIEW_SMTP_PORT:
            self.MAIL_REVIEW_SMTP_PORT = int(self.MAIL_REVIEW_SMTP_PORT)
        elif self.MAIL_SSL:
            self.MAIL_REVIEW_SMTP_PORT = 465
        else:
            self.MAIL_REVIEW_SMTP_PORT = 25

    @staticmethod
    def _add_attachment(filename):
        """
        Add email attachment.
        :param filename:
        :return:
        """
        file_msg = email.mime.base.MIMEBase("application", "octet-stream")
        file_msg.set_payload(open(filename, "rb").read())
        # Use gbk for attachment filenames to avoid garbled Chinese names
        file_msg.add_header(
            "Content-Disposition",
            "attachment",
            filename=("gbk", "", filename.split("/")[-1]),
        )
        encoders.encode_base64(file_msg)

        return file_msg

    def send_email(self, subject, body, to, **kwargs):
        """
        Send email.
        :param subject:
        :param body:
        :param to:
        :param kwargs:
        :return: str: 'success' on success, traceback text on error
        """

        try:
            if not to:
                logger.warning("Recipient list is empty; unable to send email")
                return
            if not isinstance(to, list):
                raise TypeError("Recipients must be provided as a list")
            list_cc = kwargs.get("list_cc_addr", [])
            if not isinstance(list_cc, list):
                raise TypeError("CC recipients must be provided as a list")

            # Build root MIME container
            main_msg = email.mime.multipart.MIMEMultipart()

            # Add plain-text body
            text_msg = email.mime.text.MIMEText(body, "plain", "utf-8")
            main_msg.attach(text_msg)

            # Add attachments
            filename_list = kwargs.get("filename_list")
            if filename_list:
                for filename in kwargs["filename_list"]:
                    file_msg = self._add_attachment(filename)
                    main_msg.attach(file_msg)

            # Message headers
            main_msg["Subject"] = Header(subject, "utf-8").encode()
            main_msg["From"] = formataddr(["Archery Notification", self.MAIL_REVIEW_FROM_ADDR])
            main_msg["To"] = ",".join(list(set(to)))
            main_msg["Cc"] = ", ".join(str(cc) for cc in list(set(list_cc)))
            main_msg["Date"] = email.utils.formatdate()

            if self.MAIL_SSL:
                server = smtplib.SMTP_SSL(
                    self.MAIL_REVIEW_SMTP_SERVER, self.MAIL_REVIEW_SMTP_PORT, timeout=3
                )
            else:
                server = smtplib.SMTP(
                    self.MAIL_REVIEW_SMTP_SERVER, self.MAIL_REVIEW_SMTP_PORT, timeout=3
                )

                # Skip SMTP login when password is empty
            if self.MAIL_REVIEW_FROM_PASSWORD:
                server.login(self.MAIL_REVIEW_FROM_ADDR, self.MAIL_REVIEW_FROM_PASSWORD)
            server.sendmail(
                self.MAIL_REVIEW_FROM_ADDR, to + list_cc, main_msg.as_string()
            )
            server.quit()
            logger.debug(
                f"Email sent successfully\nSubject:{subject}\nRecipients:{to + list_cc}\nContent:{body}"
            )
            return "success"
        except Exception:
            errmsg = "Email push failed\n{}".format(traceback.format_exc())
            logger.error(errmsg)
            return errmsg

    @staticmethod
    def send_ding(url, content):
        """
        Send DingTalk webhook message.
        :param url:
        :param content:
        :return:
        """
        data = {
            "msgtype": "text",
            "text": {"content": "{}".format(content)},
        }
        r = requests.post(url=url, json=data)
        r_json = r.json()
        if r_json["errcode"] == 0:
            logger.debug(f"DingTalk webhook sent successfully\nTarget:{url}\nContent:{content}")
        else:
            logger.error(
                f"DingTalk webhook failed\nRequest url:{url}\nRequest data:{data}\nResponse:{r_json}"
            )

    def send_ding2user(self, userid_list, content):
        """
        Send DingTalk message to specific users.
        :param userid_list:
        :param content:
        :return:
        """
        access_token = get_access_token()
        send_url = f"https://oapi.dingtalk.com/topapi/message/corpconversation/asyncsend_v2?access_token={access_token}"
        data = {
            "userid_list": ",".join(list(set(userid_list))),
            "agent_id": self.ding_agent_id,
            "msg": {"msgtype": "text", "text": {"content": f"{content}"}},
        }
        r = requests.post(url=send_url, json=data, timeout=5)
        r_json = r.json()
        if r_json["errcode"] == 0:
            logger.debug(
                f"DingTalk message sent successfully\nTargets:{userid_list}\nContent:{content}"
            )
        else:
            logger.error(
                f"DingTalk message failed\nRequest url:{send_url}\nRequest data:{data}\nResponse:{r_json}"
            )

    def send_wx2user(self, msg, user_list):
        if not user_list:
            logger.error("WeCom push failed: unable to determine target users.")
            return
        to_user = "|".join(list(set(user_list)))
        access_token = get_wx_access_token()
        send_url = f"https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={access_token}"
        data = {
            "touser": to_user,
            "msgtype": "text",
            "agentid": self.wx_agent_id,
            "text": {"content": msg},
        }
        res = requests.post(url=send_url, json=data, timeout=5)
        r_json = res.json()
        if r_json["errcode"] == 0:
            logger.debug(f"WeCom push sent successfully\nTarget:{to_user}")
        else:
            logger.error(
                f"WeCom push failed\nRequest url:{send_url}\nRequest data:{data}\nResponse:{r_json}"
            )

    def send_qywx_webhook(self, qywx_webhook, msg):
        send_url = qywx_webhook

        # Convert links
        # Convert plain links to markdown links
        _msg = re.findall("https://.+(?=\n)|http://.+(?=\n)", msg)
        for url in _msg:
            # Prevent re-replacement of existing markdown links
            if url.strip()[-1] != ")":
                msg = msg.replace(url, "[Please click the link](%s)" % url)

        data = {
            "msgtype": "markdown",
            "markdown": {"content": msg},
        }
        res = requests.post(url=send_url, json=data, timeout=5)
        r_json = res.json()
        if r_json["errcode"] == 0:
            logger.debug("WeCom bot push sent successfully\nTarget:bot")
        else:
            logger.error(
                f"WeCom bot push failed\nRequest url:{send_url}\nRequest data:{data}\nResponse:{r_json}"
            )

    @staticmethod
    def send_feishu_webhook(url, title, content):
        data = {"title": title, "text": content}
        if "/v2/" in url:
            data = {
                "msg_type": "post",
                "content": {
                    "post": {
                        "zh_cn": {
                            "title": title,
                            "content": [[{"tag": "text", "text": content}]],
                        }
                    }
                },
            }

        r = requests.post(url=url, json=data)
        r_json = r.json()
        if (
            "ok" in r_json
            or ("StatusCode" in r_json and r_json["StatusCode"] == 0)
            or ("code" in r_json and r_json["code"] == 0)
        ):
            logger.debug(f"Feishu webhook sent successfully\nTarget:{url}\nContent:{content}")
        else:
            logger.error(
                f"Feishu webhook failed\nRequest url:{url}\nRequest data:{data}\nResponse:{r_json}"
            )

    @staticmethod
    def send_feishu_user(title, content, open_id, user_mail):
        if user_mail:
            open_id = open_id + get_feishu_open_id(user_mail)
        if not open_id:
            return
        url = "https://open.feishu.cn/open-apis/message/v4/batch_send/"
        data = {
            "open_ids": open_id,
            "msg_type": "text",
            "content": {"text": f"{title}\n{content}"},
        }
        r = requests.post(
            url=url,
            json=data,
            headers={"Authorization": "Bearer " + get_feishu_access_token()},
        ).json()
        if r["code"] == 0:
            logger.debug(
                f"Feishu direct message sent successfully\nTarget:{url}\nContent:{content}"
            )
        else:
            logger.error(
                f"Feishu direct message failed\nRequest url:{url}\nRequest data:{data}\nResponse:{r}"
            )
