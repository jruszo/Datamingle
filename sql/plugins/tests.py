# -*- coding: UTF-8 -*-
"""
@author: hhyo
@license: Apache Licence
@file: tests.py
@time: 2019/03/04
"""

import json
from django.test import Client, TestCase
from unittest.mock import patch, ANY, Mock
from pytest_mock import MockerFixture
from django.contrib.auth import get_user_model

from sql.plugins.plugin import Plugin
from sql.plugins.pt_archiver import PtArchiver
from sql.plugins.password import VaultMixin

from common.config import SysConfig

User = get_user_model()

__author__ = "hhyo"


class TestPlugin(TestCase):
    """
    Test Plugin calls.
    """

    @classmethod
    def setUpClass(cls):
        cls.superuser = User(username="super", is_superuser=True)
        cls.superuser.save()
        cls.sys_config = SysConfig()
        cls.client = Client()
        cls.client.force_login(cls.superuser)

    @classmethod
    def tearDownClass(cls):
        cls.superuser.delete()
        cls.sys_config.replace(json.dumps({}))

    def test_check_args_path(self):
        """
        Test executable path validation.
        :return:
        """
        plugin = Plugin(path=None)
        args_check_result = plugin.check_args({"query": "select 1;"})
        self.assertDictEqual(
            args_check_result,
            {"status": 1, "msg": "Executable path cannot be empty!", "data": {}},
        )
        # Path is not empty.
        plugin = Plugin(path="/usr/bin/example")
        args_check_result = plugin.check_args({"query": "select 1;"})
        self.assertDictEqual(args_check_result, {"status": 0, "msg": "ok", "data": {}})

    def test_check_args_disable(self):
        """
        Test disabled argument validation.
        :return:
        """
        plugin = Plugin(path="/usr/bin/example")
        plugin.disable_args = ["allow-online-as-test"]
        args_check_result = plugin.check_args(
            {"allow-online-as-test": "false", "query": "select 1;"}
        )
        self.assertDictEqual(
            args_check_result,
            {
                "status": 1,
                "msg": "Argument allow-online-as-test is disabled",
                "data": {},
            },
        )

    def test_check_args_required(self):
        """
        Test required argument validation.
        :return:
        """
        args = {"report-type": "markdown"}
        plugin = Plugin(path="/usr/bin/example")
        plugin.required_args = ["query"]
        args_check_result = plugin.check_args(args)
        self.assertDictEqual(
            args_check_result,
            {
                "status": 1,
                "msg": "Required argument query must be specified",
                "data": {},
            },
        )
        args["query"] = ""
        args_check_result = plugin.check_args(args)
        self.assertDictEqual(
            args_check_result,
            {
                "status": 1,
                "msg": "Value for argument query cannot be empty",
                "data": {},
            },
        )

    def test_plugin_generate_args2cmd(self):
        """
        Test argument conversion.
        :return:
        """
        plugin = Plugin(path="/usr/bin/example")
        cmd_args = plugin.generate_args2cmd({"query": "select 1;"})
        self.assertIsInstance(cmd_args, list)

    def test_pt_archiver_generate_args2cmd(self):
        """
        Test pt_archiver argument conversion.
        :return:
        """
        args = {
            "no-version-check": True,
            "source": "",
            "where": "",
            "progress": 5000,
            "statistics": True,
            "charset": "UTF8",
            "limit": 10000,
            "txn-size": 1000,
            "sleep": 1,
        }
        pt_archiver = PtArchiver()
        cmd_args = pt_archiver.generate_args2cmd(args)
        self.assertIsInstance(cmd_args, list)

    @patch("sql.plugins.plugin.subprocess")
    def test_execute_cmd(self, mock_subprocess):
        plugin = Plugin(path="/usr/bin/example")
        cmd_args = plugin.generate_args2cmd({"query": "select 1;"})

        mock_subprocess.Popen.return_value.communicate.return_value = (
            "some_stdout",
            "some_stderr",
        )
        stdout, _stderr = plugin.execute_cmd(cmd_args).communicate()
        mock_subprocess.Popen.assert_called_once_with(
            cmd_args, shell=False, stdout=ANY, stderr=ANY, universal_newlines=ANY
        )
        self.assertIn("some_stdout", stdout)
        # Exception.

        mock_subprocess.Popen.side_effect = Exception("Boom! some exception!")
        with self.assertRaises(RuntimeError):
            plugin.execute_cmd(cmd_args)


def test_password_mixin(mocker: MockerFixture):
    from sql.plugins.password import requests

    class MockReponse(Mock):
        def json(self):
            return {"data": {"username": "test", "password": "test", "ttl": 360}}

    mocker.patch.object(requests, "get", return_value=MockReponse())

    class DummyInstance:
        instance_name = "dummy"

    class CompondInstance(DummyInstance, VaultMixin):
        pass

    instance = CompondInstance()
    username, password = instance.get_username_password()
    assert username == "test"
    assert password == "test"
    assert requests.get.call_count == 1
