# -*- coding: UTF-8 -*-
"""
@author: hhyo
@license: Apache Licence
@file: plugin.py
@time: 2019/03/04
"""

__author__ = "hhyo"

import logging
import subprocess
import traceback

logger = logging.getLogger("default")


class Plugin:
    def __init__(self, path):
        self.path = path
        self.required_args = []  # Required arguments.
        self.disable_args = []  # Disabled arguments.

    def check_args(self, args):
        """
        Validate request arguments.
        :return: {'status': 0, 'msg': 'ok', 'data': {}}
        """
        args_check_result = {"status": 0, "msg": "ok", "data": {}}
        # Check executable path.
        if self.path is None:
            return {
                "status": 1,
                "msg": "Executable path cannot be empty!",
                "data": {},
            }
        # Check disabled arguments.
        for arg in args.keys():
            if arg in self.disable_args:
                return {
                    "status": 1,
                    "msg": "Argument {arg} is disabled".format(arg=arg),
                    "data": {},
                }
        # Check required arguments.
        for req_arg in self.required_args:
            if req_arg not in args.keys():
                return {
                    "status": 1,
                    "msg": "Required argument {arg} must be specified".format(
                        arg=req_arg
                    ),
                    "data": {},
                }
            elif args[req_arg] is None or args[req_arg] == "":
                return {
                    "status": 1,
                    "msg": "Value for argument {arg} cannot be empty".format(
                        arg=req_arg
                    ),
                    "data": {},
                }
        return args_check_result

    def generate_args2cmd(self, args):
        """
        Convert request arguments to command-line arguments.
        :return:
        """
        cmd_args = [self.path]
        for arg, value in args.items():
            if not value:
                continue
            cmd_args.append(f"-{arg}")
            if not isinstance(value, bool):
                cmd_args.append(f"{value}")
        return cmd_args

    @staticmethod
    def execute_cmd(cmd_args):
        """
        Execute command and return process.
        :return:
        """
        try:
            p = subprocess.Popen(
                cmd_args,
                shell=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
            )
            return p
        except Exception as e:
            logger.error("Command execution failed\n{}".format(traceback.format_exc()))
            raise RuntimeError("Command execution failed, reason: %s" % str(e))
