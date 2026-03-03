# -*- coding: UTF-8 -*-
"""
@author: hhyo
@license: Apache Licence
@file: tests.py
@time: 2019/03/14
"""

import datetime
import json
from unittest.mock import patch, MagicMock

from django.conf import settings
from django.contrib.auth.models import Permission, Group
from django.test import TestCase, Client
from django_q.models import Schedule

from common.config import SysConfig
from sql.engines.models import ReviewResult, ReviewSet
from sql.models import (
    Users,
    SqlWorkflow,
    SqlWorkflowContent,
    Instance,
    ResourceGroup,
    WorkflowLog,
    DataMaskingRules,
    DataMaskingColumns,
    InstanceTag,
)
from sql.utils.resource_group import user_groups, user_instances, auth_group_users
from sql.utils.sql_review import (
    can_execute,
    can_timingtask,
    can_cancel,
    on_correct_time_period,
)
from sql.utils.sql_utils import *
from sql.utils.execute_sql import execute, execute_callback
from sql.utils.tasks import add_sql_schedule, del_schedule, task_info
from sql.utils.data_masking import data_masking, brute_mask, simple_column_mask

User = Users
__author__ = "hhyo"


class TestSQLReview(TestCase):
    """
    Test methods in sql review.
    """

    def setUp(self):
        self.superuser = User.objects.create(username="super", is_superuser=True)
        self.user = User.objects.create(username="user")
        # Keep instance aligned with the test service when using travis.ci.
        self.master = Instance(
            instance_name="test_instance",
            type="master",
            db_type="mysql",
            host=settings.DATABASES["default"]["HOST"],
            port=settings.DATABASES["default"]["PORT"],
            user=settings.DATABASES["default"]["USER"],
            password=settings.DATABASES["default"]["PASSWORD"],
        )
        self.master.save()
        self.sys_config = SysConfig()
        self.client = Client()
        self.group = ResourceGroup.objects.create(group_id=1, group_name="group_name")
        self.wf1 = SqlWorkflow.objects.create(
            workflow_name="workflow_name",
            group_id=self.group.group_id,
            group_name=self.group.group_name,
            engineer=self.superuser.username,
            engineer_display=self.superuser.display,
            audit_auth_groups="audit_auth_groups",
            create_time=datetime.datetime.now(),
            status="workflow_review_pass",
            is_backup=True,
            instance=self.master,
            db_name="db_name",
            syntax_type=1,
        )
        self.wfc1 = SqlWorkflowContent.objects.create(
            workflow=self.wf1, sql_content="some_sql", execute_result=""
        )

    def tearDown(self):
        self.wf1.delete()
        self.group.delete()
        self.superuser.delete()
        self.user.delete()
        self.master.delete()
        self.sys_config.replace(json.dumps({}))

    def test_can_execute_for_resource_group(
        self,
    ):
        """
        Test can_execute condition: user has resource-group-level execute
        permission and belongs to the group.
        :return:
        """
        # Set workflow to review_pass, user has resource-group execute permission
        # and belongs to the group.
        self.wf1.status = "workflow_review_pass"
        self.wf1.save(update_fields=("status",))
        sql_execute_for_resource_group = Permission.objects.get(
            codename="sql_execute_for_resource_group"
        )
        self.user.user_permissions.add(sql_execute_for_resource_group)
        self.user.resource_group.add(self.group)
        r = can_execute(user=self.user, workflow_id=self.wfc1.workflow_id)
        self.assertTrue(r)

    def test_can_execute_true(
        self,
    ):
        """
        Test can_execute condition: user is submitter, has execute permission,
        and workflow status is review passed.
        :return:
        """
        # Set workflow to review_pass, current user is submitter and has execute
        # permission.
        self.wf1.status = "workflow_review_pass"
        self.wf1.engineer = self.user.username
        self.wf1.save(update_fields=("status", "engineer"))
        sql_execute = Permission.objects.get(codename="sql_execute")
        self.user.user_permissions.add(sql_execute)
        r = can_execute(user=self.user, workflow_id=self.wfc1.workflow_id)
        self.assertTrue(r)

    def test_can_execute_workflow_timing_task(
        self,
    ):
        """
        Test can_execute condition: user is submitter, has execute permission,
        and workflow status is timing task.
        :return:
        """
        # Set workflow to timing task, current user is submitter and has execute
        # permission.
        self.wf1.status = "workflow_timingtask"
        self.wf1.engineer = self.user.username
        self.wf1.save(update_fields=("status", "engineer"))
        sql_execute = Permission.objects.get(codename="sql_execute")
        self.user.user_permissions.add(sql_execute)
        r = can_execute(user=self.user, workflow_id=self.wfc1.workflow_id)
        self.assertTrue(r)

    def test_can_execute_false_no_permission(
        self,
    ):
        """
        Current user is submitter but has no execute permission.
        :return:
        """
        # Set workflow to timing task and current user as submitter.
        self.wf1.status = "workflow_timingtask"
        self.wf1.engineer = self.user.username
        self.wf1.save(update_fields=("status", "engineer"))
        r = can_execute(user=self.user, workflow_id=self.wfc1.workflow_id)
        self.assertFalse(r)

    def test_can_execute_false_not_in_group(
        self,
    ):
        """
        User has resource-group-level execute permission but is not in the group.
        :return:
        """
        # Set workflow to review_pass, user has resource-group execute permission
        # but is not in the group.
        self.wf1.status = "workflow_review_pass"
        self.wf1.save(update_fields=("status",))
        sql_execute_for_resource_group = Permission.objects.get(
            codename="sql_execute_for_resource_group"
        )
        self.user.user_permissions.add(sql_execute_for_resource_group)
        r = can_execute(user=self.user, workflow_id=self.wfc1.workflow_id)
        self.assertFalse(r)

    def test_can_execute_false_wrong_status(
        self,
    ):
        """
        Current user is submitter and has execute permission, but workflow
        status is waiting for review.
        :return:
        """
        # Set workflow to manreviewing, current user is submitter and has execute
        # permission, but status is waiting for review.
        self.wf1.status = "workflow_manreviewing"
        self.wf1.engineer = self.user.username
        self.wf1.save(update_fields=("status", "engineer"))
        sql_execute = Permission.objects.get(codename="sql_execute")
        self.user.user_permissions.add(sql_execute)
        r = can_execute(user=self.user, workflow_id=self.wfc1.workflow_id)
        self.assertFalse(r)

    def test_can_timingtask_true(
        self,
    ):
        """
        Test can_timingtask condition: user is submitter, has execute permission,
        and workflow status is review passed.
        :return:
        """
        # Set workflow to review_pass, current user is submitter and has execute
        # permission.
        self.wf1.status = "workflow_review_pass"
        self.wf1.engineer = self.user.username
        self.wf1.save(update_fields=("status", "engineer"))
        sql_execute = Permission.objects.get(codename="sql_execute")
        self.user.user_permissions.add(sql_execute)
        r = can_timingtask(user=self.user, workflow_id=self.wfc1.workflow_id)
        self.assertTrue(r)

    def test_can_timingtask_false(
        self,
    ):
        """
        Test can_timingtask condition: user has execute permission and workflow is
        review passed, but user is not submitter.
        :return:
        """
        # Set workflow to review_pass, submitter is a different user.
        self.wf1.status = "workflow_review_pass"
        self.wf1.engineer = self.superuser.username
        self.wf1.save(update_fields=("status", "engineer"))
        sql_execute = Permission.objects.get(codename="sql_execute")
        self.user.user_permissions.add(sql_execute)
        r = can_timingtask(user=self.user, workflow_id=self.wfc1.workflow_id)
        self.assertFalse(r)

    @patch("sql.utils.workflow_audit.Audit.can_review")
    def test_can_cancel_true_for_apply_user(self, _can_review):
        """
        Test can_cancel: submitter can cancel while workflow is under review.
        :return:
        """
        # Set workflow to manreviewing, current user is submitter.
        self.wf1.status = "workflow_manreviewing"
        self.wf1.engineer = self.user.username
        self.wf1.save(update_fields=("status", "engineer"))
        _can_review.return_value = False
        r = can_cancel(user=self.user, workflow_id=self.wfc1.workflow_id)
        self.assertTrue(r)

    @patch("sql.utils.workflow_audit.Audit.can_review")
    def test_can_cancel_true_for_audit_user(self, _can_review):
        """
        Test can_cancel: reviewer can cancel while workflow is under review.
        :return:
        """
        # Set workflow to manreviewing, submitter is a different user.
        self.wf1.status = "workflow_manreviewing"
        self.wf1.engineer = self.superuser.username
        self.wf1.save(update_fields=("status", "engineer"))
        _can_review.return_value = True
        r = can_cancel(user=self.user, workflow_id=self.wfc1.workflow_id)
        self.assertTrue(r)

    @patch("sql.utils.sql_review.can_execute")
    def test_can_cancel_true_for_execute_user(self, _can_execute):
        """
        Test can_cancel: for review-passed but unexecuted workflow, user with
        execute permission can cancel.
        :return:
        """
        # Set workflow to review_pass, current user is submitter.
        self.wf1.status = "workflow_review_pass"
        self.wf1.engineer = self.user.username
        self.wf1.save(update_fields=("status", "engineer"))
        _can_execute.return_value = True
        r = can_cancel(user=self.user, workflow_id=self.wfc1.workflow_id)
        self.assertTrue(r)

    @patch("sql.utils.sql_review.can_execute")
    def test_can_cancel_true_for_submit_user(self, _can_execute):
        """
        Test can_cancel: for review-passed but unexecuted workflow, submitter can
        cancel.
        :return:
        """
        # Set workflow to review_pass, current user is submitter.
        self.wf1.status = "workflow_review_pass"
        self.wf1.engineer = self.user.username
        self.wf1.save(update_fields=("status", "engineer"))
        _can_execute.return_value = True
        r = can_cancel(user=self.user, workflow_id=self.wfc1.workflow_id)
        self.assertTrue(r)

    def test_on_correct_time_period(self):
        """
        Test verification time within executable period.
        :return:
        """
        # Set executable time window.
        self.wf1.run_date_start = "2019-06-15 11:10:00"
        self.wf1.run_date_end = "2019-06-15 11:30:00"
        self.wf1.save(update_fields=("run_date_start", "run_date_end"))
        run_date = datetime.datetime.strptime(
            "2019-06-15 11:15:00", "%Y-%m-%d %H:%M:%S"
        )
        r = on_correct_time_period(self.wf1.id, run_date=run_date)
        self.assertTrue(r)

    def test_not_in_correct_time_period(self):
        """
        Test verification time outside executable period.
        :return:
        """
        # Set executable time window.
        self.wf1.run_date_start = "2019-06-15 11:10:00"
        self.wf1.run_date_end = "2019-06-15 11:30:00"
        self.wf1.save(update_fields=("run_date_start", "run_date_end"))
        run_date = datetime.datetime.strptime(
            "2019-06-15 11:45:00", "%Y-%m-%d %H:%M:%S"
        )
        r = on_correct_time_period(self.wf1.id, run_date=run_date)
        self.assertFalse(r)

    @patch("sql.utils.sql_review.datetime")
    def test_now_on_correct_time_period(self, _datetime):
        """
        Test current time within executable period.
        :return:
        """
        # Set executable time window.
        self.wf1.run_date_start = "2019-06-15 11:10:00"
        self.wf1.run_date_end = "2019-06-15 11:30:00"
        self.wf1.save(update_fields=("run_date_start", "run_date_end"))
        _datetime.datetime.now.return_value = datetime.datetime.strptime(
            "2019-06-15 11:15:00", "%Y-%m-%d %H:%M:%S"
        )
        r = on_correct_time_period(self.wf1.id)
        self.assertTrue(r)

    @patch("sql.utils.sql_review.datetime")
    def test_now_not_in_correct_time_period(self, _datetime):
        """
        Test current time outside executable period.
        :return:
        """
        # Set executable time window.
        self.wf1.run_date_start = "2019-06-15 11:10:00"
        self.wf1.run_date_end = "2019-06-15 11:30:00"
        self.wf1.save(update_fields=("run_date_start", "run_date_end"))
        _datetime.datetime.now.return_value = datetime.datetime.strptime(
            "2019-06-15 11:55:00", "%Y-%m-%d %H:%M:%S"
        )
        r = on_correct_time_period(self.wf1.id)
        self.assertFalse(r)


class TestExecuteSql(TestCase):
    def setUp(self):
        self.ins = Instance.objects.create(
            instance_name="some_ins",
            type="slave",
            db_type="mysql",
            host="some_host",
            port=3306,
            user="ins_user",
            password="some_str",
        )
        self.wf = SqlWorkflow.objects.create(
            workflow_name="some_name",
            group_id=1,
            group_name="g1",
            engineer_display="",
            audit_auth_groups="some_group",
            create_time=datetime.datetime.now(),
            status="workflow_timingtask",
            is_backup=True,
            instance=self.ins,
            db_name="some_db",
            syntax_type=1,
        )
        SqlWorkflowContent.objects.create(
            workflow=self.wf,
            sql_content="some_sql",
            execute_result=ReviewSet(
                rows=[
                    ReviewResult(
                        id=0,
                        stage="Execute failed",
                        errlevel=2,
                        stagestatus="Aborted unexpectedly",
                        errormessage="",
                        sql="Execution exception message",
                        affected_rows=0,
                        actual_affected_rows=0,
                        sequence="0_0_0",
                        backup_dbname=None,
                        execute_time=0,
                        sqlsha1="",
                    )
                ]
            ).json(),
        )

    def tearDown(self):
        self.ins.delete()
        SqlWorkflow.objects.all().delete()
        SqlWorkflowContent.objects.all().delete()
        WorkflowLog.objects.all().delete()

    @patch("sql.utils.execute_sql.Audit")
    @patch("sql.engines.mysql.MysqlEngine.execute_workflow")
    @patch("sql.engines.get_engine")
    def test_execute(self, _get_engine, _execute_workflow, _audit):
        _audit.detail_by_workflow_id.return_value.audit_id = 1
        execute(self.wf.id)
        _execute_workflow.assert_called_once()
        _audit.add_log.assert_called_with(
            audit_id=1,
            operation_type=5,
            operation_type_desc="Execute workflow",
            operation_info="System scheduled workflow execution",
            operator="",
            operator_display="System",
        )

    @patch("sql.utils.execute_sql.notify_for_execute")
    @patch("sql.utils.execute_sql.Audit")
    def test_execute_callback_success(self, _audit, _notify):
        # Initialize workflow execution result object.
        self.task_result = MagicMock()
        self.task_result.args = [self.wf.id]
        self.task_result.success = True
        self.task_result.stopped = datetime.datetime.now()
        self.task_result.result.json.return_value = json.dumps(
            [{"id": 1, "sql": "some_content"}]
        )
        self.task_result.result.warning = ""
        self.task_result.result.error = ""
        _audit.detail_by_workflow_id.return_value.audit_id = 123
        _audit.add_log.return_value = "any thing"
        # Set status to executing first.
        self.wf.status = "workflow_executing"
        self.wf.save(update_fields=["status"])
        execute_callback(self.task_result)
        _audit.detail_by_workflow_id.assert_called_with(
            workflow_id=self.wf.id, workflow_type=2
        )
        kwargs = _audit.add_log.call_args.kwargs
        self.assertEqual(kwargs["audit_id"], 123)
        self.assertEqual(kwargs["operation_type"], 6)
        self.assertEqual(kwargs["operation_type_desc"], "Execution finished")
        self.assertTrue(kwargs["operation_info"].startswith("Execution result: "))
        self.assertEqual(kwargs["operator"], "")
        self.assertEqual(kwargs["operator_display"], "System")
        _notify.assert_called_once()

    @patch("sql.utils.execute_sql.notify_for_execute")
    @patch("sql.utils.execute_sql.Audit")
    def test_execute_callback_failure(self, _audit, _notify):
        # Initialize workflow execution result object.
        self.task_result = MagicMock()
        self.task_result.args = [self.wf.id]
        self.task_result.success = False
        self.task_result.stopped = datetime.datetime.now()
        self.task_result.result = "Execution exception"
        _audit.detail_by_workflow_id.return_value.audit_id = 123
        _audit.add_log.return_value = "any thing"
        # Set status to executing.
        self.wf.status = "workflow_executing"
        self.wf.save(update_fields=["status"])
        execute_callback(self.task_result)
        _audit.detail_by_workflow_id.assert_called_with(
            workflow_id=self.wf.id, workflow_type=2
        )
        kwargs = _audit.add_log.call_args.kwargs
        self.assertEqual(kwargs["audit_id"], 123)
        self.assertEqual(kwargs["operation_type"], 6)
        self.assertEqual(kwargs["operation_type_desc"], "Execution finished")
        self.assertTrue(kwargs["operation_info"].startswith("Execution result: "))
        self.assertEqual(kwargs["operator"], "")
        self.assertEqual(kwargs["operator_display"], "System")
        _notify.assert_called_once()

    @patch("sql.utils.execute_sql.notify_for_execute")
    @patch("sql.utils.execute_sql.Audit")
    def test_execute_callback_failure_no_execute_result(self, _audit, _notify):
        # Initialize workflow execution result object.
        self.task_result = MagicMock()
        self.task_result.args = [self.wf.id]
        self.task_result.success = False
        self.task_result.stopped = datetime.datetime.now()
        self.task_result.result = "Execution exception"
        _audit.detail_by_workflow_id.return_value.audit_id = 123
        _audit.add_log.return_value = "any thing"
        # Remove execute_result and set status to executing.
        self.wf.sqlworkflowcontent.execute_result = ""
        self.wf.sqlworkflowcontent.save()
        self.wf.status = "workflow_executing"
        self.wf.save(update_fields=["status"])
        execute_callback(self.task_result)
        _audit.detail_by_workflow_id.assert_called_with(
            workflow_id=self.wf.id, workflow_type=2
        )
        kwargs = _audit.add_log.call_args.kwargs
        self.assertEqual(kwargs["audit_id"], 123)
        self.assertEqual(kwargs["operation_type"], 6)
        self.assertEqual(kwargs["operation_type_desc"], "Execution finished")
        self.assertTrue(kwargs["operation_info"].startswith("Execution result: "))
        self.assertEqual(kwargs["operator"], "")
        self.assertEqual(kwargs["operator_display"], "System")
        _notify.assert_called_once()


class TestTasks(TestCase):
    def setUp(self):
        self.Schedule = Schedule.objects.create(name="some_name")

    def tearDown(self):
        Schedule.objects.all().delete()

    @patch("sql.utils.tasks.schedule")
    def test_add_sql_schedule(self, _schedule):
        add_sql_schedule("test", datetime.datetime.now(), 1)
        _schedule.assert_called_once()

    def test_del_schedule(self):
        del_schedule("some_name")
        with self.assertRaises(Schedule.DoesNotExist):
            Schedule.objects.get(name="some_name")

    def test_del_schedule_not_exists(self):
        del_schedule("some_name1")

    def test_task_info(self):
        task_info("some_name")

    def test_task_info_not_exists(self):
        with self.assertRaises(Schedule.DoesNotExist):
            Schedule.objects.get(name="some_name1")


class TestDataMasking(TestCase):
    def setUp(self):
        self.superuser = User.objects.create(username="super", is_superuser=True)
        self.user = User.objects.create(username="user")
        self.ins = Instance.objects.create(
            instance_name="some_ins",
            type="slave",
            db_type="mysql",
            host="some_host",
            port=3306,
            user="ins_user",
            password="some_str",
        )
        self.sys_config = SysConfig()
        self.wf1 = SqlWorkflow.objects.create(
            workflow_name="workflow_name",
            group_id=1,
            group_name="group_name",
            engineer=self.superuser.username,
            engineer_display=self.superuser.display,
            audit_auth_groups="audit_auth_groups",
            create_time=datetime.datetime.now(),
            status="workflow_review_pass",
            is_backup=True,
            instance=self.ins,
            db_name="db_name",
            syntax_type=1,
        )
        # Create masking rules for unit tests.
        DataMaskingRules.objects.create(
            rule_type=1, rule_regex="(.{3})(.*)(.{4})", hide_group=2
        )
        DataMaskingColumns.objects.create(
            rule_type=1,
            active=True,
            instance=self.ins,
            table_schema="archer_test",
            table_name="users",
            column_name="phone",
        )
        # Rule type=100 is auto-created. Only add masked columns.
        DataMaskingColumns.objects.create(
            rule_type=100,
            active=True,
            instance=self.ins,
            table_schema="*",
            table_name="*",
            column_name="mobile",
        )

    def tearDown(self):
        User.objects.all().delete()
        SqlWorkflow.objects.all().delete()
        SqlWorkflowContent.objects.all().delete()
        DataMaskingColumns.objects.all().delete()
        DataMaskingRules.objects.all().delete()

    @patch("sql.utils.data_masking.GoInceptionEngine")
    def test_data_masking_not_hit_rules(self, _inception):
        DataMaskingColumns.objects.all().delete()
        DataMaskingRules.objects.all().delete()
        _inception.return_value.query_data_masking.return_value = [
            {
                "index": 0,
                "field": "phone",
                "type": "varchar(80)",
                "table": "users",
                "schema": "archer_test",
                "alias": "phone",
            }
        ]
        sql = """select phone from users;"""
        rows = (("18888888888",), ("18888888889",), ("18888888810",))
        query_result = ReviewSet(column_list=["phone"], rows=rows, full_sql=sql)
        r = data_masking(self.ins, "archery", sql, query_result)
        print("test_data_masking_not_hit_rules:", r.rows)
        self.assertEqual(r, query_result)

    @patch("sql.utils.data_masking.GoInceptionEngine")
    def test_data_masking_hit_rules_not_exists_star(self, _inception):
        """Result includes a null value returned from database."""
        _inception.return_value.query_data_masking.return_value = [
            {
                "index": 0,
                "field": "phone",
                "type": "varchar(80)",
                "table": "users",
                "schema": "archer_test",
                "alias": "phone",
            }
        ]
        sql = """select phone from users;"""
        rows = (("18888888888",), (None,), ("18888888889",), ("18888888810",))
        query_result = ReviewSet(column_list=["phone"], rows=rows, full_sql=sql)
        r = data_masking(self.ins, "archery", sql, query_result)
        print("test_data_masking_hit_rules_not_exists_star:", r.rows)
        mask_result_rows = [
            [
                "188****8888",
            ],
            [
                None,
            ],
            [
                "188****8889",
            ],
            [
                "188****8810",
            ],
        ]
        self.assertEqual(r.rows, mask_result_rows)

    @patch("sql.utils.data_masking.GoInceptionEngine")
    def test_data_masking_hit_rules_exists_star(self, _inception):
        """[*]"""
        _inception.return_value.query_data_masking.return_value = [
            {
                "index": 0,
                "field": "phone",
                "type": "varchar(80)",
                "table": "users",
                "schema": "archer_test",
                "alias": "phone",
            }
        ]
        sql = """select * from users;"""
        rows = (("18888888888",), ("18888888889",), ("18888888810",))
        query_result = ReviewSet(column_list=["phone"], rows=rows, full_sql=sql)
        r = data_masking(self.ins, "archery", sql, query_result)
        print("test_data_masking_hit_rules_exists_star:", r.rows)
        mask_result_rows = [
            [
                "188****8888",
            ],
            [
                "188****8889",
            ],
            [
                "188****8810",
            ],
        ]
        self.assertEqual(r.rows, mask_result_rows)

    @patch("sql.utils.data_masking.GoInceptionEngine")
    def test_data_masking_hit_rules_star_and_column(self, _inception):
        """[*,column_a]"""
        _inception.return_value.query_data_masking.return_value = [
            {
                "index": 0,
                "field": "phone",
                "type": "varchar(80)",
                "table": "users",
                "schema": "archer_test",
                "alias": "phone",
            },
            {
                "index": 1,
                "field": "phone",
                "type": "varchar(80)",
                "table": "users",
                "schema": "archer_test",
                "alias": "phone",
            },
        ]
        sql = """select *,phone from users;"""
        rows = (
            (
                "18888888888",
                "18888888888",
            ),
            (
                "18888888889",
                "18888888889",
            ),
        )
        query_result = ReviewSet(
            column_list=["phone", "phone"], rows=rows, full_sql=sql
        )
        r = data_masking(self.ins, "archery", sql, query_result)
        print("test_data_masking_hit_rules_star_and_column", r.rows)
        mask_result_rows = [
            [
                "188****8888",
                "188****8888",
            ],
            [
                "188****8889",
                "188****8889",
            ],
        ]
        self.assertEqual(r.rows, mask_result_rows)

    @patch("sql.utils.data_masking.GoInceptionEngine")
    def test_data_masking_hit_rules_column_and_star(self, _inception):
        """[column_a, *]"""
        _inception.return_value.query_data_masking.return_value = [
            {
                "index": 0,
                "field": "phone",
                "type": "varchar(80)",
                "table": "users",
                "schema": "archer_test",
                "alias": "phone",
            },
            {
                "index": 1,
                "field": "phone",
                "type": "varchar(80)",
                "table": "users",
                "schema": "archer_test",
                "alias": "phone",
            },
        ]
        sql = """select phone,* from users;"""
        rows = (
            (
                "18888888888",
                "18888888888",
            ),
            (
                "18888888889",
                "18888888889",
            ),
        )
        query_result = ReviewSet(
            column_list=["phone", "phone"], rows=rows, full_sql=sql
        )
        r = data_masking(self.ins, "archery", sql, query_result)
        print("test_data_masking_hit_rules_column_and_star", r.rows)
        mask_result_rows = [
            [
                "188****8888",
                "188****8888",
            ],
            [
                "188****8889",
                "188****8889",
            ],
        ]
        self.assertEqual(r.rows, mask_result_rows)

    @patch("sql.utils.data_masking.GoInceptionEngine")
    def test_data_masking_hit_default_rules_column_and_star(self, _inception):
        """Hit default masking rule (rule code 100) when SQL includes * and fields.
        1. Masking rule: schema=* table=* field=mobile.
        2. Masking rule: schema=archer_test table=users field=phone.
        """
        _inception.return_value.query_data_masking.return_value = [
            {
                "index": 0,
                "field": "phone",
                "type": "varchar(80)",
                "table": "users",
                "schema": "archer_test",
                "alias": "p",
            },
            {
                "index": 1,
                "field": "id",
                "type": "varchar(80)",
                "table": "users",
                "schema": "archer_test",
                "alias": "id",
            },
            {
                "index": 2,
                "field": "mobile",
                "type": "varchar(80)",
                "table": "users_not_config",
                "schema": "archer_test_not_config",
                "alias": "m",
            },
        ]
        sql = """select phone,id,mobile,* from users;"""
        rows = (
            ("1", "7954597708277300617", "1"),
            ("12", "7954597708277300618", "12"),
            ("123", "7954597708277300621", "123"),
            ("1234", "7954597708277300622", "1234"),
            ("12345", "7954597708277300623", "12345"),
            ("123456", "7955140019084306231", "123456"),
            ("1234567", "7955140019084306241", "1234567"),
            ("12345678", "7955140019084306242", "12345678"),
            ("123456789", "7955140019084306243", "123456789"),
            ("123456789a", "7955140019084306244", "123456789a"),
            ("123456789ab", "7955140019084306245", "123456789ab"),
            ("123456789abc", "7955140019084306246", "123456789abc"),
            ("123456789abcd", "7955140019084306247", "123456789abcd"),
            ("123456789abcde", "7955140019084306248", "123456789abcde"),
        )
        query_result = ReviewSet(
            column_list=["phone", "id", "mobile"], rows=rows, full_sql=sql
        )
        r = data_masking(self.ins, "archery", sql, query_result)
        # First column uses masking rule 1, second column id should not be
        # masked, third column uses masking rule 100.
        mask_result_rows = [
            ["1", "7954597708277300617", "*"],
            ["12", "7954597708277300618", "*2"],
            ["123", "7954597708277300621", "1*3"],
            ["1234", "7954597708277300622", "1**4"],
            ["12345", "7954597708277300623", "1**45"],
            ["123456", "7955140019084306231", "12**56"],
            ["1234567", "7955140019084306241", "12***67"],
            ["123*5678", "7955140019084306242", "12***678"],
            ["123**6789", "7955140019084306243", "123***789"],
            ["123***789a", "7955140019084306244", "123****89a"],
            ["123****89ab", "7955140019084306245", "123****89ab"],
            ["123*****9abc", "7955140019084306246", "1234****9abc"],
            ["123******abcd", "7955140019084306247", "1234*****abcd"],
            ["123*******bcde", "7955140019084306248", "1234*****abcde"],
        ]

        self.assertEqual(r.rows, mask_result_rows)

    @patch("sql.utils.data_masking.GoInceptionEngine")
    def test_data_masking_hit_rules_column_and_star_and_column(self, _inception):
        """[column_a,a.*,column_b]"""
        _inception.return_value.query_data_masking.return_value = [
            {
                "index": 0,
                "field": "phone",
                "type": "varchar(80)",
                "table": "users",
                "schema": "archer_test",
                "alias": "phone",
            },
            {
                "index": 1,
                "field": "phone",
                "type": "varchar(80)",
                "table": "users",
                "schema": "archer_test",
                "alias": "phone",
            },
            {
                "index": 2,
                "field": "phone",
                "type": "varchar(80)",
                "table": "users",
                "schema": "archer_test",
                "alias": "phone",
            },
        ]
        sql = """select phone,*,phone from users;"""
        rows = (
            (
                "18888888888",
                "18888888888",
                "18888888888",
            ),
            (
                "18888888889",
                "18888888889",
                "18888888889",
            ),
        )
        query_result = ReviewSet(
            column_list=["phone", "phone", "phone"], rows=rows, full_sql=sql
        )
        r = data_masking(self.ins, "archery", sql, query_result)
        print("test_data_masking_hit_rules_column_and_star_and_column", r.rows)
        mask_result_rows = [
            [
                "188****8888",
                "188****8888",
                "188****8888",
            ],
            [
                "188****8889",
                "188****8889",
                "188****8889",
            ],
        ]
        self.assertEqual(r.rows, mask_result_rows)

    @patch("sql.utils.data_masking.GoInceptionEngine")
    def test_data_masking_hit_rules_star_and_column_and_star(self, _inception):
        """[a.*, column_a, b.*]"""
        _inception.return_value.query_data_masking.return_value = [
            {
                "index": 0,
                "field": "phone",
                "type": "varchar(80)",
                "table": "users",
                "schema": "archer_test",
                "alias": "phone",
            },
            {
                "index": 1,
                "field": "phone",
                "type": "varchar(80)",
                "table": "users",
                "schema": "archer_test",
                "alias": "phone",
            },
            {
                "index": 2,
                "field": "phone",
                "type": "varchar(80)",
                "table": "users",
                "schema": "archer_test",
                "alias": "phone",
            },
        ]
        sql = """select a.*,phone,a.* from users a;"""
        rows = (
            (
                "18888888888",
                "18888888888",
                "18888888888",
            ),
            (
                "18888888889",
                "18888888889",
                "18888888889",
            ),
        )
        query_result = ReviewSet(
            column_list=["phone", "phone", "phone"], rows=rows, full_sql=sql
        )
        r = data_masking(self.ins, "archery", sql, query_result)
        print("test_data_masking_hit_rules_star_and_column_and_star", r.rows)
        mask_result_rows = [
            [
                "188****8888",
                "188****8888",
                "188****8888",
            ],
            [
                "188****8889",
                "188****8889",
                "188****8889",
            ],
        ]
        self.assertEqual(r.rows, mask_result_rows)

    @patch("sql.utils.data_masking.GoInceptionEngine")
    def test_data_masking_concat_function_support(self, _inception):
        """Support concat_ function."""
        _inception.return_value.query_data_masking.return_value = [
            {
                "index": 0,
                "field": "phone",
                "type": "varchar(80)",
                "table": "users",
                "schema": "archer_test",
                "alias": "concat(phone,1)",
            }
        ]
        sql = """select concat(phone,1) from users;"""
        rows = (("18888888888",), ("18888888889",), ("18888888810",))
        query_result = ReviewSet(
            column_list=["concat(phone,1)"], rows=rows, full_sql=sql
        )
        r = data_masking(self.ins, "archery", sql, query_result)
        mask_result_rows = [
            [
                "188****8888",
            ],
            [
                "188****8889",
            ],
            [
                "188****8810",
            ],
        ]
        print("test_data_masking_concat_function_support", r.rows)
        self.assertEqual(r.rows, mask_result_rows)

    @patch("sql.utils.data_masking.GoInceptionEngine")
    def test_data_masking_max_function_support(self, _inception):
        """Support max_ function."""
        _inception.return_value.query_data_masking.return_value = [
            {
                "index": 0,
                "field": "phone",
                "type": "varchar(80)",
                "table": "users",
                "schema": "archer_test",
                "alias": "max(phone+1)",
            }
        ]
        sql = """select max(phone+1) from users;"""
        rows = (("18888888888",), ("18888888889",), ("18888888810",))
        query_result = ReviewSet(column_list=["max(phone+1)"], rows=rows, full_sql=sql)
        mask_result_rows = [
            [
                "188****8888",
            ],
            [
                "188****8889",
            ],
            [
                "188****8810",
            ],
        ]
        r = data_masking(self.ins, "archery", sql, query_result)
        print("test_data_masking_max_function_support", r.rows)
        self.assertEqual(r.rows, mask_result_rows)

    @patch("sql.utils.data_masking.GoInceptionEngine")
    def test_data_masking_union_support_keyword(self, _inception):
        """Union keyword support."""
        self.sys_config.set("query_check", "true")
        self.sys_config.get_all_config()
        _inception.return_value.query_data_masking.return_value = [
            {
                "index": 0,
                "field": "phone",
                "type": "varchar(80)",
                "table": "users",
                "schema": "archer_test",
                "alias": "phone",
            },
            {
                "index": 1,
                "field": "phone",
                "type": "varchar(80)",
                "table": "users",
                "schema": "archer_test",
                "alias": "phone",
            },
        ]
        sqls = [
            "select phone from users union select phone from users;",
            "select phone from users union all select phone from users;",
        ]
        rows = (("18888888888",), ("18888888889",), ("18888888810",))
        mask_result_rows = [
            [
                "188****8888",
            ],
            [
                "188****8889",
            ],
            [
                "188****8810",
            ],
        ]
        for sql in sqls:
            query_result = ReviewSet(column_list=["phone"], rows=rows, full_sql=sql)
            r = data_masking(self.ins, "archery", sql, query_result)
            print("test_data_masking_union_support_keyword", r.rows)
            self.assertEqual(r.rows, mask_result_rows)

    def test_brute_mask(self):
        sql = """select * from users;"""
        rows = (("18888888888",), ("18888888889",), ("18888888810",))
        query_result = ReviewSet(column_list=["phone"], rows=rows, full_sql=sql)
        r = brute_mask(self.ins, query_result)
        mask_result_rows = [("188****8888",), ("188****8889",), ("188****8810",)]
        self.assertEqual(r.rows, mask_result_rows)

    def test_simple_column_mask(self):
        sql = """select * from users;"""
        rows = (("18888888888",), ("18888888889",), ("18888888810",))
        query_result = ReviewSet(column_list=["phone"], rows=rows, full_sql=sql)
        r = simple_column_mask(self.ins, query_result)
        mask_result_rows = [("188****8888",), ("188****8889",), ("188****8810",)]
        self.assertEqual(r.rows, mask_result_rows)


class TestResourceGroup(TestCase):
    def setUp(self):
        self.sys_config = SysConfig()
        self.user = User.objects.create(
            username="test_user", display="display_name", is_active=True
        )
        self.su = User.objects.create(
            username="s_user", display="display_name", is_active=True, is_superuser=True
        )
        self.ins1 = Instance.objects.create(
            instance_name="some_ins1",
            type="slave",
            db_type="mysql",
            host="some_host",
            port=3306,
            user="ins_user",
            password="some_str",
        )
        self.ins2 = Instance.objects.create(
            instance_name="some_ins2",
            type="slave",
            db_type="mysql",
            host="some_host",
            port=3306,
            user="ins_user",
            password="some_str",
        )
        self.rgp1 = ResourceGroup.objects.create(group_name="group1")
        self.rgp2 = ResourceGroup.objects.create(group_name="group2")
        self.agp = Group.objects.create(name="auth_group")

    def tearDown(self):
        self.sys_config.purge()
        User.objects.all().delete()
        Instance.objects.all().delete()
        ResourceGroup.objects.all().delete()
        Group.objects.all().delete()

    def test_user_groups_super(self):
        """Get resource groups for superuser."""
        groups = user_groups(self.su)
        self.assertEqual(groups.__len__(), 2)
        self.assertIn(self.rgp1, groups)
        self.assertIn(self.rgp2, groups)

    def test_user_groups(self):
        """Get resource groups for normal user."""
        self.user.resource_group.add(self.rgp1)
        groups = user_groups(self.user)
        self.assertEqual(groups.__len__(), 1)
        self.assertIn(self.rgp1, groups)

    def test_user_instances_super(self):
        """Get instance list for superuser."""
        self.ins1.resource_group.add(self.rgp1)
        ins = user_instances(self.su)
        self.assertEqual(ins.__len__(), 2)
        self.assertIn(self.ins1, ins)
        self.assertIn(self.ins2, ins)

    def test_user_instances_associated_group(self):
        """Get instance list for normal user with associated resource group."""
        self.user.resource_group.add(self.rgp1)
        self.ins1.resource_group.add(self.rgp1)
        ins = user_instances(self.user)
        self.assertEqual(ins.__len__(), 1)
        self.assertIn(self.ins1, ins)

    def test_user_instances_unassociated_group(self):
        """Get instance list for normal user without associated resource group."""
        self.ins1.resource_group.add(self.rgp1)
        ins = user_instances(self.user)
        self.assertEqual(ins.__len__(), 0)

    def test_auth_group_users(self):
        """Get users in resource group associated with a specific auth group."""
        # Associate user with auth group.
        self.user.groups.add(self.agp)
        # Associate user with resource group.
        self.user.resource_group.add(self.rgp1)
        # Get users in resource group associated with target auth group.
        users = auth_group_users(
            auth_group_names=[self.agp.name], group_id=self.rgp1.group_id
        )
        self.assertIn(self.user, users)
