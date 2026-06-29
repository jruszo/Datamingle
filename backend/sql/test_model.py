"""Supplementary tests for models.py."""

import importlib

from django.conf import settings
from django.db import connection
from django.db.migrations.executor import MigrationExecutor
from django.test import TestCase


def test_password_mixin_import_error():
    settings.PASSWORD_MIXIN_PATH = "sql.not_found:ErrorMixin"
    from sql.models import PasswordMixin

    assert PasswordMixin.__name__ == "DummyMixin"


class WorkflowPolicyBackfillMigrationTests(TestCase):
    @staticmethod
    def _migration_apps():
        executor = MigrationExecutor(connection)
        return executor.loader.project_state(
            [("sql", "0041_sqlworkflow_workflow_policy_name_workflowpolicy_and_more")]
        ).apps

    @staticmethod
    def _migration_module():
        return importlib.import_module("sql.migrations.0042_backfill_workflow_policies")

    def test_backfill_skips_queryable_only_instances(self):
        apps = self._migration_apps()
        Group = apps.get_model("auth", "Group")
        Instance = apps.get_model("sql", "Instance")
        Team = apps.get_model("sql", "Team")
        WorkflowAuditSetting = apps.get_model("sql", "WorkflowAuditSetting")

        group = Group.objects.create(name="Backfill DBA")
        team = Team.objects.create(
            team_name="backfill-team",
            group_parent_id=0,
            group_sort=1,
            group_level=1,
            is_deleted=0,
        )
        WorkflowAuditSetting.objects.create(
            team_id=team.team_id,
            workflow_type=2,
            audit_auth_groups=str(group.id),
        )
        queryable_only = Instance.objects.create(
            instance_name="queryable-only",
            type="master",
            db_type="mysql",
            host="127.0.0.1",
            port=3306,
            queryable=True,
            workflow_enabled=False,
        )
        workflow_capable = Instance.objects.create(
            instance_name="workflow-capable",
            type="master",
            db_type="mysql",
            host="127.0.0.1",
            port=3306,
            queryable=False,
            workflow_enabled=True,
        )
        queryable_only.resource_group.set([team])
        workflow_capable.resource_group.set([team])

        migration = self._migration_module()
        migration.backfill_workflow_policies(apps, None)

        queryable_only.refresh_from_db()
        workflow_capable.refresh_from_db()
        self.assertIsNone(queryable_only.workflow_policy_id)
        self.assertIsNotNone(workflow_capable.workflow_policy_id)

    def test_backfill_fails_when_workflow_enabled_instance_has_no_policy_mapping(self):
        apps = self._migration_apps()
        Instance = apps.get_model("sql", "Instance")

        instance = Instance.objects.create(
            instance_name="workflow-without-setting",
            type="master",
            db_type="mysql",
            host="127.0.0.1",
            port=3306,
            workflow_enabled=True,
        )

        migration = self._migration_module()
        with self.assertRaisesRegex(
            RuntimeError,
            f"workflow-without-setting:{instance.id}",
        ):
            migration.backfill_workflow_policies(apps, None)
