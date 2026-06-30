"""Supplementary tests for models.py."""

import importlib

from django.conf import settings
from django.contrib.auth.models import Group
from django.db import connection
from django.db.migrations.executor import MigrationExecutor
from django.test import TestCase

from sql.models import Instance, Team, WorkflowAuditSetting


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


class MysqlTopologyRolloutBackfillMigrationTests(TestCase):
    @staticmethod
    def _migration_apps():
        executor = MigrationExecutor(connection)
        return executor.loader.project_state(
            [("sql", "0043_instance_mysql_cluster_membership_source_and_more")]
        ).apps

    @staticmethod
    def _migration_module():
        return importlib.import_module(
            "sql.migrations.0044_backfill_mysql_topology_rollout"
        )

    def test_backfill_keeps_existing_workflow_enabled_mysql_targets_visible(self):
        apps = self._migration_apps()
        Instance = apps.get_model("sql", "Instance")

        workflow_enabled = Instance.objects.create(
            instance_name="mysql-workflow-enabled",
            type="master",
            db_type="mysql",
            host="127.0.0.1",
            port=3306,
            workflow_enabled=True,
            mysql_topology_status="unknown",
            mysql_topology_role="unknown",
            mysql_ddl_dml_eligible=False,
        )
        queryable_only = Instance.objects.create(
            instance_name="mysql-queryable-only",
            type="master",
            db_type="mysql",
            host="127.0.0.1",
            port=3307,
            queryable=True,
            workflow_enabled=False,
            mysql_topology_status="unknown",
            mysql_topology_role="unknown",
            mysql_ddl_dml_eligible=False,
        )
        replica = Instance.objects.create(
            instance_name="mysql-replica",
            type="slave",
            db_type="mysql",
            host="127.0.0.1",
            port=3308,
            workflow_enabled=True,
            mysql_topology_status="unknown",
            mysql_topology_role="unknown",
            mysql_ddl_dml_eligible=False,
        )
        non_mysql = Instance.objects.create(
            instance_name="pgsql-workflow-enabled",
            type="master",
            db_type="pgsql",
            host="127.0.0.1",
            port=5432,
            workflow_enabled=True,
            mysql_topology_status="unknown",
            mysql_topology_role="unknown",
            mysql_ddl_dml_eligible=False,
        )

        migration = self._migration_module()
        migration.backfill_mysql_topology_rollout(apps, None)

        workflow_enabled.refresh_from_db()
        queryable_only.refresh_from_db()
        replica.refresh_from_db()
        non_mysql.refresh_from_db()
        self.assertEqual(workflow_enabled.mysql_topology_status, "standalone")
        self.assertEqual(workflow_enabled.mysql_topology_role, "standalone")
        self.assertTrue(workflow_enabled.mysql_ddl_dml_eligible)
        self.assertEqual(workflow_enabled.mysql_ddl_dml_block_reason, "")
        self.assertEqual(queryable_only.mysql_topology_status, "unknown")
        self.assertFalse(queryable_only.mysql_ddl_dml_eligible)
        self.assertEqual(replica.mysql_topology_status, "unknown")
        self.assertFalse(replica.mysql_ddl_dml_eligible)
        self.assertEqual(non_mysql.mysql_topology_status, "unknown")
        self.assertFalse(non_mysql.mysql_ddl_dml_eligible)
