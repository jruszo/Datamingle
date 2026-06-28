"""Supplementary tests for models.py."""

import importlib

from django.apps import apps
from django.conf import settings
from django.contrib.auth.models import Group
from django.test import TestCase

from sql.models import Instance, Team, WorkflowAuditSetting


def test_password_mixin_import_error():
    settings.PASSWORD_MIXIN_PATH = "sql.not_found:ErrorMixin"
    from sql.models import PasswordMixin

    assert PasswordMixin.__name__ == "DummyMixin"


class WorkflowPolicyBackfillMigrationTests(TestCase):
    def test_backfill_skips_queryable_only_instances(self):
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

        migration = importlib.import_module(
            "sql.migrations.0042_backfill_workflow_policies"
        )
        migration.backfill_workflow_policies(apps, None)

        queryable_only.refresh_from_db()
        workflow_capable.refresh_from_db()
        self.assertIsNone(queryable_only.workflow_policy_id)
        self.assertIsNotNone(workflow_capable.workflow_policy_id)
