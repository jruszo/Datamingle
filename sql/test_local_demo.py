from django.core.management import call_command
from django.test import TestCase

from sql.local_demo import (
    DEMO_APP_PASSWORD,
    managed_demo_instance_names,
    managed_demo_resource_group_names,
    managed_demo_usernames,
)
from sql.models import Instance, InstanceTag, ResourceGroup, Users, WorkflowAuditSetting


class TestLocalDemoSeed(TestCase):
    def test_seed_local_demo_is_idempotent(self):
        call_command("seed_local_demo")
        call_command("seed_local_demo")

        self.assertEqual(
            Users.objects.filter(username__in=managed_demo_usernames()).count(),
            len(managed_demo_usernames()),
        )
        self.assertEqual(
            Instance.objects.filter(
                instance_name__in=managed_demo_instance_names()
            ).count(),
            len(managed_demo_instance_names()),
        )
        self.assertEqual(
            ResourceGroup.objects.filter(
                group_name__in=managed_demo_resource_group_names(), is_deleted=0
            ).count(),
            len(managed_demo_resource_group_names()),
        )
        self.assertEqual(InstanceTag.objects.filter(tag_code="can_read").count(), 1)
        self.assertEqual(InstanceTag.objects.filter(tag_code="can_write").count(), 1)
        self.assertEqual(WorkflowAuditSetting.objects.count(), 2)

        requester = Users.objects.get(username="demo_requester")
        self.assertTrue(requester.check_password(DEMO_APP_PASSWORD))
        self.assertEqual(requester.groups.values_list("name", flat=True).get(), "RD")
        self.assertTrue(requester.has_perm("sql.sqlexport_submit"))
        self.assertTrue(requester.has_perm("sql.menu_sqlexportworkflow"))
        self.assertEqual(
            set(requester.resource_group.values_list("group_name", flat=True)),
            {
                "Demo Workflow Single Stage",
                "Demo Workflow Multi Stage",
            },
        )

        pm_user = Users.objects.get(username="demo_pm")
        self.assertEqual(
            set(pm_user.resource_group.values_list("group_name", flat=True)),
            {"Demo Workflow Multi Stage"},
        )

        dba_user = Users.objects.get(username="demo_dba")
        self.assertTrue(dba_user.has_perm("sql.offline_download"))
        self.assertTrue(dba_user.has_perm("sql.menu_sqlexportworkflow"))

        mysql_instance = Instance.objects.get(instance_name="demo-mysql-workflow")
        self.assertEqual(mysql_instance.host, "mysql_demo")
        self.assertEqual(mysql_instance.port, 3306)
        self.assertEqual(mysql_instance.db_type, "mysql")

        pg_instance = Instance.objects.get(instance_name="demo-pgsql-workflow")
        self.assertEqual(pg_instance.host, "postgres_demo")
        self.assertEqual(pg_instance.port, 5432)
        self.assertEqual(pg_instance.db_type, "pgsql")
