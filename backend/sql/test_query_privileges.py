import json
from datetime import datetime, timedelta

from django.conf import settings
from django.test import TestCase

import sql.query_privileges
from common.config import SysConfig
from sql.models import Instance, QueryPrivileges, QueryPrivilegesApply, Team
from sql.tests import User


class TestQueryPrivilegesApply(TestCase):
    """Test privilege lifecycle helpers that remain independent of removed routes."""

    def setUp(self):
        self.superuser = User.objects.create(username="super", is_superuser=True)
        self.user = User.objects.create(username="user")
        self.slave = Instance.objects.create(
            instance_name="test_instance",
            type="slave",
            db_type="mysql",
            host=settings.DATABASES["default"]["HOST"],
            port=settings.DATABASES["default"]["PORT"],
            user=settings.DATABASES["default"]["USER"],
            password=settings.DATABASES["default"]["PASSWORD"],
        )
        self.sys_config = SysConfig()
        tomorrow = datetime.today() + timedelta(days=1)
        self.group = Team.objects.create(team_id=1, team_name="team_name")
        self.query_apply_1 = QueryPrivilegesApply.objects.create(
            team_id=self.group.team_id,
            team_name=self.group.team_name,
            title="some_title1",
            user_name="some_user",
            instance=self.slave,
            db_list="some_db,some_db2",
            limit_num=100,
            valid_date=tomorrow,
            priv_type=1,
            status=0,
            audit_auth_groups="some_audit_group",
        )
        self.query_apply_2 = QueryPrivilegesApply.objects.create(
            team_id=2,
            team_name="some_group2",
            title="some_title2",
            user_name="some_user",
            instance=self.slave,
            db_list="some_db",
            table_list="some_table,some_tb2",
            limit_num=100,
            valid_date=tomorrow,
            priv_type=2,
            status=0,
            audit_auth_groups="some_audit_group",
        )

    def tearDown(self):
        self.superuser.delete()
        self.user.delete()
        Instance.objects.all().delete()
        Team.objects.all().delete()
        QueryPrivilegesApply.objects.all().delete()
        QueryPrivileges.objects.all().delete()
        self.sys_config.replace(json.dumps({}))

    def test_query_audit_call_back(self):
        sql.query_privileges._query_apply_audit_call_back(
            self.query_apply_1.apply_id, 2
        )
        self.query_apply_1.refresh_from_db()
        self.assertEqual(self.query_apply_1.status, 2)
        for db in self.query_apply_1.db_list.split(","):
            self.assertEqual(
                len(
                    QueryPrivileges.objects.filter(
                        user_name=self.query_apply_1.user_name,
                        db_name=db,
                        limit_num=100,
                    )
                ),
                0,
            )

        sql.query_privileges._query_apply_audit_call_back(
            self.query_apply_1.apply_id, 1
        )
        self.query_apply_1.refresh_from_db()
        self.assertEqual(self.query_apply_1.status, 1)
        for db in self.query_apply_1.db_list.split(","):
            self.assertEqual(
                len(
                    QueryPrivileges.objects.filter(
                        user_name=self.query_apply_1.user_name,
                        db_name=db,
                        limit_num=100,
                    )
                ),
                1,
            )

    def test_query_audit_call_back_table_privileges(self):
        sql.query_privileges._query_apply_audit_call_back(
            self.query_apply_2.apply_id, 1
        )
        self.query_apply_2.refresh_from_db()
        self.assertEqual(self.query_apply_2.status, 1)
        for tb in self.query_apply_2.table_list.split(","):
            self.assertEqual(
                len(
                    QueryPrivileges.objects.filter(
                        user_name=self.query_apply_2.user_name,
                        db_name=self.query_apply_2.db_list,
                        table_name=tb,
                        limit_num=self.query_apply_2.limit_num,
                    )
                ),
                1,
            )

    def test_table_ref_uses_local_sql_parser_and_defaults_schema(self):
        table_ref = sql.query_privileges._table_ref(
            "select u.id from users u join analytics.events e on e.user_id = u.id",
            self.slave,
            "appdb",
        )

        self.assertEqual(
            table_ref,
            [
                {"schema": "appdb", "name": "users"},
                {"schema": "analytics", "name": "events"},
            ],
        )
