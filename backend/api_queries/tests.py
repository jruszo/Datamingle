from django.test import SimpleTestCase

from api_queries.views import _static_mysql_query_check
from api_queries.serializers import (
    QueryExecuteSerializer,
    QueryFavoriteSerializer,
    QueryPrivilegesApplyCreateSerializer,
)


class QuerySerializerTests(SimpleTestCase):
    def test_query_favorite_alias_enforces_database_limit(self):
        serializer = QueryFavoriteSerializer(
            data={"query_log_id": 1, "star": True, "alias": "a" * 65}
        )

        self.assertFalse(serializer.is_valid())
        self.assertIn("alias", serializer.errors)

    def test_query_execute_limits_database_and_table_names(self):
        serializer = QueryExecuteSerializer(
            data={
                "instance_name": "i" * 51,
                "sql_content": "select 1",
                "db_name": "d" * 65,
                "tb_name": "t" * 65,
            }
        )

        self.assertFalse(serializer.is_valid())
        self.assertIn("instance_name", serializer.errors)
        self.assertIn("db_name", serializer.errors)
        self.assertIn("tb_name", serializer.errors)

    def test_query_privileges_apply_create_limits_name_fields(self):
        serializer = QueryPrivilegesApplyCreateSerializer(
            data={
                "title": "t" * 51,
                "instance_name": "i" * 51,
                "team_name": "g" * 101,
                "priv_type": 2,
                "db_name": "d" * 65,
                "table_list": ["t" * 65],
                "valid_date": "2026-04-19",
                "limit_num": 1,
            }
        )

        self.assertFalse(serializer.is_valid())
        self.assertIn("title", serializer.errors)
        self.assertIn("instance_name", serializer.errors)
        self.assertIn("team_name", serializer.errors)
        self.assertIn("db_name", serializer.errors)
        self.assertIn("table_list", serializer.errors)


class StaticMysqlQueryCheckTests(SimpleTestCase):
    def test_count_star_is_not_treated_as_select_star(self):
        result = _static_mysql_query_check("test", "select count(*) from demo")

        self.assertFalse(result["bad_query"])
        self.assertFalse(result["has_star"])

    def test_projection_wildcard_is_detected(self):
        result = _static_mysql_query_check("test", "select demo.* from demo")

        self.assertFalse(result["bad_query"])
        self.assertTrue(result["has_star"])

    def test_mysql_user_is_detected_from_identifiers(self):
        explicit_schema = _static_mysql_query_check(
            "test", "select count(*) from mysql.user"
        )
        implicit_mysql_db = _static_mysql_query_check("mysql", "select id from `user`")

        self.assertTrue(explicit_schema["bad_query"])
        self.assertTrue(implicit_mysql_db["bad_query"])
