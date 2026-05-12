from django.test import SimpleTestCase

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
                "group_name": "g" * 101,
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
        self.assertIn("group_name", serializer.errors)
        self.assertIn("db_name", serializer.errors)
        self.assertIn("table_list", serializer.errors)
