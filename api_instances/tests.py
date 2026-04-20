from api_core.legacy_tests import TestInstance
from django.test import TestCase

from api_instances.serializers import (
    InstanceCreateSerializer,
    InstanceResourceSerializer,
)


class InstanceSerializerTests(TestCase):
    def test_instance_create_serializer_trims_host_and_optional_strings(self):
        serializer = InstanceCreateSerializer(
            data={
                "instance_name": " demo ",
                "type": "master",
                "db_type": "mysql",
                "host": " host.example ",
                "port": 3306,
                "user": " demo_user ",
                "db_name": " demo_db ",
                "charset": " utf8mb4 ",
                "service_name": " svc ",
                "sid": " sid ",
            }
        )

        self.assertTrue(serializer.is_valid(), serializer.errors)
        self.assertEqual(serializer.validated_data["host"], "host.example")
        self.assertEqual(serializer.validated_data["user"], "demo_user")
        self.assertEqual(serializer.validated_data["db_name"], "demo_db")
        self.assertEqual(serializer.validated_data["charset"], "utf8mb4")
        self.assertEqual(serializer.validated_data["service_name"], "svc")
        self.assertEqual(serializer.validated_data["sid"], "sid")

    def test_instance_resource_serializer_allows_blank_optional_names(self):
        serializer = InstanceResourceSerializer(
            data={
                "instance_id": 1,
                "resource_type": "database",
                "db_name": "",
                "schema_name": "",
                "tb_name": "",
            }
        )

        self.assertTrue(serializer.is_valid(), serializer.errors)
