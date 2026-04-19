from api_core.legacy_tests import TestInstance
from django.test import TestCase
from rest_framework import serializers
from unittest.mock import patch

from api_instances.serializers import AliyunRdsSerializer, TunnelSerializer


class InstanceSerializerTests(TestCase):
    def test_tunnel_sensitive_fields_are_write_only(self):
        serializer = TunnelSerializer()

        self.assertTrue(serializer.fields["password"].write_only)
        self.assertTrue(serializer.fields["pkey"].write_only)
        self.assertTrue(serializer.fields["pkey_password"].write_only)

    @patch(
        "api_instances.serializers.CloudAccessKey.objects.create",
        side_effect=RuntimeError("secret failure"),
    )
    def test_aliyun_rds_create_hides_internal_errors(self, _mock_create):
        serializer = AliyunRdsSerializer()

        with self.assertRaises(serializers.ValidationError) as ctx:
            serializer.create({"ak": {}})

        self.assertIn(
            "Unable to create Aliyun RDS configuration.",
            str(ctx.exception.detail["errors"]),
        )
