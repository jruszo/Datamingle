from api_core.legacy_tests import ArchiveApiTests
from django.test import SimpleTestCase

from api_archives.serializers import ArchiveConfigSerializer


class ArchiveSerializerTests(SimpleTestCase):
    def test_archive_config_serializer_uses_explicit_fields(self):
        serializer = ArchiveConfigSerializer()

        self.assertNotIn("__all__", getattr(serializer.Meta, "fields", ()))
        self.assertNotIn("sys_time", serializer.fields)
