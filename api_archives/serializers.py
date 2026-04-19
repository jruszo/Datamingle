from rest_framework import serializers

from sql.models import ArchiveConfig


class ArchiveConfigSerializer(serializers.ModelSerializer):
    class Meta:
        model = ArchiveConfig
        fields = "__all__"
