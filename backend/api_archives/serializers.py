from rest_framework import serializers

from sql.models import ArchiveConfig


class ArchiveConfigSerializer(serializers.ModelSerializer):
    class Meta:
        model = ArchiveConfig
        fields = (
            "id",
            "title",
            "team",
            "audit_auth_groups",
            "src_instance",
            "src_db_name",
            "src_table_name",
            "dest_instance",
            "dest_db_name",
            "dest_table_name",
            "condition",
            "mode",
            "no_delete",
            "sleep",
            "archive_method",
            "execution_mode",
            "schedule_frequency",
            "schedule_time",
            "schedule_weekdays",
            "next_run_at",
            "status",
            "state",
            "execution_state",
            "consecutive_failures",
            "user_name",
            "user_display",
            "create_time",
            "last_archive_time",
        )
