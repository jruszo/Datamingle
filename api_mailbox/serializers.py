from rest_framework import serializers

from sql.models import MailboxItem


class MailboxItemSerializer(serializers.ModelSerializer):
    category_label = serializers.CharField(
        source="get_category_display", read_only=True
    )
    source_type_label = serializers.CharField(
        source="get_source_type_display", read_only=True
    )

    class Meta:
        model = MailboxItem
        fields = (
            "id",
            "category",
            "category_label",
            "source_type",
            "source_type_label",
            "source_id",
            "title",
            "body",
            "action_path",
            "is_unread",
            "read_at",
            "resolved_at",
            "metadata",
            "create_time",
            "sys_time",
        )


class MailboxSummarySerializer(serializers.Serializer):
    unread_count = serializers.IntegerField()
    items = MailboxItemSerializer(many=True)
