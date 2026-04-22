from drf_spectacular.utils import OpenApiParameter, OpenApiTypes, extend_schema
from django.shortcuts import get_object_or_404
from rest_framework import permissions, views
from rest_framework.generics import ListAPIView

from api_core.pagination import CustomizedPagination
from api_core.response import success_response
from api_mailbox.serializers import MailboxItemSerializer, MailboxSummarySerializer
from sql.mailbox import (
    mark_all_mailbox_items_read,
    mark_mailbox_item_read,
    preview_mailbox_items,
)
from sql.models import MailboxItem


class MailboxSummaryView(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="Mailbox Summary",
        responses={200: MailboxSummarySerializer},
        description="Return unread count and recent preview items for the authenticated user.",
    )
    def get(self, request):
        unread_count = MailboxItem.objects.filter(
            recipient=request.user,
            is_unread=True,
        ).count()
        items = preview_mailbox_items(request.user, limit=5)
        serializer = MailboxSummarySerializer(
            {
                "unread_count": unread_count,
                "items": MailboxItemSerializer(items, many=True).data,
            }
        )
        return success_response(data=serializer.data)


class MailboxItemListView(ListAPIView):
    permission_classes = [permissions.IsAuthenticated]
    pagination_class = CustomizedPagination
    serializer_class = MailboxItemSerializer

    @extend_schema(
        summary="Mailbox Items",
        responses={200: MailboxItemSerializer(many=True)},
        parameters=[
            OpenApiParameter(
                name="state",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY,
                description="Filter by read state: unread, read, or all.",
            ),
            OpenApiParameter(
                name="category",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY,
                description="Filter by mailbox category.",
            ),
            OpenApiParameter(
                name="source_type",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY,
                description="Filter by source type.",
            ),
        ],
        description="List mailbox items for the authenticated user.",
    )
    def get(self, request, *args, **kwargs):
        return self.list(request, *args, **kwargs)

    def get_queryset(self):
        queryset = MailboxItem.objects.filter(recipient=self.request.user).order_by(
            "-create_time",
            "-id",
        )
        state = self.request.query_params.get("state", "all").strip().lower()
        category = self.request.query_params.get("category", "").strip()
        source_type = self.request.query_params.get("source_type", "").strip()

        if state == "unread":
            queryset = queryset.filter(is_unread=True)
        elif state == "read":
            queryset = queryset.filter(is_unread=False)

        if category:
            queryset = queryset.filter(category=category)
        if source_type:
            queryset = queryset.filter(source_type=source_type)
        return queryset


class MailboxItemReadView(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="Mark Mailbox Item Read",
        responses={200: MailboxItemSerializer},
        description="Mark one mailbox item as read.",
    )
    def post(self, request, item_id):
        item = get_object_or_404(MailboxItem, id=item_id, recipient=request.user)
        mark_mailbox_item_read(item)
        return success_response(data=MailboxItemSerializer(item).data)


class MailboxItemReadAllView(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="Mark All Mailbox Items Read",
        responses={200: OpenApiTypes.OBJECT},
        description="Mark all mailbox items as read for the authenticated user.",
    )
    def post(self, request):
        updated_count = mark_all_mailbox_items_read(request.user)
        return success_response(data={"updated": updated_count})
