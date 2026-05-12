from django.urls import path

from api_mailbox import views

urlpatterns = [
    path("v1/mailbox/summary/", views.MailboxSummaryView.as_view()),
    path("v1/mailbox/items/", views.MailboxItemListView.as_view()),
    path("v1/mailbox/items/read-all/", views.MailboxItemReadAllView.as_view()),
    path("v1/mailbox/items/<int:item_id>/read/", views.MailboxItemReadView.as_view()),
]
