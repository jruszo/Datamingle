from django.urls import path

from api_admin import views

urlpatterns = [
    path("v1/dashboard/", views.DashboardOverview.as_view()),
    path("v1/system-settings/", views.SystemSettingsView.as_view()),
    path(
        "v1/system-settings/tests/go-inception/",
        views.SystemSettingsGoInceptionTestView.as_view(),
    ),
    path(
        "v1/system-settings/tests/email/",
        views.SystemSettingsEmailTestView.as_view(),
    ),
    path(
        "v1/system-settings/tests/storage/",
        views.SystemSettingsStorageTestView.as_view(),
    ),
]
