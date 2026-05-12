from django.urls import path

from test_api_extensions.views import TestExtensionPingView

urlpatterns = [
    path("extensions/ping/", TestExtensionPingView.as_view()),
]
