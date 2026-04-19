from django.test import override_settings
from rest_framework import status
from rest_framework.test import APITestCase

from api_core.legacy_tests import InfoTest


@override_settings(
    INSTALLED_APPS=(
        "django.contrib.admin",
        "django.contrib.auth",
        "django.contrib.contenttypes",
        "django.contrib.sessions",
        "django.contrib.messages",
        "django.contrib.staticfiles",
        "django_q",
        "sql",
        "sql_api",
        "api_core",
        "api_auth",
        "api_users",
        "api_instances",
        "api_workflows",
        "api_archives",
        "api_queries",
        "api_access",
        "api_admin",
        "test_api_extensions",
        "common",
        "rest_framework",
        "django_filters",
        "drf_spectacular",
    ),
    DATAMINGLE_API_EXTENSION_APPS=["test_api_extensions"],
)
class ApiExtensionRoutingTests(APITestCase):
    def test_extension_routes_are_loaded_from_settings(self):
        response = self.client.get("/api/extensions/ping/")
        self.assertEqual(response.status_code, status.HTTP_302_FOUND)
        self.assertIn("/login", response.url)


class ApiGatewayDocsTests(APITestCase):
    def test_schema_route_resolves(self):
        response = self.client.get("/api/schema/")
        self.assertEqual(response.status_code, status.HTTP_302_FOUND)
        self.assertIn("/login", response.url)
