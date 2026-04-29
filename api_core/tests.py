from django.contrib.auth import get_user_model
from django.urls import clear_url_caches, reverse
from django.test import override_settings
from rest_framework import status
from rest_framework.test import APIRequestFactory, APITestCase, force_authenticate

from api_core.extensions import get_extension_urlpatterns
from api_core.legacy_tests import InfoTest
import sql_api.urls as sql_api_urls

User = get_user_model()


@override_settings(
    INSTALLED_APPS=(
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
        user = User.objects.create(username="extension_test_user")
        original_urlpatterns = list(sql_api_urls.urlpatterns)
        try:
            sql_api_urls.urlpatterns = (
                original_urlpatterns + get_extension_urlpatterns()
            )
            clear_url_caches()
            extension_route = next(
                (
                    pattern
                    for pattern in sql_api_urls.urlpatterns
                    if getattr(pattern.pattern, "_route", None) == "extensions/ping/"
                ),
                None,
            )
            self.assertIsNotNone(extension_route)

            request = APIRequestFactory().get("/api/extensions/ping/")
            force_authenticate(request, user=user)
            response = extension_route.callback(request)
        finally:
            sql_api_urls.urlpatterns = original_urlpatterns
            clear_url_caches()

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data, {"detail": "ok"})


class ApiGatewayDocsTests(APITestCase):
    def test_schema_route_resolves(self):
        user = User.objects.create(username="schema_test_user")
        self.client.force_login(user)

        schema_url = reverse("sql_api:schema")
        self.assertEqual(schema_url, "/api/schema/")

        response = self.client.get(schema_url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn("application/vnd.oai.openapi", response.headers["Content-Type"])
