import json
import tempfile
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import patch

import requests
from PIL import Image
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import override_settings
from rest_framework import status
from rest_framework.test import APITestCase

from api_metrics.models import (
    MetricsDashboard,
    MetricsDashboardFavorite,
    MetricsDashboardRevision,
)
from sql.models import Users


class MetricsProxyTests(APITestCase):
    def setUp(self):
        self.user = Users.objects.create_user(
            username="metrics-user",
            email="metrics@example.com",
            is_active=True,
        )
        self.client.force_authenticate(
            user=self.user,
            token={
                "org_id": "org_test_123",
                "sub": "user_test_123",
            },
        )

    @override_settings(
        DATAMINGLE_CORTEX_URL="http://cortex.test",
        DATAMINGLE_METRICS_PROXY_TIMEOUT_SECONDS=7,
    )
    @patch("api_metrics.views.requests.request")
    def test_labels_proxy_injects_authenticated_org_scope(self, mock_request):
        mock_request.return_value = SimpleNamespace(
            status_code=200,
            json=lambda: {"status": "success", "data": ["__name__", "job"]},
            text="",
        )

        response = self.client.get("/api/v1/metrics/labels?org_id=org_from_frontend")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["data"], ["__name__", "job"])
        mock_request.assert_called_once()
        _, url = mock_request.call_args.args
        self.assertEqual(url, "http://cortex.test/prometheus/api/v1/labels")
        self.assertEqual(
            mock_request.call_args.kwargs["headers"]["X-Scope-OrgID"],
            "org_test_123",
        )
        self.assertNotIn(
            ("org_id", "org_from_frontend"),
            mock_request.call_args.kwargs["params"],
        )
        self.assertEqual(mock_request.call_args.kwargs["timeout"], 7)

    @override_settings(DATAMINGLE_CORTEX_URL="http://cortex.test")
    @patch("api_metrics.views.requests.request")
    def test_metric_names_are_filtered_and_limited_server_side(self, mock_request):
        mock_request.return_value = SimpleNamespace(
            status_code=200,
            json=lambda: {
                "status": "success",
                "data": [
                    "mysql_global_status_threads_connected",
                    "node_disk_cpu_wait_seconds",
                    "node_cpu_seconds_total",
                    "node_memory_MemAvailable_bytes",
                    "node_network_receive_bytes_total",
                ],
            },
            text="",
        )

        response = self.client.get(
            "/api/v1/metrics/names",
            {
                "search": "node_",
                "limit": "2",
            },
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            response.data["data"],
            ["node_cpu_seconds_total", "node_disk_cpu_wait_seconds"],
        )
        _, url = mock_request.call_args.args
        self.assertEqual(
            url,
            "http://cortex.test/prometheus/api/v1/label/__name__/values",
        )
        self.assertNotIn(("search", "node_"), mock_request.call_args.kwargs["params"])
        self.assertNotIn(("limit", "2"), mock_request.call_args.kwargs["params"])

    @override_settings(DATAMINGLE_CORTEX_URL="http://cortex.test")
    @patch("api_metrics.views.requests.request")
    def test_metric_name_search_is_case_insensitive_and_prioritizes_prefixes(
        self, mock_request
    ):
        mock_request.return_value = SimpleNamespace(
            status_code=200,
            json=lambda: {
                "status": "success",
                "data": [
                    "node_cpu_seconds_total",
                    "process_cpu_seconds_total",
                    "CPU",
                    "cpu_usage_percent",
                ],
            },
            text="",
        )

        response = self.client.get(
            "/api/v1/metrics/names",
            {"search": "cpu", "limit": "10"},
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            response.data["data"],
            [
                "CPU",
                "cpu_usage_percent",
                "node_cpu_seconds_total",
                "process_cpu_seconds_total",
            ],
        )

    @override_settings(DATAMINGLE_CORTEX_URL="http://cortex.test")
    @patch("api_metrics.views.requests.request")
    def test_query_range_post_is_proxied_to_cortex(self, mock_request):
        mock_request.return_value = SimpleNamespace(
            status_code=200,
            json=lambda: {
                "status": "success",
                "data": {"resultType": "matrix", "result": []},
            },
            text="",
        )

        response = self.client.post(
            "/api/v1/metrics/query_range",
            {
                "query": 'rate(http_requests_total{job="api"}[5m])',
                "start": "1780580000",
                "end": "1780583600",
                "step": "60s",
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        method, url = mock_request.call_args.args
        self.assertEqual(method, "POST")
        self.assertEqual(url, "http://cortex.test/prometheus/api/v1/query_range")
        self.assertIn(
            ("query", 'rate(http_requests_total{job="api"}[5m])'),
            mock_request.call_args.kwargs["data"],
        )

    @patch("api_metrics.views.requests.request")
    def test_stale_bucket_index_is_returned_as_retryable_service_error(
        self, mock_request
    ):
        mock_request.return_value = SimpleNamespace(
            status_code=500,
            json=lambda: {
                "status": "error",
                "errorType": "server_error",
                "error": (
                    "expanding series: bucket index is too old and the last time "
                    "it was updated exceeds the allowed max staleness"
                ),
            },
            text="",
        )

        response = self.client.get(
            "/api/v1/metrics/query_range",
            {
                "query": "up",
                "start": "1780760000",
                "end": "1780763600",
                "step": "60",
            },
        )

        self.assertEqual(response.status_code, status.HTTP_503_SERVICE_UNAVAILABLE)
        self.assertEqual(
            response.data["error"],
            "Metrics storage is refreshing after a restart. Retry the query shortly.",
        )

    @override_settings(
        DATAMINGLE_METRICS_MAX_RANGE_SECONDS=3600,
        DATAMINGLE_METRICS_MIN_STEP_SECONDS=15,
        DATAMINGLE_METRICS_MAX_RANGE_POINTS=1000,
    )
    @patch("api_metrics.views.requests.request")
    def test_query_range_rejects_oversized_ranges_before_cortex(self, mock_request):
        response = self.client.get(
            "/api/v1/metrics/query_range",
            {
                "query": "up",
                "start": "1780580000",
                "end": "1780587201",
                "step": "60s",
            },
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("end", response.data)
        mock_request.assert_not_called()

    @patch("api_metrics.views.requests.request")
    def test_query_range_rejects_too_many_points_before_cortex(self, mock_request):
        response = self.client.get(
            "/api/v1/metrics/query_range",
            {
                "query": "up",
                "start": "1780580000",
                "end": "1780590000",
                "step": "1s",
            },
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("step", response.data)
        mock_request.assert_not_called()

    @patch("api_metrics.views.requests.request")
    def test_invalid_label_name_is_rejected_before_cortex(self, mock_request):
        response = self.client.get("/api/v1/metrics/label/bad-label/values")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("label", response.data)
        mock_request.assert_not_called()


class MetricsDashboardTests(APITestCase):
    def setUp(self):
        self.media_directory = tempfile.TemporaryDirectory()
        self.media_override = override_settings(MEDIA_ROOT=self.media_directory.name)
        self.media_override.enable()
        self.addCleanup(self.media_override.disable)
        self.addCleanup(self.media_directory.cleanup)
        self.user = Users.objects.create_user(
            username="dashboard-user",
            email="dashboard@example.com",
            is_active=True,
        )
        self.other_user = Users.objects.create_user(
            username="dashboard-collaborator",
            email="collaborator@example.com",
            is_active=True,
        )
        self.authenticate(self.user, "org_test_123")

    def authenticate(self, user, organization_id):
        self.client.force_authenticate(
            user=user,
            token={
                "org_id": organization_id,
                "sub": f"user_{user.pk}",
            },
        )

    def panel(self, panel_id="94db74e1-9bd4-4f23-bac8-b36b66576520"):
        return {
            "id": panel_id,
            "title": "Request rate",
            "query": "rate(http_requests_total[5m])",
            "legend_labels": ["job", "instance"],
            "step_seconds": 60,
            "visualization": "line",
            "layout": {"x": 0, "y": 0, "w": 6, "h": 4},
        }

    def create_dashboard(self, **overrides):
        payload = {
            "name": "API overview",
            "description": "Shared service metrics",
            "time_range_seconds": 3600,
            "refresh_interval_seconds": 30,
            "panels": [self.panel()],
            **overrides,
        }
        return self.client.post("/api/v1/metrics/dashboards/", payload, format="json")

    def icon_file(self, name="icon.png", size=(640, 320), image_format="PNG"):
        content = BytesIO()
        Image.new("RGB", size, color=(37, 99, 235)).save(
            content,
            format=image_format,
        )
        return SimpleUploadedFile(
            name,
            content.getvalue(),
            content_type=f"image/{image_format.lower()}",
        )

    def test_create_and_list_dashboard(self):
        response = self.create_dashboard()

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertFalse(response.data["data"]["is_favorite"])
        self.assertEqual(response.data["data"]["revision"], 1)
        self.assertEqual(response.data["data"]["created_by"]["id"], self.user.id)
        panel = response.data["data"]["panels"][0]
        self.assertEqual(panel["visualization"]["type"], "time_series")
        self.assertEqual(panel["queries"][0]["ref_id"], "A")
        self.assertEqual(
            panel["queries"][0]["legend"],
            "{{job}} · {{instance}}",
        )
        self.assertEqual(response.data["data"]["variables"], [])

        list_response = self.client.get("/api/v1/metrics/dashboards/")
        self.assertEqual(list_response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(list_response.data["data"]), 1)
        snapshot = MetricsDashboardRevision.objects.get(
            dashboard_id=response.data["data"]["id"],
            revision=1,
        )
        self.assertEqual(snapshot.name, "API overview")
        self.assertEqual(snapshot.saved_by, self.user)

    def test_favorites_are_personal_and_favorites_sort_first(self):
        alpha = self.create_dashboard(name="Alpha").data["data"]
        zulu = self.create_dashboard(name="Zulu").data["data"]

        favorite_response = self.client.patch(
            f"/api/v1/metrics/dashboards/{zulu['id']}/favorite/",
            {"favorite": True},
            format="json",
        )
        list_response = self.client.get("/api/v1/metrics/dashboards/")

        self.assertEqual(favorite_response.status_code, status.HTTP_200_OK)
        self.assertTrue(favorite_response.data["data"]["is_favorite"])
        self.assertEqual(
            [item["id"] for item in list_response.data["data"]],
            [zulu["id"], alpha["id"]],
        )
        self.assertEqual(
            [item["is_favorite"] for item in list_response.data["data"]],
            [True, False],
        )

        self.authenticate(self.other_user, "org_test_123")
        collaborator_detail = self.client.get(
            f"/api/v1/metrics/dashboards/{zulu['id']}/"
        )
        self.assertEqual(collaborator_detail.status_code, status.HTTP_200_OK)
        self.assertFalse(collaborator_detail.data["data"]["is_favorite"])

    def test_favorite_filter_and_idempotent_toggle(self):
        favorite = self.create_dashboard(name="Favorite").data["data"]
        self.create_dashboard(name="Other")
        revisions_before = MetricsDashboardRevision.objects.count()

        for _ in range(2):
            response = self.client.patch(
                f"/api/v1/metrics/dashboards/{favorite['id']}/favorite/",
                {"favorite": True},
                format="json",
            )
            self.assertEqual(response.status_code, status.HTTP_200_OK)

        filtered_response = self.client.get("/api/v1/metrics/dashboards/?favorite=true")
        dashboard = MetricsDashboard.objects.get(pk=favorite["id"])
        self.assertEqual(
            [item["id"] for item in filtered_response.data["data"]],
            [favorite["id"]],
        )
        self.assertEqual(dashboard.revision, 1)
        self.assertEqual(MetricsDashboardRevision.objects.count(), revisions_before)
        self.assertEqual(MetricsDashboardFavorite.objects.count(), 1)

        for _ in range(2):
            response = self.client.patch(
                f"/api/v1/metrics/dashboards/{favorite['id']}/favorite/",
                {"favorite": False},
                format="json",
            )
            self.assertEqual(response.status_code, status.HTTP_200_OK)
            self.assertFalse(response.data["data"]["is_favorite"])
        self.assertFalse(MetricsDashboardFavorite.objects.exists())

    def test_favorite_is_tenant_scoped_and_deleted_with_dashboard(self):
        dashboard_id = self.create_dashboard().data["data"]["id"]
        self.client.patch(
            f"/api/v1/metrics/dashboards/{dashboard_id}/favorite/",
            {"favorite": True},
            format="json",
        )
        self.authenticate(self.other_user, "org_other")

        hidden_response = self.client.patch(
            f"/api/v1/metrics/dashboards/{dashboard_id}/favorite/",
            {"favorite": True},
            format="json",
        )
        self.assertEqual(hidden_response.status_code, status.HTTP_404_NOT_FOUND)

        self.authenticate(self.user, "org_test_123")
        delete_response = self.client.delete(
            f"/api/v1/metrics/dashboards/{dashboard_id}/"
        )
        self.assertEqual(delete_response.status_code, status.HTTP_200_OK)
        self.assertFalse(MetricsDashboardFavorite.objects.exists())

    def test_favorite_requires_boolean_value(self):
        dashboard_id = self.create_dashboard().data["data"]["id"]

        response = self.client.patch(
            f"/api/v1/metrics/dashboards/{dashboard_id}/favorite/",
            {"favorite": "true"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("favorite", response.data)

    def test_org_collaborator_can_update_dashboard(self):
        dashboard_id = self.create_dashboard().data["data"]["id"]
        self.authenticate(self.other_user, "org_test_123")

        response = self.client.patch(
            f"/api/v1/metrics/dashboards/{dashboard_id}/",
            {
                "expected_revision": 1,
                "name": "Collaborative API overview",
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["data"]["revision"], 2)
        self.assertEqual(response.data["data"]["name"], "Collaborative API overview")
        snapshot = MetricsDashboardRevision.objects.get(
            dashboard_id=dashboard_id,
            revision=2,
        )
        self.assertEqual(snapshot.saved_by, self.other_user)
        self.assertEqual(snapshot.name, "Collaborative API overview")

    def test_custom_relative_time_range_is_persisted(self):
        response = self.create_dashboard(time_range_seconds=3 * 60 * 60)

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data["data"]["time_range_mode"], "relative")
        self.assertEqual(response.data["data"]["time_range_seconds"], 10800)
        self.assertEqual(response.data["data"]["time_range_start"], "")
        self.assertEqual(response.data["data"]["time_range_end"], "")

    def test_dashboard_icon_is_normalized_and_served_to_tenant(self):
        dashboard_id = self.create_dashboard().data["data"]["id"]

        upload_response = self.client.post(
            f"/api/v1/metrics/dashboards/{dashboard_id}/icon/",
            {"icon": self.icon_file()},
            format="multipart",
        )
        icon_response = self.client.get(
            f"/api/v1/metrics/dashboards/{dashboard_id}/icon/"
        )

        self.assertEqual(upload_response.status_code, status.HTTP_200_OK)
        self.assertTrue(upload_response.data["data"]["has_icon"])
        self.assertEqual(icon_response.status_code, status.HTTP_200_OK)
        self.assertEqual(icon_response["Content-Type"], "image/png")
        normalized = Image.open(BytesIO(b"".join(icon_response.streaming_content)))
        self.assertEqual(normalized.format, "PNG")
        self.assertLessEqual(normalized.width, 256)
        self.assertLessEqual(normalized.height, 256)

    def test_dashboard_icon_is_hidden_from_other_organization(self):
        dashboard_id = self.create_dashboard().data["data"]["id"]
        self.client.post(
            f"/api/v1/metrics/dashboards/{dashboard_id}/icon/",
            {"icon": self.icon_file()},
            format="multipart",
        )
        self.authenticate(self.other_user, "org_other")

        response = self.client.get(f"/api/v1/metrics/dashboards/{dashboard_id}/icon/")

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_invalid_dashboard_icon_is_rejected(self):
        dashboard_id = self.create_dashboard().data["data"]["id"]

        response = self.client.post(
            f"/api/v1/metrics/dashboards/{dashboard_id}/icon/",
            {
                "icon": SimpleUploadedFile(
                    "icon.svg",
                    b"<svg xmlns='http://www.w3.org/2000/svg'></svg>",
                    content_type="image/svg+xml",
                )
            },
            format="multipart",
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("icon", response.data)

    def test_dashboard_icon_can_be_replaced_and_removed(self):
        dashboard_id = self.create_dashboard().data["data"]["id"]
        first_response = self.client.post(
            f"/api/v1/metrics/dashboards/{dashboard_id}/icon/",
            {"icon": self.icon_file()},
            format="multipart",
        )
        first_name = MetricsDashboard.objects.get(pk=dashboard_id).icon.name
        second_response = self.client.post(
            f"/api/v1/metrics/dashboards/{dashboard_id}/icon/",
            {"icon": self.icon_file(name="replacement.jpg", image_format="JPEG")},
            format="multipart",
        )
        dashboard = MetricsDashboard.objects.get(pk=dashboard_id)

        self.assertEqual(first_response.status_code, status.HTTP_200_OK)
        self.assertEqual(second_response.status_code, status.HTTP_200_OK)
        self.assertNotEqual(dashboard.icon.name, first_name)
        self.assertFalse(dashboard.icon.storage.exists(first_name))

        remove_response = self.client.delete(
            f"/api/v1/metrics/dashboards/{dashboard_id}/icon/"
        )

        self.assertEqual(remove_response.status_code, status.HTTP_200_OK)
        self.assertFalse(remove_response.data["data"]["has_icon"])
        self.assertFalse(MetricsDashboard.objects.get(pk=dashboard_id).icon)

    def test_absolute_time_range_is_persisted_and_restored(self):
        dashboard = self.create_dashboard(
            time_range_mode="absolute",
            time_range_start="2026-06-06T08:15:00.000Z",
            time_range_end="2026-06-06T10:45:00.000Z",
        ).data["data"]
        update_response = self.client.patch(
            f"/api/v1/metrics/dashboards/{dashboard['id']}/",
            {
                "expected_revision": 1,
                "time_range_mode": "relative",
                "time_range_seconds": 7200,
            },
            format="json",
        )
        self.assertEqual(update_response.status_code, status.HTTP_200_OK)

        restore_response = self.client.post(
            f"/api/v1/metrics/dashboards/{dashboard['id']}/revisions/1/restore/",
            {"expected_revision": 2},
            format="json",
        )

        self.assertEqual(restore_response.status_code, status.HTTP_200_OK)
        restored = restore_response.data["data"]
        self.assertEqual(restored["time_range_mode"], "absolute")
        self.assertEqual(restored["time_range_start"], "2026-06-06T08:15:00.000Z")
        self.assertEqual(restored["time_range_end"], "2026-06-06T10:45:00.000Z")

    def test_invalid_absolute_time_ranges_are_rejected(self):
        missing_end = self.create_dashboard(
            time_range_mode="absolute",
            time_range_start="2026-06-06T08:15:00.000Z",
        )
        reversed_range = self.create_dashboard(
            time_range_mode="absolute",
            time_range_start="2026-06-06T10:45:00.000Z",
            time_range_end="2026-06-06T08:15:00.000Z",
        )

        self.assertEqual(missing_end.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(reversed_range.status_code, status.HTTP_400_BAD_REQUEST)

    def test_relative_time_range_is_bounded(self):
        too_short = self.create_dashboard(time_range_seconds=30)
        too_long = self.create_dashboard(time_range_seconds=31 * 24 * 60 * 60)

        self.assertEqual(too_short.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(too_long.status_code, status.HTTP_400_BAD_REQUEST)

    def test_first_save_of_legacy_dashboard_captures_previous_state(self):
        dashboard_id = self.create_dashboard().data["data"]["id"]
        dashboard = MetricsDashboard.objects.get(pk=dashboard_id)
        previous_update_time = dashboard.update_time
        dashboard.history.all().delete()

        response = self.client.patch(
            f"/api/v1/metrics/dashboards/{dashboard_id}/",
            {
                "expected_revision": 1,
                "description": "First save after history deployment",
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        baseline = MetricsDashboardRevision.objects.get(
            dashboard_id=dashboard_id,
            revision=1,
        )
        self.assertIsNone(baseline.saved_by)
        self.assertEqual(baseline.saved_at, previous_update_time)
        self.assertEqual(baseline.description, "Shared service metrics")
        self.assertTrue(
            MetricsDashboardRevision.objects.filter(
                dashboard_id=dashboard_id,
                revision=2,
                saved_by=self.user,
            ).exists()
        )

    def test_revision_history_can_be_listed_and_inspected(self):
        dashboard_id = self.create_dashboard().data["data"]["id"]
        self.client.patch(
            f"/api/v1/metrics/dashboards/{dashboard_id}/",
            {
                "expected_revision": 1,
                "name": "Updated overview",
            },
            format="json",
        )

        list_response = self.client.get(
            f"/api/v1/metrics/dashboards/{dashboard_id}/revisions/"
        )
        detail_response = self.client.get(
            f"/api/v1/metrics/dashboards/{dashboard_id}/revisions/1/"
        )

        self.assertEqual(list_response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            [item["revision"] for item in list_response.data["data"]],
            [2, 1],
        )
        self.assertEqual(detail_response.status_code, status.HTTP_200_OK)
        self.assertEqual(detail_response.data["data"]["name"], "API overview")
        self.assertEqual(len(detail_response.data["data"]["panels"]), 1)

    def test_revision_history_is_hidden_from_other_organization(self):
        dashboard_id = self.create_dashboard().data["data"]["id"]
        self.authenticate(self.other_user, "org_other")

        list_response = self.client.get(
            f"/api/v1/metrics/dashboards/{dashboard_id}/revisions/"
        )
        detail_response = self.client.get(
            f"/api/v1/metrics/dashboards/{dashboard_id}/revisions/1/"
        )

        self.assertEqual(list_response.status_code, status.HTTP_404_NOT_FOUND)
        self.assertEqual(detail_response.status_code, status.HTTP_404_NOT_FOUND)

    def test_restore_creates_a_new_revision_and_preserves_history(self):
        dashboard_id = self.create_dashboard().data["data"]["id"]
        self.client.patch(
            f"/api/v1/metrics/dashboards/{dashboard_id}/",
            {
                "expected_revision": 1,
                "name": "Updated overview",
                "panels": [],
            },
            format="json",
        )

        response = self.client.post(
            f"/api/v1/metrics/dashboards/{dashboard_id}/revisions/1/restore/",
            {"expected_revision": 2},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["data"]["revision"], 3)
        self.assertEqual(response.data["data"]["name"], "API overview")
        self.assertEqual(len(response.data["data"]["panels"]), 1)
        self.assertEqual(
            list(
                MetricsDashboardRevision.objects.filter(dashboard_id=dashboard_id)
                .order_by("revision")
                .values_list("revision", flat=True)
            ),
            [1, 2, 3],
        )
        restored = MetricsDashboardRevision.objects.get(
            dashboard_id=dashboard_id,
            revision=3,
        )
        self.assertEqual(restored.restored_from_revision, 1)
        self.assertEqual(restored.saved_by, self.user)

    def test_stale_restore_returns_latest_dashboard(self):
        dashboard_id = self.create_dashboard().data["data"]["id"]
        self.client.patch(
            f"/api/v1/metrics/dashboards/{dashboard_id}/",
            {
                "expected_revision": 1,
                "description": "Updated",
            },
            format="json",
        )

        response = self.client.post(
            f"/api/v1/metrics/dashboards/{dashboard_id}/revisions/1/restore/",
            {"expected_revision": 1},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_409_CONFLICT)
        self.assertEqual(response.data["data"]["revision"], 2)

    def test_revision_history_retains_latest_fifty_snapshots(self):
        dashboard_id = self.create_dashboard().data["data"]["id"]
        for revision in range(1, 52):
            response = self.client.patch(
                f"/api/v1/metrics/dashboards/{dashboard_id}/",
                {
                    "expected_revision": revision,
                    "description": f"Revision {revision + 1}",
                },
                format="json",
            )
            self.assertEqual(response.status_code, status.HTTP_200_OK)

        revisions = MetricsDashboardRevision.objects.filter(dashboard_id=dashboard_id)
        self.assertEqual(revisions.count(), 50)
        self.assertEqual(
            revisions.order_by("revision").first().revision,
            3,
        )

    def test_stale_update_returns_latest_dashboard(self):
        dashboard_id = self.create_dashboard().data["data"]["id"]
        first_update = self.client.patch(
            f"/api/v1/metrics/dashboards/{dashboard_id}/",
            {
                "expected_revision": 1,
                "description": "Updated first",
            },
            format="json",
        )
        self.assertEqual(first_update.status_code, status.HTTP_200_OK)

        stale_update = self.client.patch(
            f"/api/v1/metrics/dashboards/{dashboard_id}/",
            {
                "expected_revision": 1,
                "description": "Stale local draft",
            },
            format="json",
        )

        self.assertEqual(stale_update.status_code, status.HTTP_409_CONFLICT)
        self.assertEqual(stale_update.data["data"]["revision"], 2)
        self.assertEqual(stale_update.data["data"]["description"], "Updated first")

    def test_dashboard_is_hidden_from_other_organization(self):
        dashboard_id = self.create_dashboard().data["data"]["id"]
        self.authenticate(self.other_user, "org_other")

        detail_response = self.client.get(f"/api/v1/metrics/dashboards/{dashboard_id}/")
        list_response = self.client.get("/api/v1/metrics/dashboards/")

        self.assertEqual(detail_response.status_code, status.HTTP_404_NOT_FOUND)
        self.assertEqual(list_response.status_code, status.HTTP_200_OK)
        self.assertEqual(list_response.data["data"], [])

    def test_invalid_panel_layout_is_rejected(self):
        invalid_panel = self.panel()
        invalid_panel["layout"] = {"x": 10, "y": 0, "w": 4, "h": 4}

        response = self.create_dashboard(panels=[invalid_panel])

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("panels", response.data)

    def test_duplicate_panel_ids_are_rejected(self):
        panel = self.panel()

        response = self.create_dashboard(panels=[panel, panel])

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("panels", response.data)

    def test_multi_query_panel_and_variables_are_persisted(self):
        panel = self.panel()
        panel.pop("query")
        panel.pop("legend_labels")
        panel["queries"] = [
            {
                "ref_id": "A",
                "query": "rate(http_requests_total[5m])",
                "editor_mode": "builder",
                "disabled": False,
                "legend": "{{job}}",
            },
            {
                "ref_id": "B",
                "query": "rate(http_errors_total[5m])",
                "editor_mode": "code",
                "disabled": False,
                "legend": "{{job}} errors",
            },
        ]
        panel["visualization"] = {
            "type": "stat",
            "unit": "ops/s",
            "thresholds": [{"value": 10, "color": "#dc2626"}],
        }
        variables = [
            {
                "name": "instance",
                "label": "Instance",
                "metric": "up",
                "label_name": "instance",
                "multi": True,
                "include_all": True,
            }
        ]

        response = self.create_dashboard(panels=[panel], variables=variables)

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(len(response.data["data"]["panels"][0]["queries"]), 2)
        self.assertEqual(
            response.data["data"]["panels"][0]["visualization"]["type"],
            "stat",
        )
        self.assertEqual(response.data["data"]["variables"], variables)

    def test_duplicate_query_refs_are_rejected(self):
        panel = self.panel()
        panel.pop("query")
        panel.pop("legend_labels")
        query = {
            "ref_id": "A",
            "query": "up",
            "editor_mode": "code",
            "disabled": False,
            "legend": "",
        }
        panel["queries"] = [query, query]

        response = self.create_dashboard(panels=[panel])

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("panels", response.data)

    def test_dashboard_can_be_copied_after_conflict(self):
        original = self.create_dashboard().data["data"]
        copy_response = self.create_dashboard(
            name="API overview (copy)",
            panels=original["panels"],
        )

        self.assertEqual(copy_response.status_code, status.HTTP_201_CREATED)
        self.assertNotEqual(copy_response.data["data"]["id"], original["id"])
        self.assertEqual(MetricsDashboard.objects.count(), 2)

    def test_org_collaborator_can_delete_dashboard(self):
        dashboard_id = self.create_dashboard().data["data"]["id"]
        self.authenticate(self.other_user, "org_test_123")

        response = self.client.delete(f"/api/v1/metrics/dashboards/{dashboard_id}/")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertFalse(MetricsDashboard.objects.filter(pk=dashboard_id).exists())
        self.assertFalse(
            MetricsDashboardRevision.objects.filter(dashboard_id=dashboard_id).exists()
        )


class MetricsAIAssistantTests(APITestCase):
    def setUp(self):
        user = Users.objects.create_user(
            username="metrics-ai-user",
            email="metrics-ai@example.com",
            is_active=True,
        )
        self.client.force_authenticate(
            user=user,
            token={"org_id": "org_test_123", "sub": "metrics_ai_user"},
        )

    @patch.dict("os.environ", {"OPENAI_KEY": ""}, clear=False)
    def test_availability_is_false_without_api_key(self):
        response = self.client.get("/api/v1/metrics/ai/availability")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["data"], {"available": False})

    @patch.dict("os.environ", {"OPENAI_KEY": ""}, clear=False)
    def test_assistant_returns_service_unavailable_without_api_key(self):
        response = self.client.post(
            "/api/v1/metrics/ai/assist",
            {
                "prompt": "Show request rate",
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_503_SERVICE_UNAVAILABLE)
        self.assertEqual(response.data["data"], {"available": False})

    @patch.dict("os.environ", {"OPENAI_KEY": "configured"}, clear=False)
    @patch("api_metrics.views.requests.get")
    def test_assistant_returns_service_unavailable_when_context_fails(self, mock_get):
        mock_get.side_effect = requests.RequestException("Cortex unavailable")

        response = self.client.post(
            "/api/v1/metrics/ai/assist",
            {
                "prompt": "Show request rate",
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_503_SERVICE_UNAVAILABLE)
        self.assertEqual(
            response.data["detail"], "Metrics context is temporarily unavailable."
        )

    @patch.dict(
        "os.environ",
        {
            "OPENAI_KEY": "configured",
            "OPENAI_BASE_URL": "https://ai.test",
            "OPENAI_MODEL": "test-model",
        },
        clear=False,
    )
    @patch("openai.OpenAI")
    @patch("api_metrics.views.requests.get")
    def test_assistant_builds_tenant_context_server_side(self, mock_get, openai_class):
        mock_get.side_effect = [
            SimpleNamespace(
                raise_for_status=lambda: None,
                json=lambda: {
                    "status": "success",
                    "data": [
                        "http_requests_total",
                        "node_memory_bytes",
                        "node_cpu_seconds_total",
                    ],
                },
            ),
            SimpleNamespace(
                raise_for_status=lambda: None,
                json=lambda: {
                    "status": "success",
                    "data": ["__name__", "instance", "job"],
                },
            ),
            SimpleNamespace(
                raise_for_status=lambda: None,
                json=lambda: {
                    "status": "success",
                    "data": {
                        "node_cpu_seconds_total": [
                            {
                                "type": "counter",
                                "help": "Seconds the CPUs spent in each mode.",
                                "unit": "seconds",
                            }
                        ]
                    },
                },
            ),
        ]
        completion = SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(
                        content=json.dumps(
                            {
                                "query": "rate(node_cpu_seconds_total[5m])",
                                "explanation": "CPU rate",
                                "assumptions": [],
                                "warnings": [],
                            }
                        )
                    )
                )
            ]
        )
        openai_class.return_value.chat.completions.create.return_value = completion

        response = self.client.post(
            "/api/v1/metrics/ai/assist",
            {
                "prompt": "Show CPU usage",
                "metric_names": ["client_injected_metric"],
                "label_names": ["client_injected_label"],
                "current_query": "client_injected_query",
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        create_call = openai_class.return_value.chat.completions.create.call_args
        user_message = create_call.kwargs["messages"][1]["content"]
        self.assertIn("node_cpu_seconds_total", user_message)
        self.assertIn('"instance"', user_message)
        self.assertNotIn("client_injected_metric", user_message)
        self.assertNotIn("client_injected_label", user_message)
        self.assertNotIn("client_injected_query", user_message)
        self.assertEqual(
            mock_get.call_args_list[0].kwargs["headers"]["X-Scope-OrgID"],
            "org_test_123",
        )
