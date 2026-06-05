from types import SimpleNamespace
from unittest.mock import patch

from django.test import override_settings
from rest_framework import status
from rest_framework.test import APITestCase

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
            ["node_cpu_seconds_total", "node_memory_MemAvailable_bytes"],
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
