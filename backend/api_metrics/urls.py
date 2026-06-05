from django.urls import path

from api_metrics import views

urlpatterns = [
    path("v1/metrics/names", views.MetricsNamesView.as_view(), name="metrics-names"),
    path("v1/metrics/labels", views.MetricsLabelsView.as_view(), name="metrics-labels"),
    path(
        "v1/metrics/label/<str:label_name>/values",
        views.MetricsLabelValuesView.as_view(),
        name="metrics-label-values",
    ),
    path("v1/metrics/series", views.MetricsSeriesView.as_view(), name="metrics-series"),
    path(
        "v1/metrics/metadata",
        views.MetricsMetadataView.as_view(),
        name="metrics-metadata",
    ),
    path("v1/metrics/query", views.MetricsQueryView.as_view(), name="metrics-query"),
    path(
        "v1/metrics/query_range",
        views.MetricsQueryRangeView.as_view(),
        name="metrics-query-range",
    ),
    path(
        "v1/metrics/format_query",
        views.MetricsFormatQueryView.as_view(),
        name="metrics-format-query",
    ),
    path(
        "v1/metrics/parse_query",
        views.MetricsParseQueryView.as_view(),
        name="metrics-parse-query",
    ),
]
