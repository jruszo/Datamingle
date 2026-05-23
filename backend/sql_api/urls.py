from django.urls import include, path
from rest_framework import routers
from drf_spectacular.views import (
    SpectacularAPIView,
    SpectacularRedocView,
    SpectacularSwaggerView,
)

from api_core.extensions import get_extension_urlpatterns
from api_core import views as core_views

router = routers.DefaultRouter()

urlpatterns = [
    path("v1/", include(router.urls)),
    path("", include("api_auth.urls")),
    path("", include("api_admin.urls")),
    path("", include("api_users.urls")),
    path("", include("api_instances.urls")),
    path("", include("api_workflows.urls")),
    path("", include("api_archives.urls")),
    path("", include("api_queries.urls")),
    path("", include("api_audit.urls")),
    path("", include("api_access.urls")),
    path("", include("api_mailbox.urls")),
    path("", include("api_agents.urls")),
    path("", include("api_infrastructure.urls")),
    path("schema/", SpectacularAPIView.as_view(), name="schema"),
    path(
        "swagger/",
        SpectacularSwaggerView.as_view(url_name="sql_api:schema"),
        name="swagger",
    ),
    path(
        "redoc/", SpectacularRedocView.as_view(url_name="sql_api:schema"), name="redoc"
    ),
    path("info", core_views.info),
    path("debug", core_views.debug),
]

urlpatterns += get_extension_urlpatterns()
