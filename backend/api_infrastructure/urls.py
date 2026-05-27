from django.urls import path

from api_infrastructure import views

urlpatterns = [
    path(
        "v1/infrastructure/nodes/",
        views.InfrastructureNodeListCreateView.as_view(),
        name="infrastructure-node-list",
    ),
    path(
        "v1/infrastructure/nodes/<int:node_id>/",
        views.InfrastructureNodeDetailView.as_view(),
        name="infrastructure-node-detail",
    ),
    path(
        "v1/infrastructure/nodes/<int:node_id>/discover/",
        views.InfrastructureNodeDiscoverView.as_view(),
        name="infrastructure-node-discover",
    ),
    path(
        "v1/infrastructure/nodes/<int:node_id>/remote-manager/",
        views.InfrastructureNodeRemoteManagerView.as_view(),
        name="infrastructure-node-remote-manager",
    ),
    path(
        "v1/infrastructure/services/",
        views.InfrastructureServiceCreateView.as_view(),
        name="infrastructure-service-create",
    ),
    path(
        "v1/infrastructure/services/<int:service_id>/",
        views.InfrastructureServiceDetailView.as_view(),
        name="infrastructure-service-detail",
    ),
    path(
        "v1/infrastructure/services/<int:service_id>/test/",
        views.InfrastructureServiceConnectionTestView.as_view(),
        name="infrastructure-service-test",
    ),
    path(
        "v1/infrastructure/recommendations/<int:recommendation_id>/",
        views.ServiceRecommendationDetailView.as_view(),
        name="infrastructure-recommendation-detail",
    ),
]
