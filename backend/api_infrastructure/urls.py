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
        "v1/infrastructure/nodes/<int:node_id>/remote-manager/",
        views.InfrastructureNodeRemoteManagerView.as_view(),
        name="infrastructure-node-remote-manager",
    ),
]
