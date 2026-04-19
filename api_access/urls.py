from django.urls import path

from api_access import views

urlpatterns = [
    path(
        "v1/access/resource-groups/lookup/",
        views.PermissionResourceGroupLookup.as_view(),
    ),
    path("v1/access/instances/lookup/", views.PermissionInstanceLookup.as_view()),
    path("v1/access/request/", views.PermissionRequestListCreate.as_view()),
    path(
        "v1/access/request/<int:request_id>/", views.PermissionRequestDetail.as_view()
    ),
    path(
        "v1/access/request/<int:request_id>/reviews/",
        views.PermissionRequestReviewCreate.as_view(),
    ),
    path("v1/access/grant/", views.ActiveGrantList.as_view()),
    path(
        "v1/access/grant/<str:grant_type>/<int:grant_id>/",
        views.ActiveGrantDetail.as_view(),
    ),
]
