from django.urls import path

from api_users import views

urlpatterns = [
    path("v1/user/", views.UserList.as_view()),
    path("v1/me/", views.CurrentUser.as_view()),
    path("v1/user/<int:pk>/", views.UserDetail.as_view()),
    path("v1/permission-levels/", views.PermissionLevelList.as_view()),
    path(
        "v1/permission-levels/available-permissions/",
        views.AvailableTeamPermissions.as_view(),
    ),
    path(
        "v1/permission-levels/<int:pk>/",
        views.PermissionLevelDetail.as_view(),
    ),
    path(
        "v1/teams/users/lookup/",
        views.TeamUserLookup.as_view(),
    ),
    path(
        "v1/teams/nodes/lookup/",
        views.TeamNodeLookup.as_view(),
    ),
    path("v1/teams/services/lookup/", views.TeamServiceLookup.as_view()),
    path("v1/teams/", views.TeamList.as_view()),
    path("v1/teams/<int:pk>/", views.TeamDetail.as_view()),
]
