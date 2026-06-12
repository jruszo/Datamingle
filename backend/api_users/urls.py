from django.urls import path

from api_users import views

urlpatterns = [
    path("v1/user/", views.UserList.as_view()),
    path("v1/user/invitations/", views.WorkOSUserInvitation.as_view()),
    path("v1/me/", views.CurrentUser.as_view()),
    path("v1/user/<int:pk>/", views.UserDetail.as_view()),
    path("v1/teams/permission-groups/", views.PermissionGroupCatalog.as_view()),
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
