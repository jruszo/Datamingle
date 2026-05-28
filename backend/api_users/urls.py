from django.urls import path

from api_users import views

urlpatterns = [
    path("v1/user/", views.UserList.as_view()),
    path("v1/user/invitations/", views.WorkOSUserInvitation.as_view()),
    path("v1/me/", views.CurrentUser.as_view()),
    path("v1/user/<int:pk>/", views.UserDetail.as_view()),
    path("v1/user/access-roles/", views.AccessRoleCatalog.as_view()),
    path(
        "v1/user/resourcegroup/users/lookup/",
        views.ResourceGroupUserLookup.as_view(),
    ),
    path(
        "v1/user/resourcegroup/instances/lookup/",
        views.ResourceGroupInstanceLookup.as_view(),
    ),
    path("v1/user/resourcegroup/", views.ResourceGroupList.as_view()),
    path("v1/user/resourcegroup/<int:pk>/", views.ResourceGroupDetail.as_view()),
]
