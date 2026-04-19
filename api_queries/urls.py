from django.urls import path

from api_queries import views

urlpatterns = [
    path("v1/query/instance/", views.QueryInstanceList.as_view()),
    path("v1/query/describe/", views.QueryDescribe.as_view()),
    path("v1/query/", views.QueryExecute.as_view()),
    path("v1/query/log/", views.QueryLogList.as_view()),
    path("v1/query/log/audit/", views.QueryLogAuditList.as_view()),
    path("v1/query/favorite/", views.QueryFavorite.as_view()),
    path(
        "v1/query/privilege/apply/",
        views.QueryPrivilegesApplyListCreate.as_view(),
    ),
    path("v1/query/privilege/", views.QueryPrivilegesList.as_view()),
    path(
        "v1/query/privilege/<int:privilege_id>/",
        views.QueryPrivilegeDetail.as_view(),
    ),
    path(
        "v1/query/privilege/apply/<int:apply_id>/reviews/",
        views.QueryPrivilegeApplicationReviewCreate.as_view(),
    ),
]
