from django.urls import path, include
from sql_api import views
from rest_framework import routers
from drf_spectacular.views import (
    SpectacularAPIView,
    SpectacularRedocView,
    SpectacularSwaggerView,
)
from . import api_user, api_instance, api_workflow, api_auth, api_query, api_dashboard

router = routers.DefaultRouter()

urlpatterns = [
    path("v1/", include(router.urls)),
    path(
        "auth/token/",
        api_auth.SPATokenObtainPairView.as_view(),
        name="token_obtain_pair",
    ),
    path(
        "auth/token/sms/",
        api_auth.TokenSMSCaptchaView.as_view(),
        name="token_sms_captcha",
    ),
    path(
        "auth/token/refresh/",
        api_auth.SPATokenRefreshView.as_view(),
        name="token_refresh",
    ),
    path(
        "auth/token/verify/",
        api_auth.SPATokenVerifyView.as_view(),
        name="token_verify",
    ),
    path("schema/", SpectacularAPIView.as_view(), name="schema"),
    path(
        "swagger/",
        SpectacularSwaggerView.as_view(url_name="sql_api:schema"),
        name="swagger",
    ),
    path(
        "redoc/", SpectacularRedocView.as_view(url_name="sql_api:schema"), name="redoc"
    ),
    path("v1/user/", api_user.UserList.as_view()),
    path("v1/me/", api_user.CurrentUser.as_view()),
    path("v1/me/password/", api_user.CurrentUserPassword.as_view()),
    path("v1/dashboard/", api_dashboard.DashboardOverview.as_view()),
    path("v1/user/<int:pk>/", api_user.UserDetail.as_view()),
    path("v1/user/group/", api_user.GroupList.as_view()),
    path("v1/user/group/<int:pk>/", api_user.GroupDetail.as_view()),
    path("v1/user/resourcegroup/", api_user.ResourceGroupList.as_view()),
    path("v1/user/resourcegroup/<int:pk>/", api_user.ResourceGroupDetail.as_view()),
    path("v1/user/auth/", api_user.UserAuth.as_view()),
    path("v1/user/2fa/", api_user.TwoFA.as_view()),
    path("v1/user/2fa/state/", api_user.TwoFAState.as_view()),
    path("v1/user/2fa/save/", api_user.TwoFASave.as_view()),
    path("v1/user/2fa/verify/", api_user.TwoFAVerify.as_view()),
    path("v1/instance/", api_instance.InstanceList.as_view()),
    path("v1/instance/<int:pk>/", api_instance.InstanceDetail.as_view()),
    path("v1/instance/resource/", api_instance.InstanceResource.as_view()),
    path("v1/instance/tunnel/", api_instance.TunnelList.as_view()),
    path("v1/instance/rds/", api_instance.AliyunRdsList.as_view()),
    path("v1/workflow/", api_workflow.WorkflowList.as_view()),
    path("v1/workflow/sqlcheck/", api_workflow.ExecuteCheck.as_view()),
    path("v1/workflow/auditlist/", api_workflow.WorkflowAuditList.as_view()),
    path(
        "v1/workflow/<int:workflow_id>/reviews/",
        api_workflow.WorkflowReviewCreate.as_view(),
    ),
    path(
        "v1/workflow/<int:workflow_id>/executions/",
        api_workflow.WorkflowExecutionCreate.as_view(),
    ),
    path("v1/workflow/log/", api_workflow.WorkflowLogList.as_view()),
    path("v1/query/", api_query.QueryExecute.as_view()),
    path("v1/query/log/", api_query.QueryLogList.as_view()),
    path("v1/query/log/audit/", api_query.QueryLogAuditList.as_view()),
    path("v1/query/favorite/", api_query.QueryFavorite.as_view()),
    path(
        "v1/query/privilege/apply/",
        api_query.QueryPrivilegesApplyListCreate.as_view(),
    ),
    path("v1/query/privilege/", api_query.QueryPrivilegesList.as_view()),
    path(
        "v1/query/privilege/<int:privilege_id>/",
        api_query.QueryPrivilegeDetail.as_view(),
    ),
    path(
        "v1/query/privilege/apply/<int:apply_id>/reviews/",
        api_query.QueryPrivilegeApplicationReviewCreate.as_view(),
    ),
    path("info", views.info),
    path("debug", views.debug),
    path("do_once/mirage", views.mirage),
]
