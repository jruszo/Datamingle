from django.urls import path, include
from sql_api import views
from rest_framework import routers
from drf_spectacular.views import (
    SpectacularAPIView,
    SpectacularRedocView,
    SpectacularSwaggerView,
)
from . import (
    api_user,
    api_instance,
    api_workflow,
    api_auth,
    api_query,
    api_dashboard,
    api_permission,
    api_settings,
    api_archive,
)

router = routers.DefaultRouter()

urlpatterns = [
    path("v1/", include(router.urls)),
    path(
        "auth/config/",
        api_auth.AuthConfigView.as_view(),
        name="auth_config",
    ),
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
    path(
        "auth/workos/authorize/",
        api_auth.WorkOSAuthorizeView.as_view(),
        name="workos_authorize",
    ),
    path(
        "auth/workos/callback/",
        api_auth.WorkOSCallbackView.as_view(),
        name="workos_callback",
    ),
    path(
        "auth/workos/exchange/",
        api_auth.WorkOSExchangeView.as_view(),
        name="workos_exchange",
    ),
    path(
        "auth/workos/logout/",
        api_auth.WorkOSLogoutView.as_view(),
        name="workos_logout",
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
    path("v1/system-settings/", api_settings.SystemSettingsView.as_view()),
    path(
        "v1/system-settings/tests/go-inception/",
        api_settings.SystemSettingsGoInceptionTestView.as_view(),
    ),
    path(
        "v1/system-settings/tests/email/",
        api_settings.SystemSettingsEmailTestView.as_view(),
    ),
    path(
        "v1/system-settings/tests/storage/",
        api_settings.SystemSettingsStorageTestView.as_view(),
    ),
    path("v1/user/<int:pk>/", api_user.UserDetail.as_view()),
    path("v1/user/group/", api_user.GroupList.as_view()),
    path("v1/user/group/<int:pk>/", api_user.GroupDetail.as_view()),
    path("v1/user/permission/", api_user.PermissionList.as_view()),
    path(
        "v1/user/resourcegroup/users/lookup/",
        api_user.ResourceGroupUserLookup.as_view(),
    ),
    path(
        "v1/user/resourcegroup/instances/lookup/",
        api_user.ResourceGroupInstanceLookup.as_view(),
    ),
    path("v1/user/resourcegroup/", api_user.ResourceGroupList.as_view()),
    path("v1/user/resourcegroup/<int:pk>/", api_user.ResourceGroupDetail.as_view()),
    path("v1/user/auth/", api_user.UserAuth.as_view()),
    path("v1/user/2fa/", api_user.TwoFA.as_view()),
    path("v1/user/2fa/state/", api_user.TwoFAState.as_view()),
    path("v1/user/2fa/save/", api_user.TwoFASave.as_view()),
    path("v1/user/2fa/verify/", api_user.TwoFAVerify.as_view()),
    path("v1/instance/", api_instance.InstanceList.as_view()),
    path("v1/instance/tag/", api_instance.InstanceTagList.as_view()),
    path("v1/instance/tag/<int:pk>/", api_instance.InstanceTagDetail.as_view()),
    path("v1/instance/metadata/", api_instance.InstanceMetadata.as_view()),
    path("v1/instance/tag/", api_instance.InstanceTagList.as_view()),
    path("v1/instance/tag/<int:pk>/", api_instance.InstanceTagDetail.as_view()),
    path(
        "v1/instance/test-connection/",
        api_instance.InstanceDraftConnectionTest.as_view(),
    ),
    path(
        "v1/instance/<int:pk>/test-connection/",
        api_instance.InstanceConnectionTest.as_view(),
    ),
    path("v1/instance/<int:pk>/", api_instance.InstanceDetail.as_view()),
    path("v1/instance/resource/", api_instance.InstanceResource.as_view()),
    path("v1/instance/tunnel/", api_instance.TunnelList.as_view()),
    path("v1/instance/rds/", api_instance.AliyunRdsList.as_view()),
    path("v1/workflow/", api_workflow.WorkflowList.as_view()),
    path("v1/workflow/metadata/", api_workflow.WorkflowMetadata.as_view()),
    path(
        "v1/workflow/submission-metadata/",
        api_workflow.WorkflowSubmissionMetadata.as_view(),
    ),
    path(
        "v1/workflow/export/submission-metadata/",
        api_workflow.WorkflowExportSubmissionMetadata.as_view(),
    ),
    path(
        "v1/workflow/approval-preview/",
        api_workflow.WorkflowApprovalPreview.as_view(),
    ),
    path("v1/workflow/parse/", api_workflow.WorkflowParse.as_view()),
    path("v1/workflow/sqlcheck/", api_workflow.ExecuteCheck.as_view()),
    path("v1/workflow/export/sqlcheck/", api_workflow.WorkflowExportCheck.as_view()),
    path("v1/workflow/auditlist/", api_workflow.WorkflowAuditList.as_view()),
    path("v1/workflow/<int:workflow_id>/", api_workflow.WorkflowDetail.as_view()),
    path(
        "v1/workflow/<int:workflow_id>/content/",
        api_workflow.WorkflowContentDetail.as_view(),
    ),
    path(
        "v1/workflow/<int:workflow_id>/download/",
        api_workflow.WorkflowDownload.as_view(),
    ),
    path(
        "v1/workflow/<int:workflow_id>/rollback/",
        api_workflow.WorkflowRollbackDetail.as_view(),
    ),
    path(
        "v1/workflow/<int:workflow_id>/window/",
        api_workflow.WorkflowExecutionWindowUpdate.as_view(),
    ),
    path(
        "v1/workflow/<int:workflow_id>/execution-window/",
        api_workflow.WorkflowExecutionWindowUpdate.as_view(),
    ),
    path(
        "v1/workflow/<int:workflow_id>/schedule/",
        api_workflow.WorkflowScheduleCreate.as_view(),
    ),
    path(
        "v1/workflow/<int:workflow_id>/reviews/",
        api_workflow.WorkflowReviewCreate.as_view(),
    ),
    path(
        "v1/workflow/<int:workflow_id>/executions/",
        api_workflow.WorkflowExecutionCreate.as_view(),
    ),
    path("v1/workflow/log/", api_workflow.WorkflowLogList.as_view()),
    path("v1/archive/metadata/", api_archive.ArchiveMetadata.as_view()),
    path(
        "v1/archive/approval-preview/",
        api_archive.ArchiveApprovalPreview.as_view(),
    ),
    path("v1/archive/", api_archive.ArchiveListCreate.as_view()),
    path("v1/archive/<int:archive_id>/", api_archive.ArchiveDetail.as_view()),
    path(
        "v1/archive/<int:archive_id>/reviews/",
        api_archive.ArchiveReviewCreate.as_view(),
    ),
    path(
        "v1/archive/<int:archive_id>/run/",
        api_archive.ArchiveRunNow.as_view(),
    ),
    path(
        "v1/archive/<int:archive_id>/state/",
        api_archive.ArchiveStateUpdate.as_view(),
    ),
    path(
        "v1/archive/<int:archive_id>/logs/",
        api_archive.ArchiveLogList.as_view(),
    ),
    path("v1/query/instance/", api_query.QueryInstanceList.as_view()),
    path("v1/query/describe/", api_query.QueryDescribe.as_view()),
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
    path(
        "v1/access/resource-groups/lookup/",
        api_permission.PermissionResourceGroupLookup.as_view(),
    ),
    path(
        "v1/access/instances/lookup/",
        api_permission.PermissionInstanceLookup.as_view(),
    ),
    path("v1/access/request/", api_permission.PermissionRequestListCreate.as_view()),
    path(
        "v1/access/request/<int:request_id>/",
        api_permission.PermissionRequestDetail.as_view(),
    ),
    path(
        "v1/access/request/<int:request_id>/reviews/",
        api_permission.PermissionRequestReviewCreate.as_view(),
    ),
    path("v1/access/grant/", api_permission.ActiveGrantList.as_view()),
    path(
        "v1/access/grant/<str:grant_type>/<int:grant_id>/",
        api_permission.ActiveGrantDetail.as_view(),
    ),
    path("info", views.info),
    path("debug", views.debug),
]
