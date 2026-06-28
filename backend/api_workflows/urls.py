from django.urls import path

from api_workflows import views

urlpatterns = [
    path("v1/workflow/", views.WorkflowList.as_view()),
    path("v1/workflow/metadata/", views.WorkflowMetadata.as_view()),
    path(
        "v1/workflow/submission-metadata/",
        views.WorkflowSubmissionMetadata.as_view(),
    ),
    path(
        "v1/workflow/export/submission-metadata/",
        views.WorkflowExportSubmissionMetadata.as_view(),
    ),
    path("v1/workflow/policies/", views.WorkflowPolicyList.as_view()),
    path("v1/workflow/policies/metadata/", views.WorkflowPolicyMetadata.as_view()),
    path("v1/workflow/policies/<int:policy_id>/", views.WorkflowPolicyDetail.as_view()),
    path("v1/workflow/approval-preview/", views.WorkflowApprovalPreview.as_view()),
    path("v1/workflow/parse/", views.WorkflowParse.as_view()),
    path("v1/workflow/sqlcheck/", views.ExecuteCheck.as_view()),
    path("v1/workflow/export/sqlcheck/", views.WorkflowExportCheck.as_view()),
    path("v1/workflow/auditlist/", views.WorkflowAuditList.as_view()),
    path("v1/workflow/<int:workflow_id>/", views.WorkflowDetail.as_view()),
    path(
        "v1/workflow/<int:workflow_id>/content/",
        views.WorkflowContentDetail.as_view(),
    ),
    path(
        "v1/workflow/<int:workflow_id>/download/",
        views.WorkflowDownload.as_view(),
    ),
    path(
        "v1/workflow/<int:workflow_id>/window/",
        views.WorkflowExecutionWindowUpdate.as_view(),
    ),
    path(
        "v1/workflow/<int:workflow_id>/execution-window/",
        views.WorkflowExecutionWindowUpdate.as_view(),
    ),
    path(
        "v1/workflow/<int:workflow_id>/schedule/",
        views.WorkflowScheduleCreate.as_view(),
    ),
    path(
        "v1/workflow/<int:workflow_id>/reviews/",
        views.WorkflowReviewCreate.as_view(),
    ),
    path(
        "v1/workflow/<int:workflow_id>/executions/",
        views.WorkflowExecutionCreate.as_view(),
    ),
    path("v1/workflow/log/", views.WorkflowLogList.as_view()),
]
