from django.urls import path

from api_audit import views

urlpatterns = [
    path(
        "v1/audit/general/",
        views.GeneralAuditLogList.as_view(),
        name="audit-general",
    ),
    path("v1/audit/query/", views.QueryAuditLogList.as_view(), name="audit-query"),
    path(
        "v1/audit/sql-workflow/",
        views.SqlWorkflowAuditLogList.as_view(),
        name="audit-sql-workflow",
    ),
    path(
        "v1/audit/workflow-log/",
        views.WorkflowOperationLogList.as_view(),
        name="audit-workflow-log",
    ),
]
