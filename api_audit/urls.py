from django.urls import path

from api_audit import views

urlpatterns = [
    path("v1/audit/general/", views.GeneralAuditLogList.as_view()),
    path("v1/audit/query/", views.QueryAuditLogList.as_view()),
    path("v1/audit/sql-workflow/", views.SqlWorkflowAuditLogList.as_view()),
    path("v1/audit/workflow-log/", views.WorkflowOperationLogList.as_view()),
]
