from django_filters import rest_framework as filters

from sql.models import SqlWorkflowContent, WorkflowAudit


class WorkflowFilter(filters.FilterSet):
    class Meta:
        model = SqlWorkflowContent
        fields = {
            "id": ["exact"],
            "workflow_id": ["exact"],
            "workflow__workflow_name": ["icontains"],
            "workflow__instance_id": ["exact"],
            "workflow__db_name": ["exact"],
            "workflow__engineer": ["exact"],
            "workflow__status": ["exact"],
            "workflow__create_time": ["lt", "gte"],
        }


class WorkflowAuditFilter(filters.FilterSet):
    class Meta:
        model = WorkflowAudit
        fields = {
            "workflow_title": ["icontains"],
            "workflow_type": ["exact"],
        }
