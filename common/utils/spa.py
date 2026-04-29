from common.utils.const import WorkflowType


def spa_path_for_workflow(workflow_type, workflow_id):
    if workflow_type == WorkflowType.SQL_REVIEW:
        return f"/workflows/{workflow_id}"
    if workflow_type == WorkflowType.QUERY:
        return f"/permission-management?requestId={workflow_id}"
    if workflow_type == WorkflowType.ARCHIVE:
        return f"/archives/{workflow_id}"
    raise ValueError(f"Unsupported workflow type for SPA path: {workflow_type!r}")


def spa_url_for_workflow(base_url, workflow_type, workflow_id):
    return f"{base_url.rstrip('/')}{spa_path_for_workflow(workflow_type, workflow_id)}"
