// Workflow request types: 1.sql 2.query
workflow_type = {
    'query': 1,
    'query_display': 'Query Permission Request',
    'sqlreview': 2,
    'sqlreview_display': 'SQL Release Request',
    'archive': 3,
    'archive_display': 'Data Archive Request',
}

// 0.Pending Review 1.Approved/Waiting to Execute 2.Rejected 3.Canceled 101.Running 102.Succeeded 103.Failed
workflow_status = {
    'audit_wait': 0,
    'audit_wait_display': 'Pending Review',
    'audit_success': 1,
    'audit_success_display': 'Approved',
    'audit_reject': 2,
    'audit_reject_display': 'Rejected',
    'audit_abort': 3,
    'audit_abort_display': 'Review Canceled'
}


function sqlworkflowStatus_formatter(value) {
    if (value === "workflow_finish") {
        return "<span class=\"label label-success\">" + gettext(value) + "</span>"
    }
    else if (value === "workflow_abort") {
        return "<span class=\"label label-default\">" + gettext(value) + "</span>"
    }
    else if (value === "workflow_manreviewing") {
        return "<span class=\"label label-info\">" + gettext(value) + "</span>"
    }
    else if (value === "workflow_review_pass") {
        return "<span class=\"label label-warning\">" + gettext(value) + "</span>"
    }
    else if (value === "workflow_timingtask") {
        return "<span class=\"label label-warning\">" + gettext(value) + "</span>"
    }
    else if (value === "workflow_queuing") {
        return "<span class=\"label label-info \">" + gettext(value) + "</span>"
    }
    else if (value === "workflow_executing") {
        return "<span class=\"label label-primary\">" + gettext(value) + "</span>"
    }
    else if (value === "workflow_autoreviewwrong") {
        return "<span class=\"label label-danger\">" + gettext(value) + "</span>"
    }
    else if (value === "workflow_exception") {
        return "<span class=\"label label-danger\">" + gettext(value) + "</span>"
    }
    else if (value === "workflow_finish_manual") {
        return "<span class=\"label label-success\">" + gettext(value) + "</span>"
    }
    else {
        return "<span class=\"label label-danger\">" + "Unknown Status" + "</span>"
    }
}

function workflow_type_formatter(value) {
    // If value is a string, convert it to an integer
    if (typeof value === "string") {
        value = parseInt(value, 10);
    }
    if (value === workflow_type.query) {
        return workflow_type.query_display
    }
    else if (value === workflow_type.sqlreview) {
        return workflow_type.sqlreview_display
    }
    else if (value === workflow_type.archive) {
        return workflow_type.archive_display
    }
    else {
        return 'Unknown Status'
    }
}

function workflow_status_formatter(value) {
    // If value is a string, convert it to an integer
    if (typeof value === "string") {
        value = parseInt(value, 10);
    }
    if (value === workflow_status.audit_wait) {
        return "<span class='label label-info'>" + workflow_status.audit_wait_display + " </span>"
    }
    else if (value === workflow_status.audit_success) {
        return "<span class='label label-success'> " + workflow_status.audit_success_display + " </span>"
    }
    else if (value === workflow_status.audit_reject) {
        return "<span class='label label-danger'>" + workflow_status.audit_reject_display + " </span>"
    }
    else if (value === workflow_status.audit_abort) {
        return "<span class='label label-default'>" + workflow_status.audit_abort_display + " </span>"
    }
    else {
        return "<span class=\"label label-danger\">" + 'Unknown Status' + "</span>"
    }
}
