from django.db import models


class Const(object):
    # Prefixes used for scheduled task IDs
    workflowJobprefix = {
        "query": "query",
        "sqlreview": "sqlreview",
        "archive": "archive",
    }


class WorkflowType(models.IntegerChoices):
    QUERY = 1, "Query privilege request"
    SQL_REVIEW = 2, "DDL/DML request"
    ARCHIVE = 3, "Data Archival request"
    ACCESS_REQUEST = 4, "Permission request"


class WorkflowStatus(models.IntegerChoices):
    WAITING = 0, "Pending review"
    PASSED = 1, "Approved"
    REJECTED = 2, "Rejected"
    ABORTED = 3, "Canceled"


class WorkflowAction(models.IntegerChoices):
    """Workflow actions. Values should be verbs, not states."""

    SUBMIT = 0, "Submit"
    PASS = 1, "Approve"
    REJECT = 2, "Reject"
    ABORT = 3, "Cancel"
    EXECUTE_SET_TIME = 4, "Set scheduled execution"
    EXECUTE_START = 5, "Start execution"
    EXECUTE_END = 6, "Execution finished"


class SQLTuning:
    SYS_PARM_FILTER = [
        "BINLOG_CACHE_SIZE",
        "BULK_INSERT_BUFFER_SIZE",
        "HAVE_PARTITION_ENGINE",
        "HAVE_QUERY_CACHE",
        "INTERACTIVE_TIMEOUT",
        "JOIN_BUFFER_SIZE",
        "KEY_BUFFER_SIZE",
        "KEY_CACHE_AGE_THRESHOLD",
        "KEY_CACHE_BLOCK_SIZE",
        "KEY_CACHE_DIVISION_LIMIT",
        "LARGE_PAGES",
        "LOCKED_IN_MEMORY",
        "LONG_QUERY_TIME",
        "MAX_ALLOWED_PACKET",
        "MAX_BINLOG_CACHE_SIZE",
        "MAX_BINLOG_SIZE",
        "MAX_CONNECT_ERRORS",
        "MAX_CONNECTIONS",
        "MAX_JOIN_SIZE",
        "MAX_LENGTH_FOR_SORT_DATA",
        "MAX_SEEKS_FOR_KEY",
        "MAX_SORT_LENGTH",
        "MAX_TMP_TABLES",
        "MAX_USER_CONNECTIONS",
        "OPTIMIZER_PRUNE_LEVEL",
        "OPTIMIZER_SEARCH_DEPTH",
        "QUERY_CACHE_SIZE",
        "QUERY_CACHE_TYPE",
        "QUERY_PREALLOC_SIZE",
        "RANGE_ALLOC_BLOCK_SIZE",
        "READ_BUFFER_SIZE",
        "READ_RND_BUFFER_SIZE",
        "SORT_BUFFER_SIZE",
        "SQL_MODE",
        "TABLE_CACHE",
        "THREAD_CACHE_SIZE",
        "TMP_TABLE_SIZE",
        "WAIT_TIMEOUT",
    ]
