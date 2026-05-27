# Workflow And Export Problems

## SQL Check Fails

Common causes include:

- SQL syntax does not match the workflow type,
- DDL and DML are mixed when separation is enabled,
- automatic review rejected the SQL,
- the selected database or schema is wrong,
- the agent is offline,
- your user lacks permission for the detected syntax type.

## Export Submission Fails

Common causes include:

- SQL is not `SELECT` or `WITH`,
- estimated row count exceeds the export limit,
- unsupported export format,
- service has no online command-capable agent,
- user lacks export or read access.

## Workflow Cannot Execute

Confirm approval status, executor permission, execution window, agent readiness, and whether another command is already running.

