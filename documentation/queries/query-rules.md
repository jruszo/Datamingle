# Query Rules

Datamingle enforces query controls to protect database services and sensitive data.

## Common Controls

- SQL must use supported query syntax, such as `SELECT`, `SHOW`, `EXPLAIN`, or `DESCRIBE`.
- MySQL `SELECT *` can be blocked by system settings.
- Sensitive system tables such as `mysql.user` are blocked.
- Query privilege checks can reduce or reject row limits.
- Data masking can hide sensitive values.
- Maximum execution time is controlled by system settings.

## Read vs Change Work

Use online queries for read-oriented work. Use workflows for DDL, DML, or governed exports.

