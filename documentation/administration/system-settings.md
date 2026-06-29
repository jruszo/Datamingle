# System Settings

System settings control platform behavior.

## Sections

- SQL Review Engine,
- SQL Release Controls,
- Query And Export Limits,
- Background Jobs,
- Export Storage,
- Notifications,
- Integrations And AI,
- Login, Access, And Defaults,
- Announcements And Branding.

Some sections include test actions, such as testing goInception, email, or storage.

Use caution when changing system settings because they affect all users.

## Background Jobs

`Inventory refresh interval` controls the scheduled agent inventory refresh
cadence. The default is `1h`. MySQL topology discovery runs as part of this
inventory refresh, so this setting also controls how often Datamingle refreshes
MySQL cluster membership and DDL/DML eligibility.

`MySQL topology drift policy` controls how Datamingle handles manually attached
MySQL cluster services when scheduled discovery disagrees with the manual
membership:

- `notify_block`: raise an alert and block DDL/DML.
- `auto_detach`: detach the service from the manual cluster and use discovered topology.
- `notify_only`: raise an alert but allow DDL/DML.
