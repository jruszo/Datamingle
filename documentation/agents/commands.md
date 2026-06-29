# Agent Commands

The command history shows work dispatched to the selected agent.

## Command Types

Commands can include:

- query execution,
- inventory collection,
- workflow check,
- workflow execution,
- export check,
- export execution.

For MySQL services, inventory collection includes topology details such as
server UUID, read-only state, replication source, and Group Replication members
when the database exposes them. Datamingle uses those details to discover
clusters and decide which MySQL service is eligible for DDL/DML.

## Command Statuses

| Status | Meaning |
| --- | --- |
| queued | Created but not yet dispatched. |
| dispatched | Sent to the agent. |
| accepted | Agent acknowledged the command. |
| running | Agent is executing the command. |
| succeeded | Command completed successfully. |
| failed | Command failed. |
| cancelled | Command was cancelled. |
| expired | Command timed out. |

Non-terminal commands can be cancelled when cancellation is available.
