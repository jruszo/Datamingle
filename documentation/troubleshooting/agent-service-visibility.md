# Agent And Service Visibility

If an instance appears in inventory but not in queries or workflows, check agent readiness.

## Checklist

- The instance is assigned to an agent.
- The assignment is enabled.
- Command execution is enabled.
- The agent status is online.
- The websocket is active.
- Your user has access to the instance.

Datamingle does not fall back to backend direct execution for query, DDL, DML, or export work that requires an agent.

