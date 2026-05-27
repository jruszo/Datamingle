# Agent Assignments

An agent assignment connects an agent to a Datamingle instance.

## Assignment Options

| Option | Purpose |
| --- | --- |
| Enabled | Allows the assignment to be used. |
| Command execution | Allows query, workflow, and export commands. |
| Metrics | Allows metrics collection. |
| Online schema | Allows online schema support. |
| Logs | Allows log collection. |

## Command-Capable Services

For a service to appear in query and workflow selectors:

- assignment must be enabled,
- command execution must be enabled,
- the agent must be online,
- websocket must be active,
- user access must allow the action.

Datamingle does not run these commands directly when an agent is required.

