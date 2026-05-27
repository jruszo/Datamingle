# Execution And Scheduling

Approved workflows can be executed immediately or scheduled.

## Execution Requirements

To execute, confirm:

- the workflow is approved,
- your user has execution permission,
- the current time is inside the execution window,
- the service has an online command-capable agent,
- no conflicting command is already running.

## Scheduling

Scheduled execution records the target run time. Eligible users can manage scheduling from the workflow detail page.

## MySQL DDL Execution

For MySQL DDL workflows, current agent execution exposes direct execution. If the service is not agent-runnable, Datamingle blocks execution rather than executing directly from the backend.

