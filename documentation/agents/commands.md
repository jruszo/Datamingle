# Agent Commands

The command history shows work dispatched to the selected agent.

## Command Types

Commands can include:

- query execution,
- workflow check,
- workflow execution,
- export check,
- export execution.

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

