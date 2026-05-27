# Service Visibility

A service can be visible in one Datamingle area and hidden in another.

## Inventory Visibility

Inventory visibility means the service exists in Datamingle and your user can view it through resource-group and permission rules.

## Query And Workflow Visibility

For online queries, DDL, DML, and export workflows, Datamingle also requires an online command-capable agent with an active websocket.

The service must have:

- an assigned agent,
- enabled assignment,
- enabled command execution,
- online agent status,
- active websocket connection,
- matching user access.

If any of these are missing, the service does not appear in query or workflow selectors.

