# Service Visibility

A service can be visible in one Datamingle area and hidden in another.

## Inventory Visibility

Inventory visibility means the service exists in Datamingle and your user can
view it through team and permission rules.

## Query And Workflow Visibility

For online queries, DDL, DML, and export workflows, Datamingle also requires an
online command-capable agent with an active websocket.

The service must have:

- an assigned agent,
- enabled assignment,
- enabled command execution,
- online agent status,
- active websocket connection,
- matching user access,
- the relevant service capability enabled.

Online query and export selectors require the service to be queryable. DDL and
DML selectors require the service to be workflow-enabled and to have an active
workflow policy.

For MySQL DDL/DML, Datamingle also checks topology eligibility. Replicas and
clusters with missing or ambiguous masters are hidden from workflow selectors;
direct/API attempts against those targets are blocked with the topology reason.

If any required condition is missing, the service does not appear in query or
workflow selectors.
