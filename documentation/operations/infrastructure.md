# Infrastructure Nodes And Services

Use `Infrastructure` to manage where database services run, which agents can
operate on them, and how Datamingle exposes them to query, workflow,
monitoring, and topology features.

## Nodes

An infrastructure node represents a host or environment that can run one or
more database services. A node can have:

- teams,
- monitoring labels,
- monitoring collector settings,
- a local or remote agent assignment,
- one or more registered services.

Monitoring labels on the node are inherited by services unless a service
overrides a label with the same name.

## Services

A service is a database endpoint attached to a node. The service editor controls:

- engine, host, port, user, password, database, charset, and SSL settings,
- monitoring enablement and collector selection,
- online query enablement,
- DDL/DML workflow enablement,
- workflow policy,
- teams that can see or operate on the service.

Services that enable online query or DDL/DML workflows require a workflow policy.
The selected workflow policy must be active when saving the service.

## Agent Dependency

Agent-backed features require a command-capable online agent with an active
websocket. If the agent is offline, disabled, not assigned, or command execution
is disabled, the service can remain visible in infrastructure but will not be
available in query or workflow selectors.

## MySQL Cluster Section

For MySQL services, the service detail shows discovered cluster state:

- cluster name,
- role such as master, replica, or standalone,
- topology health,
- block reason for DDL/DML when one applies,
- metric label value exposed as `dm_mysql_cluster`.

When a service belongs to a discovered cluster, administrators can edit the
cluster name and metric label from the service detail. The metric label should
remain stable because monitoring systems can use it for grouping.

## Inventory Refresh

Inventory refresh runs hourly by default through the Celery-backed scheduler.
For MySQL services, the inventory command also refreshes topology state. Use the
service connection test when you need Datamingle to ask the agent for a fresh
snapshot before the next scheduled run.
