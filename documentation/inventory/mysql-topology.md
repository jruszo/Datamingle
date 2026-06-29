# MySQL Topology

Datamingle discovers MySQL topology from agent inventory refreshes. The agent
asks each assigned MySQL service for server identity, read-only state,
replication source, and Group Replication members when available.

## Refresh Cadence

Inventory refresh is scheduled through the Celery-backed task scheduler. The
default interval is `1h`, and it can be changed from `System Settings` under
`Background Jobs`.

Each refresh asks the agent to run `inventory.collect`. For MySQL services,
that command includes topology data and Datamingle reconciles cluster membership
after the snapshot is saved.

## Cluster Discovery

Datamingle groups MySQL services into clusters using:

- the service endpoint,
- replication source host and port,
- Group Replication member host and port,
- the detected primary endpoint when Group Replication exposes it.

Clusters are discovered automatically when the related services are registered
in Datamingle. A cluster can be renamed from the infrastructure service detail
view. The cluster label is stored separately so it can be used as a stable
monitoring label.

## Master Selection

DDL and DML workflows must target the cluster master. Datamingle only lists
eligible MySQL workflow targets when the service is writable and topology
indicates that it is the standalone service or the cluster master.

Replicas remain visible in inventory and infrastructure views, but they are
blocked for DDL and DML with a reason that directs users to the master.

## Missing Or Ambiguous Masters

If Datamingle discovers a replica but the master is not registered, the cluster
is marked as missing its master. DDL and DML are blocked until the master
service is added and discovered.

If multiple possible masters are detected, the cluster is marked ambiguous. DDL
and DML are blocked until topology discovery can identify a single master.

Cluster health and active topology alerts are shown in infrastructure views.
These alerts help administrators see when Datamingle knows only part of a
cluster and should add the missing services.

## Manual Membership And Drift

Administrators can manually attach services to a cluster. If later discovery
disagrees with that manual membership, Datamingle applies the configured MySQL
topology drift policy:

- `notify_block`: keep the manual membership, raise an alert, and block DDL/DML.
- `auto_detach`: detach the service from the manually assigned cluster and use
  discovered membership.
- `notify_only`: keep the manual membership and allow DDL/DML while surfacing
  the drift.
