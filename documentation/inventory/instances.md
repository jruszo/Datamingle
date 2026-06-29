# Manage Instances

An instance is a database service registered in Datamingle.

## Common Fields

- instance name,
- type or environment,
- database type,
- host and port,
- connection user,
- password,
- default database,
- charset,
- SSL settings,
- service name or SID for Oracle,
- visible database regex,
- denied database regex,
- teams,
- workflow policy,
- query and workflow enablement.

## Add An Instance

1. Open `Inventory`.
2. Select `Add instance`.
3. Enter connection details.
4. Attach at least one team.
5. Select query or workflow enablement when the service should appear in those
   flows.
6. Select a workflow policy when query or workflow enablement is on.
7. Test the connection if you have permission.
8. Save.

## Edit An Instance

Open an instance from the inventory list, change the required fields, and save. Changes can affect which users can see or operate on the service.

## MySQL Topology

MySQL instances participate in automatic topology discovery during scheduled
inventory refreshes. Datamingle records whether a service appears standalone,
primary, replica, missing a master, ambiguous, or drifted from manual cluster
membership.

For DDL and DML workflows, only writable standalone services or detected cluster
masters are eligible targets. Replicas and clusters without a clear registered
master remain visible but are blocked for DDL/DML.
