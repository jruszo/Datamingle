# Local Demo Bootstrap

The local dev compose environment can seed a manual-testing setup for workflow UX and approval flows during app startup.

## What gets created

- Demo infrastructure services:
  - `datamingle-mysql-demo` on `localhost:3307`
  - `datamingle-postgres-demo` on `localhost:5433`
- Demo app users, auth groups, resource groups, memberships, workflow approval settings, instance tags, and inventory instances
- Demo database content for MySQL and PostgreSQL

## Triggering the seed

The local compose file sets `RUN_LOCAL_DEMO_SEED=1` on the `datamingle-app` container.

On rebuild or recreate, startup runs:

```bash
docker-compose -f src/docker-compose/docker-compose.local-dev.yml up -d --build datamingle frontend
```

The app container then runs:

```bash
docker exec datamingle-app python manage.py seed_local_demo
```

The seed is idempotent. Re-running it updates the named demo records without clearing unrelated local data.

To disable automatic seeding for local startup, set `RUN_LOCAL_DEMO_SEED` to `0` in the compose file.

## Demo app users

Seeded demo app users are local access records only. They do not have usable Datamingle passwords; sign-in still comes through WorkOS.

Created users:

- `demo_admin`
  - Full-access local superuser
- `demo_requester`
  - Primary requester for manual workflow submission flows
  - Direct member of both demo resource groups
- `demo_pm`
  - First-stage reviewer for the multi-stage approval flow
  - Direct member of the multi-stage demo resource group
- `demo_dba`
  - Single-stage reviewer, second-stage reviewer, and executor
  - Direct member of both demo resource groups

Manual role switching requires signing in through WorkOS as a user linked to the matching local record, or using test helpers that force-authenticate the seeded users.

## Demo resource groups and approval chains

- `Demo Workflow Single Stage`
  - Approval chain: `DBA`
- `Demo Workflow Multi Stage`
  - Approval chain: `PM -> DBA`

## Demo inventory instances

- `demo-mysql-workflow`
  - Engine: MySQL
  - Host: `mysql_demo`
  - Port: `3306`
  - Visible databases: `demo_orders`, `demo_billing`
- `demo-pgsql-workflow`
  - Engine: PostgreSQL
  - Host: `postgres_demo`
  - Port: `5432`
  - Visible databases: `workflow_pg`, `analytics_pg`

Both instances are tagged with `can_read` and `can_write` and are associated with both demo resource groups for manual workflow/UI testing.

## Demo database credentials

- MySQL demo user:
  - Username: `demo_datamingle`
  - Password: `demo123`
- PostgreSQL demo user:
  - Username: `demo_datamingle`
  - Password: `demo123`

## Separate smoke check

Smoke verification is intentionally separate from seeding.

Run it manually with:

```bash
docker exec datamingle-app python manage.py smoke_local_demo
```

This command verifies:

- seeded users/resource groups/instances exist
- demo-user login works
- approval preview works for both demo resource groups
- demo instances are reachable and expose the expected database names

## Resetting the demo databases

The local ARM compose services are intentionally ephemeral:

- the app MySQL database is not bind-mounted
- the demo MySQL/PostgreSQL services do not use named volumes

If you want to recreate the initial SQL content from scratch, tear down the local stack and bring it back up again:

```bash
docker-compose -f src/docker-compose/docker-compose.local-dev.yml down -v
docker-compose -f src/docker-compose/docker-compose.local-dev.yml up -d --build datamingle frontend
```

That recreates the databases, reruns migrations, and reapplies the local demo seed.
