# Local Demo Bootstrap

The local dev compose environment can seed a manual-testing setup for workflow UX and approval flows during app startup.

## What gets created

- Demo infrastructure services:
  - `datamingle-mysql-demo` on `localhost:3307`
  - `datamingle-postgres-demo` on `localhost:5433`
- Auth groups, teams, workflow approval settings, instance tags, and inventory instances
- Demo database content for MySQL and PostgreSQL

## Triggering the seed

The local compose file sets `RUN_LOCAL_DEMO_SEED=1` on the `datamingle-app` container.

The demo databases run in a separate compose stack. Start them first:

```bash
docker-compose -f backend/src/docker-compose/docker-compose.demo-dbs.yml up -d
```

On rebuild or recreate, startup runs:

```bash
docker-compose -f backend/src/docker-compose/docker-compose.local-dev.yml up -d --build datamingle frontend
```

The app container then runs:

```bash
docker exec -w /opt/datamingle/backend datamingle-app python manage.py seed_local_demo
```

The seed is idempotent. Re-running it updates the named demo records without clearing unrelated local data.

To disable automatic seeding for local startup, set `RUN_LOCAL_DEMO_SEED` to `0` in the compose file.

## Demo app users

The local demo seed no longer creates local app users. It removes legacy seeded
records named `demo_admin`, `demo_requester`, `demo_pm`, and `demo_dba` if they
exist.

Manual testing should sign in through WorkOS. A WorkOS user with the built-in
Admin role (`admin`) is refreshed into local Datamingle superuser access on
every login.

## Demo teams and approval chains

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

Both instances are tagged with `can_read` and `can_write` and are associated with both demo teams for manual workflow/UI testing.

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
docker exec -w /opt/datamingle/backend datamingle-app python manage.py smoke_local_demo
```

This command verifies:

- legacy seeded users have been removed
- seeded teams/instances exist
- approval preview works for both demo teams
- demo instances are reachable and expose the expected database names

## Resetting the demo databases

The local compose services are intentionally ephemeral:

- the app MySQL database is not bind-mounted
- the demo MySQL/PostgreSQL services do not use named volumes

If you want to recreate the initial SQL content from scratch, tear down all local stacks and bring them back up again:

```bash
docker-compose -f backend/src/docker-compose/docker-compose.local-dev.yml down -v
docker-compose -f backend/src/docker-compose/docker-compose.demo-dbs.yml down -v
docker-compose -f backend/src/docker-compose/docker-compose.demo-dbs.yml up -d
docker-compose -f backend/src/docker-compose/docker-compose.local-dev.yml up -d --build datamingle frontend
```

That recreates the databases, reruns migrations, and reapplies the local demo seed.
