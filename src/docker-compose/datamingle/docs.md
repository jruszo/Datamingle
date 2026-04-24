# Datamingle Local Docker Notes

This file is mounted into the local Docker app container at
`/opt/datamingle/docs/docs.md`. It gives operators and developers a short
reference for the renamed Datamingle compose stack.

## Service Naming

The application service is named `datamingle` in compose and the running app
container is `datamingle-app`.

```bash
docker-compose -f src/docker-compose/docker-compose.local-arm.yml up -d --build datamingle
docker exec datamingle-app python manage.py migrate --noinput
docker exec datamingle-app python manage.py smoke_local_demo
```

Datamingle still uses the `archery` Python package name internally for Django
imports. That module path is not the container, image, database, or product
name.

## Database Rename

New local Datamingle environments use the MySQL database named `datamingle`.
Older local environments may still contain data in a database named `archery`.
Before starting the renamed stack against an existing MySQL data directory,
copy the old database into the new name:

```bash
scripts/docker/migrate-archery-db-to-datamingle.sh
```

The migration helper creates a timestamped SQL backup in `/tmp`, creates the
target database if needed, and imports the source data into `datamingle`. Set
`BACKUP_PATH` if `/tmp` is small, tmpfs-backed, or unsuitable for a large
database dump. If you are using the non-ARM compose file where the MySQL
container is named `mysql`, run:

```bash
MYSQL_CONTAINER=mysql scripts/docker/migrate-archery-db-to-datamingle.sh
```

After migration, run the normal Django checks:

```bash
docker exec datamingle-app python manage.py makemigrations sql --check
docker exec datamingle-app python manage.py migrate --noinput
```

## Local Demo Credentials

The seeded demo database user is `demo_datamingle` with password `demo123`.
The local demo seed stores that username on the MySQL and PostgreSQL demo
instances.
