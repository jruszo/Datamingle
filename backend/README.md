# Datamingle Backend

This directory contains the Django backend, local Docker assets, demo compose files, and Python dependency files.

The supported local demo stack is documented in the root [README](../README.md)
and [backend/src/docker-compose/LOCAL_DEMO.md](src/docker-compose/LOCAL_DEMO.md).
The legacy Helm chart and generic compose deployment were removed because they
were stale Archery-era deployment paths and were not used by the Datamingle demo
environment.

Common commands should run from this directory, or with Docker using `/opt/datamingle/backend` as the working directory.

```bash
python manage.py test -v 3
docker exec -w /opt/datamingle/backend datamingle-app python manage.py makemigrations sql --check
```
