# Datamingle Backend

This directory contains the Django backend, backend Docker assets, Helm chart, and Python dependency files.

Common commands should run from this directory, or with Docker using `/opt/datamingle/backend` as the working directory.

```bash
python manage.py test -v 3
docker exec -w /opt/datamingle/backend datamingle-app python manage.py makemigrations sql --check
```
