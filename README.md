<div align="center">

[![Repository](https://img.shields.io/badge/GitHub-jruszo%2FDatamingle-181717?logo=github)](https://github.com/jruszo/Datamingle)
[![version](https://img.shields.io/pypi/pyversions/django)](https://img.shields.io/pypi/pyversions/django/)
[![version](https://img.shields.io/badge/django-4.1-brightgreen.svg)](https://docs.djangoproject.com/en/4.1/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# Datamingle
<h4>SQL Review and Query Platform</h4>

[Repository](https://github.com/jruszo/Datamingle)

![](https://github.com/jruszo/Datamingle/wiki/images/dashboard.png)

</div>

Repository Layout
===============
This repository is organized as a monorepo:

- `backend/` contains the Django API, local Docker files, demo database compose files, and backend Python dependencies.
- `frontend/` contains the Vue/Vite SPA.
- `shared-infra/` contains the local shared observability stack for per-tenant VictoriaMetrics, Quickwit, Grafana, Jaeger, Prometheus, and MinIO.
- `agent/` is reserved for the upcoming Datamingle agent module.
- `scripts/` contains repo-level helper scripts.
- `documentation/` contains end-user documentation for the Datamingle web application.

User Documentation
===============
Start with the [Datamingle User Documentation](documentation/README.md) for task-oriented guides covering sign-in, inventory, agents, queries, SQL workflows, data exports, archives, permissions, audit, instance operations, and administration.

Fork Attribution
===============
Datamingle is a fork of [Archery](https://github.com/hhyo/Archery), and we retain attribution in accordance with the Apache-2.0 license.
Original project copyright and license notices are preserved in this repository.

Feature Matrix
====

| Database   | Query | Review | Execute | Backup | Data Dictionary | Session Management | Account Management | Parameter Management | Data Archive |
|------------| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| MySQL      | √ | √ | √ | √ | √ | √ | √ | √ | √ |
| MsSQL      | √ | × | √ | × | √ | × | × | × | × |
| Redis      | √ | × | √ | × | × | × | × | × | × |
| PgSQL      | √ | × | √ | × | × | × | × | × | × |
| Oracle     | √ | √ | √ | √ | √ | √ | × | × | × |
| MongoDB    | √ | √ | √ | × | × | × | √ | √ | × | × |
| Phoenix    | √ | × | √ | × | × | × | × | × | × | × |
| ODPS       | √ | × | × | × | × | × | × | × | × | × |
| ClickHouse | √ | √ | √ | × | × | × | × | × | × | × |
| Cassandra  | √ | × | √ | × | × | × | × | × | × | × |
| Doris      | √ | × | √ | × | × | × | √ | × | × | × |

Quick Start
===============
### Live Demo
Public Datamingle demo: coming soon.

### Local Demo Environment
The supported local demo setup uses three compose files:

- `backend/src/docker-compose/docker-compose.local-dev.yml` for Datamingle, MySQL, Redis, goInception, Celery, and the Vite frontend.
- `backend/src/docker-compose/docker-compose.demo-dbs.yml` for demo MySQL and PostgreSQL databases used by seeded workflow examples.
- `shared-infra/docker-compose.yml` for optional local observability services.

Create the shared Docker network once:

```bash
docker network create datamingle
```

Prepare local app settings from the tracked example:

```bash
cp backend/src/docker-compose/.env.example backend/src/docker-compose/.env.local-dev
```

Edit `backend/src/docker-compose/.env.local-dev` and generate local-only values
for `SECRET_KEY` and `FIELD_ENCRYPTION_KEYS`; that file is ignored by Git and
must not be committed:

```bash
python3 - <<'PY'
import base64
import os
import secrets

print("SECRET_KEY=" + secrets.token_urlsafe(50))
print("FIELD_ENCRYPTION_KEYS=" + base64.urlsafe_b64encode(os.urandom(32)).decode())
PY
```

Start the demo databases first:

```bash
docker-compose -f backend/src/docker-compose/docker-compose.demo-dbs.yml up -d
```

Build and start the app and frontend:

```bash
docker-compose -f backend/src/docker-compose/docker-compose.local-dev.yml up -d --build datamingle frontend
```

Open the frontend at http://localhost:5173. The backend is proxied through the
frontend dev server and is also available directly at http://localhost:9123.

The shared observability infrastructure lives outside the app stack in
`shared-infra/`. It is a dedicated local stack for shared observability services
used to test metrics and trace ingestion. It is optional for basic demo usage:

```bash
cp shared-infra/.env.example shared-infra/.env
docker-compose -f shared-infra/docker-compose.yml up -d
```

See [shared-infra/README.md](shared-infra/README.md) for service URLs, VictoriaMetrics
tenant routing, and reset instructions.

Existing local Docker data directories from before the Datamingle rename may
still have the application database under the old `archery` name. Copy that data
into the new `datamingle` database before starting the renamed stack:

```bash
scripts/docker/migrate-archery-db-to-datamingle.sh
```

The script defaults to the local demo app MySQL container, `datamingle-mysql`.

### Local Demo Seed
The local ARM compose setup seeds resource groups, auth groups, workflow settings, instance tags, and demo database instances for manual UX testing.

It no longer seeds local app users. Sign-in uses django-allauth headless JWT
email/password authentication for local Datamingle users.

See [backend/src/docker-compose/LOCAL_DEMO.md](backend/src/docker-compose/LOCAL_DEMO.md) for:

- demo roles and approval chains
- demo MySQL/PostgreSQL instance credentials
- the separate smoke-check command
- reset and reseed instructions

Manual Installation
===============
Use this repository as the source of truth: https://github.com/jruszo/Datamingle

Authentication
===============
Datamingle uses django-allauth headless JWT endpoints for SPA authentication.
End users sign in with local Datamingle email/password accounts against the
configured headless client `app`.

Relevant Django settings:

```python
SITE_ID = 1
HEADLESS_CLIENTS = ("app",)
```

Behavior:

- The SPA login page posts to `/_allauth/app/v1/auth/login` and stores the JWT
  access/refresh tokens returned by allauth.
- Public signup is closed. Superusers create local Datamingle email/password
  users through user management.
- Datamingle keeps its own local `Users`, auth/permission groups, and resource
  groups.
- Superusers manage resource-group assignments and can change active/inactive state.
- Auth groups remain permission/role levels used inside resource groups. Resource access is assigned through Datamingle resource groups. Access requests can grant temporary access to an individual user or a resource group; approved permanent requests add the user to a resource group or attach an instance to a resource group.

After authentication or environment changes, rebuild the app container:

```bash
docker-compose -f backend/src/docker-compose/docker-compose.local-dev.yml up -d --build datamingle frontend
```

### Notes
- The `django.contrib.sites` migrations create the default `Site` row used by
  `SITE_ID = 1`; apply migrations before using allauth endpoints.
- Existing local users may remain for permissions and audit history, but sign-in
  now uses allauth email/password JWT endpoints.

Run Tests
===============
```bash
cd backend
python manage.py test -v 3
```

Dependencies
===============
### Framework
- [Django](https://github.com/django/django)
- [Bootstrap](https://github.com/twbs/bootstrap)
- [jQuery](https://github.com/jquery/jquery)

### Frontend Components
- Navigation menu [metisMenu](https://github.com/onokumus/metismenu)
- Theme [sb-admin-2](https://github.com/BlackrockDigital/startbootstrap-sb-admin-2)
- Editor [ace](https://github.com/ajaxorg/ace)
- SQL formatter [sql-formatter](https://github.com/zeroturnaround/sql-formatter)
- Table [bootstrap-table](https://github.com/wenzhixin/bootstrap-table)
- Table editing [bootstrap-editable](https://github.com/vitalets/x-editable)
- Dropdown [bootstrap-select](https://github.com/snapappointments/bootstrap-select)
- File upload [bootstrap-fileinput](https://github.com/kartik-v/bootstrap-fileinput)
- Datetime picker [bootstrap-datetimepicker](https://github.com/smalot/bootstrap-datetimepicker)
- Date range picker [daterangepicker](https://github.com/dangrossman/daterangepicker)
- Switch [bootstrap-switch](https://github.com/Bttstrp/bootstrap-switch)
- Markdown rendering [marked](https://github.com/markedjs/marked)

### Backend
- Queue tasks [Celery](https://docs.celeryq.dev/)
- MySQL connector [mysqlclient-python](https://github.com/PyMySQL/mysqlclient-python)
- MsSQL connector [pyodbc](https://github.com/mkleehammer/pyodbc)
- Redis connector [redis-py](https://github.com/andymccurdy/redis-py)
- PostgreSQL connector [psycopg2](https://github.com/psycopg/psycopg2)
- Oracle connector [cx_Oracle](https://github.com/oracle/python-cx_Oracle)
- MongoDB connector [pymongo](https://github.com/mongodb/mongo-python-driver)
- Phoenix connector [phoenixdb](https://github.com/lalinsky/python-phoenixdb)
- ODPS connector [pyodps](https://github.com/aliyun/aliyun-odps-python-sdk)
- ClickHouse connector [clickhouse-driver](https://github.com/mymarilyn/clickhouse-driver)
- SQL parse/split/type detection [sqlparse](https://github.com/andialbrecht/sqlparse)
- Serialization [simplejson](https://github.com/simplejson/simplejson)
- Time utilities [python-dateutil](https://github.com/paxan/python-dateutil)

### Functional Dependencies
- Visualization [pyecharts](https://github.com/pyecharts/pyecharts)
- MySQL review/execute/backup [goInception](https://github.com/hanchuanchuan/goInception) | [inception](https://github.com/hhyo/inception)
- Large table DDL [gh-ost](https://github.com/github/gh-ost) | [pt-online-schema-change](https://www.percona.com/doc/percona-toolkit/3.0/pt-online-schema-change.html)
- MyBatis XML parsing [mybatis-mapper2sql](https://github.com/hhyo/mybatis-mapper2sql)
- RDS management [aliyun-openapi-python-sdk](https://github.com/aliyun/aliyun-openapi-python-sdk)
- Data encryption [django-mirage-field](https://github.com/luojilab/django-mirage-field)

Contributing
===============
You can check the roadmap and dependency list in this repository, claim related issues, or submit a PR directly. Thanks for contributing to Datamingle.

Contributions include but are not limited to:
- [Wiki documentation](https://github.com/jruszo/Datamingle/wiki) (if enabled)
- Bug fixes
- New features
- Code optimization
- Better test coverage

Feedback
===============
- Usage questions and requirements discussion: [Discussions](https://github.com/jruszo/Datamingle/discussions)
- Bug reports: [Issues](https://github.com/jruszo/Datamingle/issues)

Acknowledgements
===============
- [Archery](https://github.com/hhyo/Archery) Datamingle is forked from Archery.
- [archer](https://github.com/jly8866/archer) Archery is based on secondary development of archer.
- [goInception](https://github.com/hanchuanchuan/goInception) A MySQL operations tool integrating review, execution, backup, and rollback SQL generation.
- [JetBrains Open Source](https://www.jetbrains.com/opensource/) for providing free IDE licenses to this project.
  [<img src="https://resources.jetbrains.com/storage/products/company/brand/logos/jb_beam.png" width="200"/>](https://www.jetbrains.com/opensource/)
