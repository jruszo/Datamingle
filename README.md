<div align="center">

[![Repository](https://img.shields.io/badge/GitHub-jruszo%2FDatamingle-181717?logo=github)](https://github.com/jruszo/Datamingle)
[![version](https://img.shields.io/pypi/pyversions/django)](https://img.shields.io/pypi/pyversions/django/)
[![version](https://img.shields.io/badge/django-6.0-brightgreen.svg)](https://docs.djangoproject.com/en/6.0/)
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
- `shared-infra/` contains the local shared observability stack for per-tenant VictoriaMetrics, Quickwit, Grafana, Jaeger, and MinIO.
- `agent/` contains the Datamingle worker that runs assigned database commands and
  monitoring collectors near managed services.
- `scripts/` contains repo-level helper scripts.
- `documentation/` contains end-user documentation for the Datamingle web application.

User Documentation
===============
Start with the [Datamingle User Documentation](documentation/README.md) for task-oriented guides covering sign-in, inventory, agents, queries, SQL workflows, data exports, archives, permissions, audit, instance operations, metrics, dashboards, and administration.

Fork Attribution
===============
Datamingle is a fork of [Archery](https://github.com/hhyo/Archery), and we retain attribution in accordance with the Apache-2.0 license.
Original project copyright and license notices are preserved in this repository.

Feature Matrix
====

Datamingle is migrating database work onto the agent. The agent-backed product
surface currently supports MySQL and PostgreSQL services. MySQL has monitoring,
inventory, topology discovery, online query, governed DDL/DML, and export
command support through assigned agents. PostgreSQL is supported for inventory
and monitoring workflows.

Legend: `Yes` = supported, `Partial` = supported with database-specific limits,
`No` = not currently wired.

| Database | Agent Commands | Monitoring | Online Queries | Governed DDL/DML | Data Exports | Data Dictionary | Database Management | Account Management | Parameter Management | Session Diagnostics | Archives |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| MySQL | Partial | Yes | Partial | Partial | Partial | Yes | Yes | Yes | Yes | Yes | Yes |
| PostgreSQL | No | Yes | No | No | No | No | No | No | No | No | No |

Notes:

- Agent commands (connection test, inventory collection, monitoring, governed
  DDL/DML checks and execution, and export checks and execution) are functional
  for MySQL via online, command-enabled agents.
- MySQL inventory refreshes include topology discovery. DDL/DML targets are
  limited to writable standalone services or detected cluster masters; replicas
  and clusters with missing or ambiguous masters are blocked.
- Online queries for MySQL execute through agents, but data masking and
  table-level permission checks still rely on the internal goInception engine.
- PostgreSQL services can be registered on infrastructure nodes and monitored
  with `postgres_exporter`.
- Data Dictionary, database/account/parameter operations, diagnostics, and
  archive workflows operate through direct backend engine connections.
- Some legacy backend engine connectors remain in the codebase while features
  are moved behind agent execution; they are not listed as supported
  agent-backed databases until the agent can run them.

Quick Start
===============
### Live Demo
Public Datamingle demo: coming soon.

### Local Demo Environment
The supported local demo setup uses three compose files:

- `backend/src/docker-compose/docker-compose.local-dev.yml` for Datamingle, MySQL, Redis, Celery, and the Vite frontend.
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
The local compose setup seeds teams, permission groups, workflow settings,
instance tags, agents, agent assignments, and demo database instances for manual
UX testing.

It no longer seeds local app users. Sign-in uses django-allauth headless JWT
email/password authentication for local Datamingle users.

See [backend/src/docker-compose/LOCAL_DEMO.md](backend/src/docker-compose/LOCAL_DEMO.md) for:

- demo teams, roles, and approval chains
- demo MySQL/PostgreSQL instance credentials
- demo agent setup and monitoring tool artifacts
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
- Datamingle keeps its own local `Users`, permission groups, teams, and
  instance/team assignments.
- Superusers manage users, permission levels, and team membership and can change
  active/inactive state.
- Permission groups remain role levels used inside teams. Resource access is
  assigned through Datamingle teams. Access requests can grant temporary access
  to an individual user or a team; approved permanent requests add the user to a
  team or attach an instance to a team.

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
docker exec -w /opt/datamingle/backend datamingle-app python manage.py test
docker exec -w /opt/datamingle datamingle-app black --check backend
cd frontend && npm run build
```

Dependencies
===============
### Framework
- [Django](https://github.com/django/django)
- [Django REST framework](https://github.com/encode/django-rest-framework)
- [django-allauth headless](https://github.com/pennersr/django-allauth)
- [Channels](https://github.com/django/channels)
- [Vue](https://github.com/vuejs/core)
- [Vite](https://github.com/vitejs/vite)

### Frontend Components
- UI primitives [Reka UI](https://github.com/unovue/reka-ui)
- Icons [lucide-vue-next](https://github.com/lucide-icons/lucide)
- State management [Pinia](https://github.com/vuejs/pinia)
- Routing [Vue Router](https://github.com/vuejs/router)
- SQL editor [CodeMirror](https://github.com/codemirror/dev)
- PromQL editor support [prometheus-io/codemirror-promql](https://github.com/prometheus/prometheus)
- SQL formatting [sql-formatter](https://github.com/sql-formatter-org/sql-formatter)
- Charts [Apache ECharts](https://github.com/apache/echarts)
- Dashboard layouts [GridStack](https://github.com/gridstack/gridstack.js)
- Utilities [VueUse](https://github.com/vueuse/vueuse)

### Backend
- Queue tasks [Celery](https://docs.celeryq.dev/)
- MySQL connector [mysqlclient-python](https://github.com/PyMySQL/mysqlclient-python)
- MsSQL connector [pyodbc](https://github.com/mkleehammer/pyodbc)
- Redis connector [redis-py](https://github.com/andymccurdy/redis-py)
- PostgreSQL connector [psycopg2](https://github.com/psycopg/psycopg2)
- Oracle connector [python-oracledb](https://github.com/oracle/python-oracledb)
- MongoDB connector [pymongo](https://github.com/mongodb/mongo-python-driver)
- Phoenix connector [phoenixdb](https://github.com/lalinsky/python-phoenixdb)
- ODPS connector [pyodps](https://github.com/aliyun/aliyun-odps-python-sdk)
- ClickHouse connector [clickhouse-driver](https://github.com/mymarilyn/clickhouse-driver)
- Cassandra connector [cassandra-driver](https://github.com/datastax/python-driver)
- Elasticsearch connector [elasticsearch-py](https://github.com/elastic/elasticsearch-py)
- OpenSearch connector [opensearch-py](https://github.com/opensearch-project/opensearch-py)
- Memcached connector [pymemcache](https://github.com/pinterest/pymemcache)
- Serialization [simplejson](https://github.com/simplejson/simplejson)
- Storage integrations [django-storages](https://github.com/jschneier/django-storages)
- AI query and metrics assistant [openai-python](https://github.com/openai/openai-python)

### Functional Dependencies
- Visualization [pyecharts](https://github.com/pyecharts/pyecharts)
- Large table DDL [gh-ost](https://github.com/github/gh-ost) | [pt-online-schema-change](https://www.percona.com/doc/percona-toolkit/3.0/pt-online-schema-change.html)
- MyBatis XML parsing [mybatis-mapper2sql](https://github.com/hhyo/mybatis-mapper2sql)
- Field encryption [cryptography](https://github.com/pyca/cryptography)
- MySQL data masking and query permission checks [goInception](https://github.com/hanchuanchuan/goInception)
- Observability [VictoriaMetrics](https://github.com/VictoriaMetrics/VictoriaMetrics), [Quickwit](https://github.com/quickwit-oss/quickwit), [Grafana](https://github.com/grafana/grafana), and [Jaeger](https://github.com/jaegertracing/jaeger)

Contributing
===============
You can check the roadmap and dependency list in this repository, claim related issues, or submit a PR directly. Thanks for contributing to Datamingle.

Contributions include but are not limited to:
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
