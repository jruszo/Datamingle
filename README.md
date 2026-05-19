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

- `backend/` contains the Django API, backend Docker files, Helm chart, and backend Python dependencies.
- `frontend/` contains the Vue/Vite SPA.
- `shared-infra/` contains the local shared observability stack for Cortex, Quickwit, Grafana, Jaeger, Prometheus, and MinIO.
- `agent/` is reserved for the upcoming Datamingle agent module.
- `scripts/` contains repo-level helper scripts.

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

### Docker
Use the Docker and compose files in this repository (`backend/src/docker` and `backend/src/docker-compose`).

The shared observability infrastructure lives outside the app stack in
`shared-infra/`. It is a dedicated local stack for tenant-shared services used
to test metrics and trace ingestion:

```bash
docker-compose -f shared-infra/docker-compose.yml up -d
```

See [shared-infra/README.md](shared-infra/README.md) for service URLs, tenant
headers, and reset instructions.

Existing local Docker data directories from before the Datamingle rename may
still have the application database under the old `archery` name. Copy that data
into the new `datamingle` database before starting the renamed stack:

```bash
scripts/docker/migrate-archery-db-to-datamingle.sh
```

The script defaults to the local ARM MySQL container, `datamingle-mysql`. If you
are using `backend/src/docker-compose/docker-compose.yml`, where the MySQL container is
named `mysql`, override the container name:

```bash
MYSQL_CONTAINER=mysql scripts/docker/migrate-archery-db-to-datamingle.sh
```

### Local Demo Seed
The local ARM compose setup seeds resource groups, auth groups, workflow settings, instance tags, and demo database instances for manual UX testing.

It no longer seeds local app users. Sign-in and privileged access come from WorkOS.

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
Datamingle uses WorkOS as the only sign-in method for every deployment. Each running Datamingle instance maps to one fixed WorkOS organization.

```env
WORKOS_API_KEY=sk_test_or_live_xxx
WORKOS_CLIENT_ID=client_xxx
WORKOS_ORGANIZATION_ID=org_xxx
WORKOS_WEBHOOK_SECRET=whsec_xxx
WORKOS_STAFF_EMAILS=ops@datamingle.dev,admin@datamingle.dev
WORKOS_SUPERUSER_EMAILS=admin@datamingle.dev
WORKOS_SUPERADMIN_ROLE_SLUGS=superadmin
```

Behavior:

- The SPA login page shows only the WorkOS button.
- Local password login, user-created passwords, and local 2FA login routes are not registered.
- Datamingle still keeps its own local `Users`, auth/permission groups, and resource groups after login.
- WorkOS Directory Sync mirrors directory groups into Datamingle resource groups and owns resource-group membership for directory-managed users.
- Superusers manage fallback Datamingle resource-group assignments for customers without SSO/Directory Sync and can still change active/inactive state.
- Auth groups remain permission/role levels used inside resource groups. Resource access is assigned through Datamingle resource groups. Access requests can grant temporary access to an individual user or a resource group; approved permanent requests add the user to a resource group or attach an instance to a resource group.

WorkOS setup assumptions in this repo:

- One Datamingle deployment maps to one tenant.
- That deployment uses one fixed WorkOS organization via `WORKOS_ORGANIZATION_ID`.
- Users are provisioned just-in-time on first successful WorkOS login.
- Directory Sync users can also be created or updated from WorkOS webhooks.
- The local Datamingle account uses the WorkOS email as the username.
- Users with a WorkOS role slug listed in `WORKOS_SUPERADMIN_ROLE_SLUGS` are refreshed into Datamingle superuser access on every login, including membership in the local `superadmin` auth group.
- `WORKOS_STAFF_EMAILS` and `WORKOS_SUPERUSER_EMAILS` remain bootstrap allowlists for initial elevated access.
- Datamingle derives the WorkOS callback and logout return URLs from the current request host.
- Directory Sync group names auto-create matching Datamingle resource groups. Assign database servers to those resource groups in Datamingle.

### WorkOS Setup Steps
1. Create or choose the tenant organization in your WorkOS account.
2. Configure the Datamingle application redirect URI in WorkOS:
   `https://<tenant-host>/api/auth/workos/callback/`
3. Configure the logout return URL in WorkOS:
   `https://<tenant-host>/login`
   For local Vite development, use:
   - Redirect URI: `http://localhost:5173/api/auth/workos/callback/`
   - Logout return URL: `http://localhost:5173/login`
4. Set the required `WORKOS_*` values in `.env`.
5. Configure the WorkOS webhook endpoint:
   `https://<tenant-host>/api/auth/workos/webhook/`
   Include Directory Sync events. After creating the endpoint, copy its signing
   secret from the WorkOS Dashboard and set it in `.env` as
   `WORKOS_WEBHOOK_SECRET` before continuing.
6. Optionally backfill an existing directory after setup:

```bash
docker exec -w /opt/datamingle/backend datamingle-app python manage.py sync_workos_directory --directory-id directory_xxx
```

Use the directory ID from the WorkOS Dashboard, or fetch it through the WorkOS
API, in place of `directory_xxx`.

7. Rebuild the app container so the `workos` Python dependency is installed:

```bash
docker-compose -f backend/src/docker-compose/docker-compose.local-dev.yml up -d --build datamingle frontend
```

8. Restart the deployment and open `/login/`.
9. Sign in through WorkOS. A local Datamingle user will be created automatically on first login.

### Notes
- WorkOS still issues Datamingle JWTs to the SPA after the WorkOS callback completes.
- Existing local users may remain for permissions and audit history, but sign-in must come through WorkOS.

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
- WorkOS auth [workos-python](https://github.com/workos/workos-python)
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
