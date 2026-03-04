<div align="center">

[![Repository](https://img.shields.io/badge/GitHub-jruszo%2FDatamingle-181717?logo=github)](https://github.com/jruszo/Datamingle)
[![version](https://img.shields.io/pypi/pyversions/django)](https://img.shields.io/pypi/pyversions/django/)
[![version](https://img.shields.io/badge/django-4.1-brightgreen.svg)](https://docs.djangoproject.com/en/4.1/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# Datamingle
<h4>SQL Review and Query Platform</h4>

[Repository](https://github.com/jruszo/Datamingle)

![](https://github.com/hhyo/Archery/wiki/images/dashboard.png)

</div>

Fork Attribution
===============
Datamingle is a fork of [Archery](https://github.com/hhyo/Archery), and we retain attribution in accordance with the Apache-2.0 license.
Original project copyright and license notices are preserved in this repository.

Feature Matrix
====

| Database   | Query | Review | Execute | Backup | Data Dictionary | Slow Log | Session Management | Account Management | Parameter Management | Data Archive |
|------------| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| MySQL      | √ | √ | √ | √ | √ | √ | √ | √ | √ | √ |
| MsSQL      | √ | × | √ | × | √ | × | × | × | × | × |
| Redis      | √ | × | √ | × | × | × | × | × | × | × |
| PgSQL      | √ | × | √ | × | × | × | × | × | × | × |
| Oracle     | √ | √ | √ | √ | √ | × | √ | × | × | × |
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
Use the Docker and compose files in this repository (`src/docker` and `src/docker-compose`).

Manual Installation
===============
Use this repository as the source of truth: https://github.com/jruszo/Datamingle

Run Tests
===============
```bash
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
- Queue tasks [django-q](https://github.com/Koed00/django-q)
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
- MySQL binlog parse/rollback [python-mysql-replication](https://github.com/noplay/python-mysql-replication)
- LDAP [django-auth-ldap](https://github.com/django-auth-ldap/django-auth-ldap)
- Serialization [simplejson](https://github.com/simplejson/simplejson)
- Time utilities [python-dateutil](https://github.com/paxan/python-dateutil)

### Functional Dependencies
- Visualization [pyecharts](https://github.com/pyecharts/pyecharts)
- MySQL review/execute/backup [goInception](https://github.com/hanchuanchuan/goInception) | [inception](https://github.com/hhyo/inception)
- MySQL index optimization [SQLAdvisor](https://github.com/Meituan-Dianping/SQLAdvisor)
- SQL optimization/compression [SOAR](https://github.com/XiaoMi/soar)
- My2SQL [my2sql](https://github.com/liuhr/my2sql)
- Schema sync [SchemaSync](https://github.com/hhyo/SchemaSync)
- Slow log parsing and display [pt-query-digest](https://www.percona.com/doc/percona-toolkit/3.0/pt-query-digest.html) | [aquila_v2](https://github.com/thinkdb/aquila_v2)
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
