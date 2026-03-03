# Engine Notes

## Cassandra
The current connection setup mostly uses hardcoded parameters. See the code for details.

If you need to override this behavior, create a custom subclass.

Steps:
1. Add an `extras` directory at the project root (same level as `sql`, `sql_api`, etc.). You can include it in Docker build context or mount it as a volume.
2. Add a new file: `mycassandra.py`
```python
from sql.engines.cassandra import CassandraEngine

class MyCassandraEngine(CassandraEngine):
    def get_connection(self, db_name=None):
        db_name = db_name or self.db_name
        if self.conn:
            if db_name:
                self.conn.execute(f"use {db_name}")
            return self.conn
        hosts = self.host.split(",")
        # Customize how the session is created here.
        auth_provider = PlainTextAuthProvider(
            username=self.user, password=self.password
        )
        cluster = Cluster(hosts, port=self.port, auth_provider=auth_provider,
                          load_balancing_policy=RoundRobinPolicy(), protocol_version=5)
        self.conn = cluster.connect(keyspace=db_name)
        # Keep the following line unchanged if possible.
        self.conn.row_factory = tuple_factory
        return self.conn
```
3. Update settings to load your custom engine:
```python
AVAILABLE_ENGINES = {
    "mysql": {"path": "sql.engines.mysql:MysqlEngine"},
    # Replace this with your custom engine.
    "cassandra": {"path": "extras.mycassandra:MyCassandraEngine"},
    "clickhouse": {"path": "sql.engines.clickhouse:ClickHouseEngine"},
    "goinception": {"path": "sql.engines.goinception:GoInceptionEngine"},
    "mssql": {"path": "sql.engines.mssql:MssqlEngine"},
    "redis": {"path": "sql.engines.redis:RedisEngine"},
    "pqsql": {"path": "sql.engines.pgsql:PgSQLEngine"},
    "oracle": {"path": "sql.engines.oracle:OracleEngine"},
    "mongo": {"path": "sql.engines.mongo:MongoEngine"},
    "phoenix": {"path": "sql.engines.phoenix:PhoenixEngine"},
    "odps": {"path": "sql.engines.odps:ODPSEngine"},
}
```
