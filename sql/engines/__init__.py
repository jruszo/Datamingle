"""Engine base module with ``EngineBase`` class and ``get_engine`` function."""

import importlib
import re
from sql.engines.models import ResultSet, ReviewSet
from sql.models import Instance
from sql.utils.ssh_tunnel import SSHConnection
from django.conf import settings


class EngineBase:
    """EngineBase defines shared interface; concrete engines implement methods."""

    test_query = None

    name = "Base"
    info = "base engine"

    def __init__(self, instance: Instance = None):
        self.conn = None
        self.thread_id = None
        if instance:
            self.instance = instance  # type: Instance
            self.instance_name = instance.instance_name
            self.host = instance.host
            self.port = int(instance.port)
            self.user, self.password = self.instance.get_username_password()
            self.db_name = instance.db_name
            self.mode = instance.mode

            # If tunnel is configured, connect through SSH tunnel (tested with MySQL).
            if self.instance.tunnel:
                self.ssh = SSHConnection(
                    self.host,
                    self.port,
                    instance.tunnel.host,
                    instance.tunnel.port,
                    instance.tunnel.user,
                    instance.tunnel.password,
                    instance.tunnel.pkey,
                    instance.tunnel.pkey_password,
                )
                self.host, self.port = self.ssh.get_ssh()

    def __del__(self):
        if hasattr(self, "ssh"):
            del self.ssh
        if hasattr(self, "remotessh"):
            del self.remotessh

    def remote_instance_conn(self, instance=None):
        user, password = instance.get_username_password()
        # If tunnel is configured, connect through SSH tunnel.
        if not hasattr(self, "remotessh") and instance.tunnel:
            self.remotessh = SSHConnection(
                instance.host,
                instance.port,
                instance.tunnel.host,
                instance.tunnel.port,
                instance.tunnel.user,
                instance.tunnel.password,
                instance.tunnel.pkey,
                instance.tunnel.pkey_password,
            )
            self.remote_host, self.remote_port = self.remotessh.get_ssh()
            user, password = instance.get_username_password()
            self.remote_user = user
            self.remote_password = password
        elif not instance.tunnel:
            self.remote_host = instance.host
            self.remote_port = instance.port
            self.remote_user = user
            self.remote_password = password
        return (
            self.remote_host,
            self.remote_port,
            self.remote_user,
            self.remote_password,
        )

    def get_connection(self, db_name=None):
        """Return a connection instance."""

    def test_connection(self):
        """Test whether instance connection is available."""
        return self.query(sql=self.test_query)

    def escape_string(self, value: str) -> str:
        """Escape parameters."""
        return value

    @property
    def auto_backup(self):
        """Whether backup is supported."""
        return False

    @property
    def seconds_behind_master(self):
        """Replication lag in seconds."""
        return None

    @property
    def server_version(self):
        """Return engine server version as tuple (x, y, z)."""
        return tuple()

    def processlist(self, command_type, **kwargs) -> ResultSet:
        """Get connection information."""
        return ResultSet()

    def kill_connection(self, thread_id):
        """Terminate database connection."""

    def get_all_databases(self):
        """Get database list and return a ResultSet with rows=list."""
        return ResultSet()

    def get_all_tables(self, db_name, **kwargs):
        """Get table list and return a ResultSet with rows=list."""
        return ResultSet()

    def get_group_tables_by_db(self, db_name, **kwargs):
        """Get table list grouped by first character and return a dict."""
        return dict()

    def get_table_meta_data(self, db_name, tb_name, **kwargs):
        """Get table metadata."""
        return dict()

    def get_table_desc_data(self, db_name, tb_name, **kwargs):
        """Get table column details."""
        return dict()

    def get_table_index_data(self, db_name, tb_name, **kwargs):
        """Get table index information."""
        return dict()

    def get_tables_metas_data(self, db_name, **kwargs):
        """Get metadata of all tables in database for data dictionary export."""
        return list()

    def get_all_databases_summary(self):
        """Get summary of all databases for instance database management."""
        return ResultSet()

    def get_instance_users_summary(self):
        """Get summary of all accounts for instance user management."""
        return ResultSet()

    def create_instance_user(self, **kwargs):
        """Create instance account for instance user management."""
        return ResultSet()

    def drop_instance_user(self, **kwargs):
        """Delete instance account for instance user management."""
        return ResultSet()

    def reset_instance_user_pwd(self, **kwargs):
        """Reset instance account password for instance user management."""
        return ResultSet()

    def get_all_columns_by_tb(self, db_name, tb_name, **kwargs):
        """Get all columns and return a ResultSet with rows=list."""
        return ResultSet()

    def describe_table(self, db_name, tb_name, **kwargs):
        """Get table schema and return a ResultSet with rows=list."""
        return ResultSet()

    def query_check(self, db_name=None, sql=""):
        """Check query, strip comments, split SQL, and return check dict."""

    def filter_sql(self, sql="", limit_num=0):
        """Add row limit or rewrite query statement and return modified SQL."""
        return sql.strip()

    def query(
        self,
        db_name=None,
        sql="",
        limit_num=0,
        close_conn=True,
        parameters=None,
        **kwargs,
    ):
        """Execute query and return a ResultSet."""
        return ResultSet()

    def query_masking(self, db_name=None, sql="", resultset=None):
        """Input SQL, db name, and resultset; return masked resultset."""
        return resultset

    def execute_check(self, db_name=None, sql=""):
        """Validate execution statement and return a ReviewSet."""
        return ReviewSet()

    def execute(self, **kwargs):
        """Execute statement and return a ReviewSet."""
        return ReviewSet()

    def get_execute_percentage(self):
        """Get execution progress."""

    def get_rollback(self, workflow):
        """Get rollback SQL for workflow."""
        return list()

    def get_variables(self, variables=None):
        """Get instance variables and return a ResultSet."""
        return ResultSet()

    def set_variable(self, variable_name, variable_value):
        """Set instance variable and return a ResultSet."""
        return ResultSet()


def get_engine_map():
    available_engines = settings.AVAILABLE_ENGINES
    enabled_engines = {}
    for e in settings.ENABLED_ENGINES:
        config = available_engines.get(e)
        if not config:
            raise ValueError(f"invalid engine {e}, not found in engine map")
        module, o = config["path"].split(":")
        engine = getattr(importlib.import_module(module), o)
        enabled_engines[e] = engine
    return enabled_engines


engine_map = get_engine_map()


def get_engine(instance=None):  # pragma: no cover
    """Get database operation engine."""
    if instance.db_type == "mysql":
        from sql.models import AliyunRdsConfig

        if AliyunRdsConfig.objects.filter(instance=instance, is_enable=True).exists():
            from .cloud.aliyun_rds import AliyunRDS

            return AliyunRDS(instance=instance)
    engine = engine_map.get(instance.db_type)
    if not engine:
        raise ValueError(
            f"engine {instance.db_type} not enabled or not supported, please contact admin"
        )
    return engine(instance=instance)
