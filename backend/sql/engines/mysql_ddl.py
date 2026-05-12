import os
import re
import shutil
import subprocess
from dataclasses import dataclass, field

from common.utils.timer import FuncTimer
from common.config import SysConfig
from sql.engines.models import ReviewResult

ALTER_TABLE_RE = re.compile(
    r"^\s*alter\s+table\s+(?P<object>(?:`[^`]+`|[A-Za-z0-9_$]+)(?:\.(?:`[^`]+`|[A-Za-z0-9_$]+))?)\s+(?P<alter>.+?)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
RENAME_RE = re.compile(r"\brename\s+(?:to|as)\b", re.IGNORECASE)


@dataclass
class MysqlDDLStatement:
    sql: str
    db_name: str
    table_name: str | None = None
    alter_clause: str | None = None
    is_alter_table: bool = False


@dataclass
class MysqlDDLTableMetadata:
    db_name: str
    table_name: str
    engine: str = ""
    has_primary_key: bool = False
    has_non_nullable_unique_key: bool = False
    has_triggers: bool = False
    outbound_foreign_keys: list[str] = field(default_factory=list)
    inbound_foreign_keys: list[str] = field(default_factory=list)


@dataclass
class MysqlDDLRuntime:
    read_only: bool = False
    server_version: tuple[int | None, int | None, int | None] = tuple()
    server_fork_type: str = ""
    binlog_format: str = ""
    binlog_row_image: str = ""


@dataclass
class MysqlDDLExecutorChoice:
    id: str
    label: str
    kind: str


@dataclass
class MysqlDDLExecutorInspection:
    available_executors: list[MysqlDDLExecutorChoice]
    blockers: dict[str, str]
    read_only: bool

    @property
    def available_executor_ids(self):
        return [choice.id for choice in self.available_executors]


@dataclass
class MysqlDDLResolvedExecutor:
    executor_id: str
    label: str
    kind: str
    inspection: MysqlDDLExecutorInspection


class MysqlDDLExecutorError(RuntimeError):
    pass


def _normalize_identifier(identifier: str) -> str:
    return identifier.strip().strip("`").strip()


def _resolve_binary(path: str):
    if not path:
        return None

    candidate = path.strip()
    if not candidate:
        return None

    if os.path.isabs(candidate) or os.sep in candidate:
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
        return None

    return shutil.which(candidate)


def validate_binary_path(path: str, label: str):
    if not path:
        return path
    if _resolve_binary(path):
        return path
    raise ValueError(
        f"{label} binary must be empty or point to an executable file available on this server."
    )


def _version_at_least(version, minimum):
    normalized = []
    for index in range(3):
        value = version[index] if len(version) > index else 0
        normalized.append(value if isinstance(value, int) else 0)
    return tuple(normalized) >= tuple(minimum)


class BaseMysqlDDLExecutor:
    id = ""
    label = ""
    kind = "online"
    config_key = ""

    def __init__(self, engine, runtime: MysqlDDLRuntime):
        self.engine = engine
        self.runtime = runtime
        self.config = SysConfig()

    def is_configured(self):
        if not self.config_key:
            return True, ""

        configured_path = self.config.get(self.config_key, "")
        if not configured_path:
            return False, f"{self.label} binary is not configured."
        if not _resolve_binary(str(configured_path)):
            return (
                False,
                f"{self.label} binary is configured but not executable on the Datamingle server.",
            )
        return True, ""

    def inspect(self, statements, table_metadata):
        raise NotImplementedError

    def dry_run(self, workflow, statements):
        return None

    def execute(self, workflow, statements):
        raise NotImplementedError

    def _binary(self):
        path = self.config.get(self.config_key, "")
        resolved = _resolve_binary(str(path))
        if not resolved:
            raise MysqlDDLExecutorError(
                f"{self.label} binary is not available on the Datamingle server."
            )
        return resolved

    def _run_subprocess(self, cmd, statement, dry_run=False):
        with FuncTimer() as timer:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False,
            )
        output = (proc.stderr or proc.stdout or "").strip()
        if proc.returncode != 0:
            mode = "preflight" if dry_run else "execution"
            raise MysqlDDLExecutorError(
                f"{self.label} {mode} failed for `{statement.db_name}.{statement.table_name}`: {output or 'unknown error'}"
            )
        return timer.cost

    def _base_connection_args(self, db_name, table_name):
        args = [
            self._binary(),
            f"--host={self.engine.host}",
            f"--port={self.engine.port}",
            f"--user={self.engine.user}",
            f"--password={self.engine.password}",
        ]
        if self.engine.instance.charset:
            args.append(f"--charset={self.engine.instance.charset}")
        return args


class DirectMysqlDDLExecutor(BaseMysqlDDLExecutor):
    id = "direct"
    label = "Direct"
    kind = "direct"

    def inspect(self, statements, table_metadata):
        if self.runtime.read_only:
            return "Instance read_only=1, online execution is not allowed."
        if not statements:
            return "No executable SQL statements found."
        return ""

    def execute(self, workflow, statements):
        return None


class GhostMysqlDDLExecutor(BaseMysqlDDLExecutor):
    id = "gh-ost"
    label = "gh-ost"
    config_key = "gh_ost"

    def inspect(self, statements, table_metadata):
        if self.runtime.read_only:
            return "Instance read_only=1, online execution is not allowed."
        if self.runtime.server_fork_type == "mariadb":
            return "gh-ost does not support MariaDB."
        if self.runtime.server_version and not _version_at_least(
            self.runtime.server_version, (5, 7, 0)
        ):
            return "gh-ost requires MySQL 5.7 or newer."
        if str(self.runtime.binlog_format).upper() != "ROW":
            return "gh-ost requires binlog_format=ROW."
        if str(self.runtime.binlog_row_image).upper() != "FULL":
            return "gh-ost requires binlog_row_image=FULL."

        for statement in statements:
            if not statement.is_alter_table:
                return "gh-ost only supports ALTER TABLE statements."
            if RENAME_RE.search(statement.alter_clause or ""):
                return "gh-ost does not support ALTER TABLE ... RENAME."

            metadata = table_metadata.get((statement.db_name, statement.table_name))
            if metadata is None:
                return f"Failed to load metadata for `{statement.db_name}.{statement.table_name}`."
            if metadata.has_triggers:
                return "gh-ost does not support tables with triggers."
            if metadata.outbound_foreign_keys or metadata.inbound_foreign_keys:
                return "gh-ost does not support tables with foreign keys."
            if not (metadata.has_primary_key or metadata.has_non_nullable_unique_key):
                return "gh-ost requires a primary key or non-nullable unique key."
        return ""

    def _build_command(self, statement, execute=False):
        args = self._base_connection_args(statement.db_name, statement.table_name)
        args.extend(
            [
                f"--database={statement.db_name}",
                f"--table={statement.table_name}",
                f"--alter={statement.alter_clause}",
                "--allow-on-master",
                "--assume-rbr",
                "--exact-rowcount",
                "--initially-drop-ghost-table",
                "--initially-drop-old-table",
            ]
        )
        if execute:
            args.append("--execute")
        return args

    def dry_run(self, workflow, statements):
        for statement in statements:
            self._run_subprocess(
                self._build_command(statement, execute=False),
                statement,
                dry_run=True,
            )

    def execute(self, workflow, statements):
        rows = []
        for index, statement in enumerate(statements, start=1):
            execute_time = self._run_subprocess(
                self._build_command(statement, execute=True),
                statement,
                dry_run=False,
            )
            rows.append(
                ReviewResult(
                    id=index,
                    stage=self.label,
                    errlevel=0,
                    stagestatus="Execute Successfully",
                    errormessage="None",
                    sql=statement.sql,
                    affected_rows=0,
                    execute_time=execute_time,
                    executor=self.id,
                    table_name=statement.table_name,
                )
            )
        return rows


class PtOscMysqlDDLExecutor(BaseMysqlDDLExecutor):
    id = "pt-osc"
    label = "pt-online-schema-change"
    config_key = "pt_osc"

    def inspect(self, statements, table_metadata):
        if self.runtime.read_only:
            return "Instance read_only=1, online execution is not allowed."
        for statement in statements:
            if not statement.is_alter_table:
                return "pt-online-schema-change only supports ALTER TABLE statements."
            if RENAME_RE.search(statement.alter_clause or ""):
                return (
                    "pt-online-schema-change does not support ALTER TABLE ... RENAME."
                )

            metadata = table_metadata.get((statement.db_name, statement.table_name))
            if metadata is None:
                return f"Failed to load metadata for `{statement.db_name}.{statement.table_name}`."
            if metadata.has_triggers:
                return "pt-online-schema-change does not support tables that already have triggers."
            if not (metadata.has_primary_key or metadata.has_non_nullable_unique_key):
                return "pt-online-schema-change requires a primary key or unique key."
        return ""

    def _build_command(self, statement, execute=False):
        args = self._base_connection_args(statement.db_name, statement.table_name)
        args.extend(
            [
                f"--alter={statement.alter_clause}",
                "--alter-foreign-keys-method=auto",
                "--recursion-method=none",
            ]
        )
        args.append("--execute" if execute else "--dry-run")
        args.append(f"D={statement.db_name},t={statement.table_name}")
        return args

    def dry_run(self, workflow, statements):
        for statement in statements:
            self._run_subprocess(
                self._build_command(statement, execute=False),
                statement,
                dry_run=True,
            )

    def execute(self, workflow, statements):
        rows = []
        for index, statement in enumerate(statements, start=1):
            execute_time = self._run_subprocess(
                self._build_command(statement, execute=True),
                statement,
                dry_run=False,
            )
            rows.append(
                ReviewResult(
                    id=index,
                    stage=self.label,
                    errlevel=0,
                    stagestatus="Execute Successfully",
                    errormessage="None",
                    sql=statement.sql,
                    affected_rows=0,
                    execute_time=execute_time,
                    executor=self.id,
                    table_name=statement.table_name,
                )
            )
        return rows


class MysqlDDLExecutorService:
    def __init__(self, engine):
        self.engine = engine
        self.runtime = MysqlDDLRuntime(
            read_only=self._read_only(),
            server_version=engine.server_version,
            server_fork_type=getattr(
                engine.server_fork_type, "value", str(engine.server_fork_type)
            ),
            binlog_format=self._show_variable("binlog_format"),
            binlog_row_image=self._show_variable("binlog_row_image"),
        )
        self.executors = [
            DirectMysqlDDLExecutor(engine, self.runtime),
            GhostMysqlDDLExecutor(engine, self.runtime),
            PtOscMysqlDDLExecutor(engine, self.runtime),
        ]

    def inspect_workflow(self, workflow, statements):
        parsed_statements = [
            self._parse_statement(workflow.db_name, statement)
            for statement in statements
        ]
        metadata = self._load_statement_metadata(parsed_statements)

        available = []
        blockers = {}
        for executor in self.executors:
            configured, reason = executor.is_configured()
            if not configured:
                blockers[executor.id] = reason
                continue

            reason = executor.inspect(parsed_statements, metadata)
            if reason:
                blockers[executor.id] = reason
                continue

            available.append(
                MysqlDDLExecutorChoice(
                    id=executor.id,
                    label=executor.label,
                    kind=executor.kind,
                )
            )

        return MysqlDDLExecutorInspection(
            available_executors=available,
            blockers=blockers,
            read_only=self.runtime.read_only,
        )

    def resolve_executor(self, workflow, statements, requested_executor=None):
        inspection = self.inspect_workflow(workflow, statements)

        if not inspection.available_executors:
            raise MysqlDDLExecutorError(
                "No compatible MySQL DDL executors are available for this workflow."
            )

        if requested_executor:
            if requested_executor not in inspection.available_executor_ids:
                reason = inspection.blockers.get(
                    requested_executor,
                    "Requested executor is not compatible with this workflow.",
                )
                raise MysqlDDLExecutorError(reason)
            selected = next(
                executor
                for executor in self.executors
                if executor.id == requested_executor
            )
        elif len(inspection.available_executors) == 1:
            selected = next(
                executor
                for executor in self.executors
                if executor.id == inspection.available_executors[0].id
            )
        else:
            raise MysqlDDLExecutorError(
                "Multiple compatible executors are available. Select one explicitly."
            )

        return MysqlDDLResolvedExecutor(
            executor_id=selected.id,
            label=selected.label,
            kind=selected.kind,
            inspection=inspection,
        )

    def preflight(self, workflow, statements, resolved_executor):
        executor = self._executor_by_id(resolved_executor.executor_id)
        parsed_statements = [
            self._parse_statement(workflow.db_name, statement)
            for statement in statements
        ]
        if executor.id == "direct":
            return
        executor.dry_run(workflow, parsed_statements)

    def execute(self, workflow, statements, resolved_executor):
        executor = self._executor_by_id(resolved_executor.executor_id)
        parsed_statements = [
            self._parse_statement(workflow.db_name, statement)
            for statement in statements
        ]
        if executor.id == "direct":
            return None
        return executor.execute(workflow, parsed_statements)

    def _executor_by_id(self, executor_id):
        return next(
            executor for executor in self.executors if executor.id == executor_id
        )

    def _parse_statement(self, default_db_name, statement):
        normalized = statement.strip().rstrip(";")
        match = ALTER_TABLE_RE.match(normalized)
        if not match:
            return MysqlDDLStatement(sql=normalized, db_name=default_db_name)

        object_name = match.group("object")
        alter_clause = match.group("alter").strip()
        if "." in object_name:
            db_name, table_name = object_name.split(".", 1)
        else:
            db_name, table_name = default_db_name, object_name
        return MysqlDDLStatement(
            sql=normalized,
            db_name=_normalize_identifier(db_name),
            table_name=_normalize_identifier(table_name),
            alter_clause=alter_clause,
            is_alter_table=True,
        )

    def _load_statement_metadata(self, statements):
        metadata = {}
        for statement in statements:
            if (
                not statement.is_alter_table
                or (statement.db_name, statement.table_name) in metadata
            ):
                continue
            metadata[(statement.db_name, statement.table_name)] = (
                self._load_table_metadata(statement.db_name, statement.table_name)
            )
        return metadata

    def _load_table_metadata(self, db_name, table_name):
        table_info = self.engine.query(
            db_name="information_schema",
            sql="""
                SELECT ENGINE
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA=%(db_name)s AND TABLE_NAME=%(table_name)s
            """,
            parameters={"db_name": db_name, "table_name": table_name},
            close_conn=False,
        )
        index_info = self.engine.query(
            db_name="information_schema",
            sql="""
                SELECT s.INDEX_NAME, s.NON_UNIQUE, c.IS_NULLABLE
                FROM information_schema.STATISTICS s
                JOIN information_schema.COLUMNS c
                  ON c.TABLE_SCHEMA = s.TABLE_SCHEMA
                 AND c.TABLE_NAME = s.TABLE_NAME
                 AND c.COLUMN_NAME = s.COLUMN_NAME
                WHERE s.TABLE_SCHEMA=%(db_name)s
                  AND s.TABLE_NAME=%(table_name)s
                ORDER BY s.INDEX_NAME, s.SEQ_IN_INDEX
            """,
            parameters={"db_name": db_name, "table_name": table_name},
            close_conn=False,
        )
        trigger_info = self.engine.query(
            db_name="information_schema",
            sql="""
                SELECT TRIGGER_NAME
                FROM information_schema.TRIGGERS
                WHERE TRIGGER_SCHEMA=%(db_name)s
                  AND EVENT_OBJECT_TABLE=%(table_name)s
            """,
            parameters={"db_name": db_name, "table_name": table_name},
            close_conn=False,
        )
        outbound_fk_info = self.engine.query(
            db_name="information_schema",
            sql="""
                SELECT CONSTRAINT_NAME
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA=%(db_name)s
                  AND TABLE_NAME=%(table_name)s
                  AND REFERENCED_TABLE_NAME IS NOT NULL
            """,
            parameters={"db_name": db_name, "table_name": table_name},
            close_conn=False,
        )
        inbound_fk_info = self.engine.query(
            db_name="information_schema",
            sql="""
                SELECT CONCAT(TABLE_SCHEMA, '.', TABLE_NAME, '.', CONSTRAINT_NAME)
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE REFERENCED_TABLE_SCHEMA=%(db_name)s
                  AND REFERENCED_TABLE_NAME=%(table_name)s
            """,
            parameters={"db_name": db_name, "table_name": table_name},
            close_conn=False,
        )

        indexes = {}
        for index_name, non_unique, is_nullable in index_info.rows:
            indexes.setdefault(index_name, {"non_unique": non_unique, "nullable": []})
            indexes[index_name]["nullable"].append(str(is_nullable).upper() == "YES")

        has_primary_key = "PRIMARY" in indexes
        has_non_nullable_unique_key = any(
            index_name != "PRIMARY"
            and int(index_data["non_unique"]) == 0
            and not any(index_data["nullable"])
            for index_name, index_data in indexes.items()
        )

        return MysqlDDLTableMetadata(
            db_name=db_name,
            table_name=table_name,
            engine=table_info.rows[0][0] if table_info.rows else "",
            has_primary_key=has_primary_key,
            has_non_nullable_unique_key=has_non_nullable_unique_key,
            has_triggers=bool(trigger_info.rows),
            outbound_foreign_keys=[row[0] for row in outbound_fk_info.rows],
            inbound_foreign_keys=[row[0] for row in inbound_fk_info.rows],
        )

    def _show_variable(self, variable_name):
        result = self.engine.query(
            sql=f"SHOW VARIABLES LIKE '{variable_name}';",
            close_conn=False,
        )
        if result.rows:
            return result.rows[0][1]
        return ""

    def _read_only(self):
        result = self.engine.query(sql="SELECT @@global.read_only;", close_conn=False)
        if result.rows:
            return result.rows[0][0] in (1, "1", "ON", "on", True)
        return False
