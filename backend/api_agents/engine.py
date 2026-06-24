import uuid

from common.config import SysConfig
from sql.engines import get_engine
from sql.engines.models import ResultSet
from api_agents.models import AgentCommandType
from api_agents.services import (
    AgentCommandDispatchError,
    AgentCommandExecutionError,
    result_set_from_agent_result,
    run_agent_command_sync,
)


def get_engine_via_agent(instance, submitted_by="system"):
    """Return an engine whose query() dispatches through the agent.

    All SQL generation still uses the concrete engine class (e.g. MySQL,
    PostgreSQL).  Only the execution path changes: ``engine.query()``
    sends the SQL to the agent via websocket instead of opening a direct
    database connection.
    """
    engine = get_engine(instance=instance)

    def _max_execution_ms():
        try:
            value = int(float(SysConfig().get("max_execution_time", 60)))
            return (value if value > 0 else 60) * 1000
        except (TypeError, ValueError):
            return 60_000

    def _timeout_seconds():
        try:
            value = int(float(SysConfig().get("max_execution_time", 60)))
            return value if value > 0 else 60
        except (TypeError, ValueError):
            return 60

    def agent_query(
        self,
        db_name=None,
        sql="",
        limit_num=0,
        close_conn=True,
        parameters=None,
        **kwargs,
    ):
        result_set = ResultSet(full_sql=sql)
        try:
            command = run_agent_command_sync(
                instance=instance,
                command_type=AgentCommandType.QUERY_EXECUTE,
                workflow_type="query.resource",
                workflow_id=f"{submitted_by}:{uuid.uuid4().hex}",
                payload={
                    "db_name": db_name or "",
                    "sql": sql,
                    "limit": limit_num,
                    "max_execution_time_ms": _max_execution_ms(),
                    "parameters": parameters or {},
                    "submitted_by": submitted_by,
                },
                timeout_seconds=_timeout_seconds(),
            )
            if not command.result:
                raise AgentCommandExecutionError(
                    "Agent command completed without producing a result.",
                    command=command,
                )
            result_set = result_set_from_agent_result(sql, command.result)
        except (AgentCommandDispatchError, AgentCommandExecutionError) as exc:
            result_set.error = str(exc)
        except Exception as exc:
            result_set.error = str(exc)
        return result_set

    engine.query = agent_query.__get__(engine, type(engine))
    return engine
