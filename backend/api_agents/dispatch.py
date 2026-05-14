import logging

from asgiref.sync import async_to_sync
from channels.exceptions import ChannelFull
from channels.layers import get_channel_layer

from api_agents.models import Agent

logger = logging.getLogger("default")

ACTIVE_WEBSOCKET_METADATA_KEY = "active_websocket"
WEBSOCKET_CHANNEL_METADATA_KEY = "channel_name"


def notify_config_changed(agent, reason="config.changed"):
    agent_id = agent.id if hasattr(agent, "id") else agent
    try:
        current_agent = Agent.objects.only(
            "id", "metadata", "desired_config_revision"
        ).get(pk=agent_id)
    except Agent.DoesNotExist:
        return False
    return send_agent_message(
        current_agent.id,
        {
            "type": "agent.config_changed",
            "revision": current_agent.desired_config_revision,
            "reason": reason,
        },
        agent=current_agent,
    )


def notify_command_available(command):
    return send_agent_message(
        command.agent_id,
        {
            "type": "agent.command_available",
            "command_id": command.id,
            "command_type": command.command_type,
        },
    )


def notify_command_cancel(command):
    return send_agent_message(
        command.agent_id,
        {
            "type": "agent.command_cancel",
            "command_id": command.id,
        },
    )


def send_agent_message(agent_id, message, agent=None):
    channel_layer = get_channel_layer()
    if channel_layer is None:
        return False
    channel_name = active_agent_channel_name(agent or agent_id)
    if not channel_name:
        return False
    try:
        async_to_sync(channel_layer.send)(channel_name, message)
    except (ChannelFull, ConnectionError, OSError) as exc:
        logger.warning(
            "Failed to send agent websocket message",
            extra={
                "agent_id": agent_id,
                "channel_name": channel_name,
                "error": str(exc),
            },
        )
        return False
    except Exception:
        logger.exception(
            "Unexpected error while sending agent websocket message",
            extra={"agent_id": agent_id, "channel_name": channel_name},
        )
        return False
    return True


def active_agent_channel_name(agent):
    if not hasattr(agent, "metadata"):
        try:
            agent = Agent.objects.only("metadata").get(pk=agent)
        except Agent.DoesNotExist:
            return ""
    active_websocket = dict(
        (agent.metadata or {}).get(ACTIVE_WEBSOCKET_METADATA_KEY) or {}
    )
    return active_websocket.get(WEBSOCKET_CHANNEL_METADATA_KEY, "")
