from channels.db import database_sync_to_async
from channels.generic.websocket import AsyncJsonWebsocketConsumer
from django.utils import timezone

from api_agents.dispatch import (
    ACTIVE_WEBSOCKET_METADATA_KEY,
    WEBSOCKET_CHANNEL_METADATA_KEY,
)
from api_agents.models import Agent
from api_agents.models import AgentCommand
from api_agents.services import AgentAPIKeyRejected, authenticate_agent_api_key


class AgentConsumer(AsyncJsonWebsocketConsumer):
    async def connect(self):
        api_key = self._authorization_bearer()
        if not api_key:
            await self.close(code=4401)
            return

        try:
            self.agent = await database_sync_to_async(authenticate_agent_api_key)(
                api_key
            )
        except AgentAPIKeyRejected:
            await self.close(code=4403)
            return

        if self.agent is None:
            await self.close(code=4401)
            return

        await self._mark_connected()
        await self.accept()
        await self.send_json(
            {
                "type": "hello.ack",
                "agent_id": self.agent.id,
                "desired_config_revision": self.agent.desired_config_revision,
            }
        )

    async def disconnect(self, code):
        if hasattr(self, "agent"):
            await self._mark_disconnected()

    async def receive_json(self, content, **kwargs):
        message_type = content.get("type")
        if message_type == "hello":
            await self.send_json(
                {
                    "type": "hello.ack",
                    "agent_id": self.agent.id,
                    "desired_config_revision": self.agent.desired_config_revision,
                }
            )
        elif message_type == "pong":
            await self._store_metadata("last_pong", content)
        elif message_type == "config.applied":
            await self._mark_config_applied(
                content.get("revision", 0), content.get("config_hash", "")
            )
        elif message_type == "module.status":
            await self._store_module_status(content)
        elif message_type == "command.progress":
            await self._append_command_progress(content)

    async def agent_config_changed(self, event):
        await self.send_json(
            {
                "type": "config.changed",
                "revision": event["revision"],
                "reason": event.get("reason", "config.changed"),
            }
        )

    async def agent_command_available(self, event):
        await self.send_json(
            {
                "type": "command.available",
                "command_id": event["command_id"],
                "command_type": event["command_type"],
            }
        )

    async def agent_command_cancel(self, event):
        await self.send_json(
            {
                "type": "command.cancel",
                "command_id": event["command_id"],
            }
        )

    def _authorization_bearer(self):
        headers = dict(self.scope.get("headers") or [])
        try:
            value = headers.get(b"authorization", b"").decode("utf-8")
        except UnicodeDecodeError:
            return ""
        parts = value.split()
        if len(parts) == 2 and parts[0].lower() == "bearer":
            return parts[1]
        return ""

    @database_sync_to_async
    def _mark_connected(self):
        now = timezone.now()
        self.agent.status = "online"
        self.agent.last_connected_at = now
        self.agent.last_seen_at = now
        metadata = dict(self.agent.metadata or {})
        metadata[ACTIVE_WEBSOCKET_METADATA_KEY] = {
            WEBSOCKET_CHANNEL_METADATA_KEY: self.channel_name,
            "connected_at": now.isoformat(),
        }
        self.agent.metadata = metadata
        self.agent.save(
            update_fields=[
                "status",
                "last_connected_at",
                "last_seen_at",
                "metadata",
                "update_time",
            ]
        )

    @database_sync_to_async
    def _mark_disconnected(self):
        agent = Agent.objects.only("metadata", "last_disconnected_at").get(
            pk=self.agent.pk
        )
        metadata = dict(agent.metadata or {})
        active_websocket = dict(metadata.get(ACTIVE_WEBSOCKET_METADATA_KEY) or {})
        if active_websocket.get(WEBSOCKET_CHANNEL_METADATA_KEY) == self.channel_name:
            metadata.pop(ACTIVE_WEBSOCKET_METADATA_KEY, None)
        agent.metadata = metadata
        agent.last_disconnected_at = timezone.now()
        agent.save(update_fields=["metadata", "last_disconnected_at", "update_time"])

    @database_sync_to_async
    def _mark_config_applied(self, revision, config_hash):
        try:
            revision = int(revision)
        except (TypeError, ValueError):
            revision = 0
        agent = Agent.objects.only(
            "metadata", "last_config_revision", "last_seen_at"
        ).get(pk=self.agent.pk)
        metadata = dict(agent.metadata or {})
        metadata["last_config_hash"] = config_hash
        now = timezone.now()
        agent.last_config_revision = revision
        agent.metadata = metadata
        agent.last_seen_at = now
        agent.save(
            update_fields=[
                "last_config_revision",
                "metadata",
                "last_seen_at",
                "update_time",
            ]
        )
        self.agent.last_config_revision = revision
        self.agent.metadata = metadata
        self.agent.last_seen_at = now

    @database_sync_to_async
    def _store_metadata(self, key, value):
        agent = Agent.objects.only("metadata", "last_seen_at").get(pk=self.agent.pk)
        metadata = dict(agent.metadata or {})
        metadata[key] = value
        now = timezone.now()
        agent.metadata = metadata
        agent.last_seen_at = now
        agent.save(update_fields=["metadata", "last_seen_at", "update_time"])
        self.agent.metadata = metadata
        self.agent.last_seen_at = now

    @database_sync_to_async
    def _store_module_status(self, content):
        agent = Agent.objects.only("metadata", "last_seen_at").get(pk=self.agent.pk)
        metadata = dict(agent.metadata or {})
        statuses = dict(metadata.get("module_status") or {})
        module_name = content.get("module")
        now = timezone.now()
        if module_name:
            statuses[module_name] = {
                "status": content.get("status", ""),
                "message": content.get("message", ""),
                "updated_at": now.isoformat(),
            }
        metadata["module_status"] = statuses
        agent.metadata = metadata
        agent.last_seen_at = now
        agent.save(update_fields=["metadata", "last_seen_at", "update_time"])
        self.agent.metadata = metadata
        self.agent.last_seen_at = now

    @database_sync_to_async
    def _append_command_progress(self, content):
        command_id = content.get("command_id")
        if not command_id:
            return
        try:
            command = AgentCommand.objects.get(id=command_id, agent=self.agent)
        except AgentCommand.DoesNotExist:
            return
        command.append_event(
            "command.progress",
            content.get("message", ""),
            payload=content,
        )
