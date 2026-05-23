import uuid

from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import models, transaction
from django.utils import timezone

from sql.models import InfrastructureNode, Instance


class AgentStatus(models.TextChoices):
    PENDING = "pending", "Pending"
    ONLINE = "online", "Online"
    OFFLINE = "offline", "Offline"
    DISABLED = "disabled", "Disabled"
    REVOKED = "revoked", "Revoked"


class Agent(models.Model):
    organization_id = models.CharField(max_length=128, blank=True, default="")
    name = models.CharField(max_length=128, unique=True)
    display_name = models.CharField(max_length=200, blank=True, default="")
    status = models.CharField(
        max_length=20,
        choices=AgentStatus.choices,
        default=AgentStatus.PENDING,
        db_index=True,
    )
    workos_api_key_id = models.CharField(
        max_length=128, blank=True, default="", db_index=True
    )
    api_key_prefix = models.CharField(max_length=32, blank=True, default="")
    api_key_hash = models.CharField(
        max_length=64, blank=True, default=None, null=True, db_index=True, unique=True
    )
    hostname = models.CharField(max_length=255, blank=True, default="")
    platform = models.CharField(max_length=64, blank=True, default="")
    architecture = models.CharField(max_length=64, blank=True, default="")
    agent_version = models.CharField(max_length=64, blank=True, default="")
    install_id = models.CharField(
        max_length=80, blank=True, default=None, unique=True, null=True
    )
    last_seen_at = models.DateTimeField(null=True, blank=True)
    last_connected_at = models.DateTimeField(null=True, blank=True)
    last_disconnected_at = models.DateTimeField(null=True, blank=True)
    last_config_revision = models.PositiveIntegerField(default=0)
    desired_config_revision = models.PositiveIntegerField(default=1)
    enabled = models.BooleanField(default=True)
    metadata = models.JSONField(default=dict, blank=True)
    local_node = models.ForeignKey(
        InfrastructureNode,
        related_name="local_agents",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
    )
    create_time = models.DateTimeField(auto_now_add=True)
    update_time = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.display_name or self.name

    @property
    def can_connect(self):
        return self.enabled and self.status not in {
            AgentStatus.DISABLED,
            AgentStatus.REVOKED,
        }

    def mark_seen(self, status=AgentStatus.ONLINE, config_revision=None):
        self.status = status
        self.last_seen_at = timezone.now()
        update_fields = ["status", "last_seen_at", "update_time"]
        if config_revision is not None:
            self.last_config_revision = config_revision
            update_fields.append("last_config_revision")
        self.save(update_fields=update_fields)

    def bump_desired_config_revision(self, summary=None, created_by=None):
        summary = summary or {}
        with transaction.atomic():
            locked = Agent.objects.select_for_update().get(pk=self.pk)
            locked.desired_config_revision += 1
            locked.save(update_fields=["desired_config_revision", "update_time"])
            AgentConfigRevision.objects.create(
                agent=locked,
                revision=locked.desired_config_revision,
                summary=summary,
                created_by=created_by,
            )
            self.desired_config_revision = locked.desired_config_revision
        return self.desired_config_revision

    class Meta:
        db_table = "agent"
        ordering = ("name",)
        permissions = (("menu_agent", "Can access Agents menu"),)


class AgentNodeAssignment(models.Model):
    agent = models.ForeignKey(
        Agent, related_name="node_assignments", on_delete=models.CASCADE
    )
    node = models.ForeignKey(
        InfrastructureNode, related_name="agent_assignments", on_delete=models.CASCADE
    )
    enabled = models.BooleanField(default=True)
    modules = models.JSONField(default=list, blank=True)
    capabilities = models.JSONField(default=list, blank=True)
    command_enabled = models.BooleanField(default=False)
    active_command_node_id = models.PositiveIntegerField(
        null=True, blank=True, unique=True, editable=False
    )
    metrics_enabled = models.BooleanField(default=True)
    online_schema_enabled = models.BooleanField(default=False)
    logs_enabled = models.BooleanField(default=False)
    create_time = models.DateTimeField(auto_now_add=True)
    update_time = models.DateTimeField(auto_now=True)

    def clean(self):
        super().clean()
        self.active_command_node_id = (
            self.node_id if self.enabled and self.command_enabled else None
        )
        if self.enabled and self.command_enabled:
            duplicate = AgentNodeAssignment.objects.filter(
                node=self.node,
                enabled=True,
                command_enabled=True,
            )
            if self.pk:
                duplicate = duplicate.exclude(pk=self.pk)
            if duplicate.exists():
                raise ValidationError(
                    {
                        "command_enabled": (
                            "This node already has an active command-capable "
                            "remote manager assignment."
                        )
                    }
                )

    def save(self, *args, **kwargs):
        self.full_clean()
        from api_agents.services import sync_node_assignment_to_services

        with transaction.atomic():
            super().save(*args, **kwargs)
            sync_node_assignment_to_services(self)

    def delete(self, *args, **kwargs):
        from api_agents.services import clear_node_assignment_from_services

        with transaction.atomic():
            clear_node_assignment_from_services(self)
            return super().delete(*args, **kwargs)

    class Meta:
        db_table = "agent_node_assignment"
        ordering = ("agent_id", "node_id")
        unique_together = ("agent", "node")


class AgentInstanceAssignment(models.Model):
    agent = models.ForeignKey(
        Agent, related_name="assignments", on_delete=models.CASCADE
    )
    instance = models.ForeignKey(
        Instance, related_name="agent_assignments", on_delete=models.CASCADE
    )
    node_assignment = models.ForeignKey(
        AgentNodeAssignment,
        related_name="instance_assignments",
        null=True,
        blank=True,
        on_delete=models.CASCADE,
    )
    local_node = models.ForeignKey(
        InfrastructureNode,
        related_name="local_agent_instance_assignments",
        null=True,
        blank=True,
        on_delete=models.CASCADE,
    )
    enabled = models.BooleanField(default=True)
    modules = models.JSONField(default=list, blank=True)
    capabilities = models.JSONField(default=list, blank=True)
    command_enabled = models.BooleanField(default=False)
    active_command_instance_id = models.PositiveIntegerField(
        null=True, blank=True, unique=True, editable=False
    )
    metrics_enabled = models.BooleanField(default=True)
    online_schema_enabled = models.BooleanField(default=False)
    logs_enabled = models.BooleanField(default=False)
    create_time = models.DateTimeField(auto_now_add=True)
    update_time = models.DateTimeField(auto_now=True)

    def clean(self):
        super().clean()
        self.active_command_instance_id = (
            self.instance_id if self.enabled and self.command_enabled else None
        )
        if self.enabled and self.command_enabled:
            duplicate = AgentInstanceAssignment.objects.filter(
                instance=self.instance,
                enabled=True,
                command_enabled=True,
            )
            if self.pk:
                duplicate = duplicate.exclude(pk=self.pk)
            if duplicate.exists():
                raise ValidationError(
                    {
                        "command_enabled": (
                            "This instance already has an active command-capable "
                            "agent assignment."
                        )
                    }
                )

    def save(self, *args, **kwargs):
        self.full_clean()
        with transaction.atomic():
            super().save(*args, **kwargs)
            self.agent.bump_desired_config_revision(
                summary={
                    "action": "assignment.saved",
                    "assignment_id": self.pk,
                    "instance_id": self.instance_id,
                }
            )
            transaction.on_commit(
                lambda: self._notify_config_changed("assignment.saved")
            )

    def delete(self, *args, **kwargs):
        agent = self.agent
        instance_id = self.instance_id
        with transaction.atomic():
            result = super().delete(*args, **kwargs)
            agent.bump_desired_config_revision(
                summary={"action": "assignment.deleted", "instance_id": instance_id}
            )
            transaction.on_commit(
                lambda: self._notify_config_changed("assignment.deleted", agent=agent)
            )
        return result

    def _notify_config_changed(self, reason, agent=None):
        from api_agents.dispatch import notify_config_changed

        notify_config_changed(agent or self.agent, reason=reason)

    class Meta:
        db_table = "agent_instance_assignment"
        ordering = ("agent_id", "instance_id")
        unique_together = ("agent", "instance")


class AgentConfigRevision(models.Model):
    agent = models.ForeignKey(
        Agent, related_name="config_revisions", on_delete=models.CASCADE
    )
    revision = models.PositiveIntegerField()
    config_hash = models.CharField(max_length=64, blank=True, default="")
    summary = models.JSONField(default=dict, blank=True)
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="agent_config_revisions",
    )
    create_time = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.agent_id}:{self.revision}"

    class Meta:
        db_table = "agent_config_revision"
        ordering = ("-revision",)
        unique_together = ("agent", "revision")


class AgentCommandStatus(models.TextChoices):
    QUEUED = "queued", "Queued"
    DISPATCHED = "dispatched", "Dispatched"
    ACCEPTED = "accepted", "Accepted"
    RUNNING = "running", "Running"
    SUCCEEDED = "succeeded", "Succeeded"
    FAILED = "failed", "Failed"
    CANCELLED = "cancelled", "Cancelled"
    EXPIRED = "expired", "Expired"


class AgentCommandType(models.TextChoices):
    QUERY_EXECUTE = "query.execute", "Query Execute"
    SCHEMA_CHANGE = "schema.change", "Schema Change"
    CONNECTION_TEST = "connection.test", "Connection Test"


class AgentCommand(models.Model):
    agent = models.ForeignKey(Agent, related_name="commands", on_delete=models.CASCADE)
    instance = models.ForeignKey(
        Instance, related_name="agent_commands", on_delete=models.CASCADE
    )
    workflow_type = models.CharField(max_length=64)
    workflow_id = models.CharField(max_length=128)
    command_type = models.CharField(
        max_length=40, choices=AgentCommandType.choices, db_index=True
    )
    status = models.CharField(
        max_length=20,
        choices=AgentCommandStatus.choices,
        default=AgentCommandStatus.QUEUED,
        db_index=True,
    )
    idempotency_key = models.CharField(
        max_length=128, unique=True, null=True, blank=True, default=None
    )
    payload = models.JSONField(default=dict, blank=True)
    result = models.JSONField(default=dict, blank=True)
    error = models.JSONField(default=dict, blank=True)
    lease_owner = models.CharField(max_length=128, blank=True, default="")
    lease_expires_at = models.DateTimeField(null=True, blank=True)
    cancel_requested_at = models.DateTimeField(null=True, blank=True)
    queued_at = models.DateTimeField(default=timezone.now)
    dispatched_at = models.DateTimeField(null=True, blank=True)
    accepted_at = models.DateTimeField(null=True, blank=True)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    create_time = models.DateTimeField(auto_now_add=True)
    update_time = models.DateTimeField(auto_now=True)

    def append_event(self, event_type, message="", payload=None):
        return AgentCommandEvent.objects.create(
            command=self,
            event_type=event_type,
            message=message,
            payload=payload or {},
        )

    def save(self, *args, **kwargs):
        if not self.idempotency_key:
            self.idempotency_key = f"agent_command:{uuid.uuid4().hex}"
        super().save(*args, **kwargs)

    class Meta:
        db_table = "agent_command"
        ordering = ("-create_time",)
        indexes = (
            models.Index(fields=("agent", "status")),
            models.Index(fields=("instance", "status")),
        )


class AgentCommandEvent(models.Model):
    command = models.ForeignKey(
        AgentCommand, related_name="events", on_delete=models.CASCADE
    )
    event_type = models.CharField(max_length=64)
    message = models.TextField(blank=True, default="")
    payload = models.JSONField(default=dict, blank=True)
    create_time = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "agent_command_event"
        ordering = ("create_time", "id")


class AgentToolArtifact(models.Model):
    TOOL_GHOST = "gh-ost"
    TOOL_PT_OSC = "pt-online-schema-change"
    TOOL_CHOICES = (
        (TOOL_GHOST, "gh-ost"),
        (TOOL_PT_OSC, "pt-online-schema-change"),
    )

    tool_name = models.CharField(max_length=64, choices=TOOL_CHOICES)
    version = models.CharField(max_length=64)
    platform = models.CharField(max_length=64)
    architecture = models.CharField(max_length=64)
    download_url = models.URLField(max_length=1024)
    sha256 = models.CharField(max_length=64, blank=True, default="")
    size_bytes = models.PositiveBigIntegerField(default=0)
    enabled = models.BooleanField(default=False)
    notes = models.TextField(blank=True, default="")
    create_time = models.DateTimeField(auto_now_add=True)
    update_time = models.DateTimeField(auto_now=True)

    def clean(self):
        super().clean()
        if self.enabled and not self.sha256:
            raise ValidationError({"sha256": "Enabled artifacts require a SHA256."})
        if self.sha256 and len(self.sha256) != 64:
            raise ValidationError(
                {"sha256": "SHA256 must be 64 hexadecimal characters."}
            )

    def save(self, *args, **kwargs):
        self.full_clean()
        super().save(*args, **kwargs)

    class Meta:
        db_table = "agent_tool_artifact"
        ordering = ("tool_name", "version", "platform", "architecture")
        unique_together = ("tool_name", "version", "platform", "architecture")
