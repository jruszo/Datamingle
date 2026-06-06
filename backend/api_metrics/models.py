from django.conf import settings
from django.db import models
from django.utils import timezone


class MetricsDashboard(models.Model):
    organization_id = models.CharField(max_length=128, db_index=True)
    name = models.CharField(max_length=120)
    description = models.CharField(max_length=500, blank=True, default="")
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        related_name="metrics_dashboards",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
    )
    revision = models.PositiveIntegerField(default=1)
    time_range_seconds = models.PositiveIntegerField(default=3600)
    refresh_interval_seconds = models.PositiveIntegerField(default=0)
    variables = models.JSONField(default=list, blank=True)
    panels = models.JSONField(default=list, blank=True)
    create_time = models.DateTimeField(auto_now_add=True)
    update_time = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "metrics_dashboard"
        ordering = ("name", "id")
        indexes = [
            models.Index(
                fields=("organization_id", "name"),
                name="metrics_dash_org_name_idx",
            )
        ]

    def __str__(self):
        return self.name


class MetricsDashboardRevision(models.Model):
    dashboard = models.ForeignKey(
        MetricsDashboard,
        related_name="history",
        on_delete=models.CASCADE,
    )
    revision = models.PositiveIntegerField()
    name = models.CharField(max_length=120)
    description = models.CharField(max_length=500, blank=True, default="")
    time_range_seconds = models.PositiveIntegerField(default=3600)
    refresh_interval_seconds = models.PositiveIntegerField(default=0)
    variables = models.JSONField(default=list, blank=True)
    panels = models.JSONField(default=list, blank=True)
    saved_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        related_name="metrics_dashboard_revisions",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
    )
    saved_at = models.DateTimeField(default=timezone.now)
    restored_from_revision = models.PositiveIntegerField(null=True, blank=True)

    class Meta:
        db_table = "metrics_dashboard_revision"
        ordering = ("-revision",)
        constraints = [
            models.UniqueConstraint(
                fields=("dashboard", "revision"),
                name="metrics_dash_revision_unique",
            )
        ]

    def __str__(self):
        return f"{self.dashboard_id} revision {self.revision}"
