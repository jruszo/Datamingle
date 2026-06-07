from django.conf import settings
from django.db import models
from django.utils import timezone


class MetricsDashboard(models.Model):
    organization_id = models.CharField(max_length=128, db_index=True)
    name = models.CharField(max_length=120)
    description = models.CharField(max_length=500, blank=True, default="")
    icon = models.ImageField(upload_to="dashboard_icons/", blank=True, null=True)
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        related_name="metrics_dashboards",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
    )
    revision = models.PositiveIntegerField(default=1)
    time_range_mode = models.CharField(max_length=16, default="relative")
    time_range_seconds = models.PositiveIntegerField(default=3600)
    time_range_start = models.CharField(max_length=40, blank=True, default="")
    time_range_end = models.CharField(max_length=40, blank=True, default="")
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


class MetricsDashboardFavorite(models.Model):
    dashboard = models.ForeignKey(
        MetricsDashboard,
        related_name="favorites",
        on_delete=models.CASCADE,
    )
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        related_name="favorite_metrics_dashboards",
        on_delete=models.CASCADE,
    )
    create_time = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "metrics_dashboard_favorite"
        constraints = [
            models.UniqueConstraint(
                fields=("dashboard", "user"),
                name="metrics_dash_favorite_unique",
            )
        ]

    def __str__(self):
        return f"{self.user_id} favorites dashboard {self.dashboard_id}"


class MetricsDashboardRevision(models.Model):
    dashboard = models.ForeignKey(
        MetricsDashboard,
        related_name="history",
        on_delete=models.CASCADE,
    )
    revision = models.PositiveIntegerField()
    name = models.CharField(max_length=120)
    description = models.CharField(max_length=500, blank=True, default="")
    time_range_mode = models.CharField(max_length=16, default="relative")
    time_range_seconds = models.PositiveIntegerField(default=3600)
    time_range_start = models.CharField(max_length=40, blank=True, default="")
    time_range_end = models.CharField(max_length=40, blank=True, default="")
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
