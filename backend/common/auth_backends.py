import datetime

from django.contrib.auth.backends import ModelBackend
from django.db.models import Q

from common.team_permissions import TEAM_PERMISSION_CODES


class TeamPermissionBackend(ModelBackend):
    """Include permission levels from active team memberships in user permissions."""

    def get_group_permissions(self, user_obj, obj=None):
        direct_group_permissions = super().get_group_permissions(user_obj, obj=obj)
        if (
            obj is not None
            or not user_obj.is_active
            or user_obj.is_anonymous
            or user_obj.is_superuser
        ):
            return direct_group_permissions

        from django.contrib.auth.models import Permission

        team_permissions = Permission.objects.filter(
            Q(
                group__team_memberships__user=user_obj,
                group__team_memberships__team__is_deleted=0,
            )
            | Q(
                group__temporary_team_grants__user=user_obj,
                group__temporary_team_grants__team__is_deleted=0,
                group__temporary_team_grants__is_revoked=False,
                group__temporary_team_grants__valid_date__gte=datetime.date.today(),
            ),
            content_type__app_label="sql",
            codename__in=TEAM_PERMISSION_CODES,
        ).values_list(
            "content_type__app_label",
            "codename",
        )
        return direct_group_permissions | {
            f"{app_label}.{codename}" for app_label, codename in team_permissions
        }
