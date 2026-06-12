import datetime
from types import SimpleNamespace

from django.contrib.auth.models import Group
from django.test import TestCase

from api_access.views import (
    PermissionRequestCreateSerializer,
    _permission_request_audit_callback,
)
from common.utils.const import WorkflowStatus
from sql.models import (
    PermissionRequest,
    PermissionRequestDuration,
    PermissionRequestSubject,
    PermissionRequestTarget,
    Team,
    TeamMembership,
    Users,
)


class TeamPermissionRequestTests(TestCase):
    def setUp(self):
        self.user = Users.objects.create_user(
            username="requester",
            display="Requester",
            is_active=True,
        )
        self.team = Team.objects.create(team_name="Platform")
        self.permission_level, _ = Group.objects.get_or_create(name="QA")

    def serializer(self, **overrides):
        data = {
            "title": "Join platform",
            "reason": "Need query access",
            "target_type": PermissionRequestTarget.TEAM,
            "subject_type": PermissionRequestSubject.USER,
            "access_duration": PermissionRequestDuration.PERMANENT,
            "team_id": self.team.team_id,
            "permission_level_id": self.permission_level.id,
            "valid_date": datetime.date.today(),
        }
        data.update(overrides)
        return PermissionRequestCreateSerializer(
            data=data,
            context={"request": SimpleNamespace(user=self.user)},
        )

    def test_team_request_requires_permission_level(self):
        serializer = self.serializer(permission_level_id=None)

        self.assertFalse(serializer.is_valid())
        self.assertIn("permission_level_id", serializer.errors)

    def test_team_request_resolves_permission_level(self):
        serializer = self.serializer()

        self.assertTrue(serializer.is_valid(), serializer.errors)
        self.assertEqual(
            serializer.validated_data["permission_level"],
            self.permission_level,
        )

    def test_permanent_approval_creates_team_membership_with_requested_group(self):
        permission_request = PermissionRequest.objects.create(
            team=self.team,
            permission_level=self.permission_level,
            target_type=PermissionRequestTarget.TEAM,
            instance=None,
            access_level="",
            title="Join platform",
            reason="Need query access",
            subject_type=PermissionRequestSubject.USER,
            access_duration=PermissionRequestDuration.PERMANENT,
            user_name=self.user.username,
            user_display=self.user.display,
            valid_date=datetime.date.today(),
            status=WorkflowStatus.WAITING,
            audit_auth_groups="",
        )

        _permission_request_audit_callback(
            permission_request.request_id,
            WorkflowStatus.PASSED,
        )

        membership = TeamMembership.objects.get(
            user=self.user,
            team=self.team,
        )
        self.assertEqual(membership.permission_level, self.permission_level)
