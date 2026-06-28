import datetime
from types import SimpleNamespace
from unittest.mock import Mock, patch

from django.contrib.auth.models import Group, Permission
from django.test import TestCase
from rest_framework.test import APIClient

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
    TemporaryTeamGrant,
    Users,
)


class TeamPermissionRequestTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.user = Users.objects.create_user(
            username="requester",
            display="Requester",
            is_active=True,
        )
        self.user.user_permissions.add(
            Permission.objects.get(codename="query_applypriv")
        )
        self.team = Team.objects.create(team_name="Platform")
        self.permission_level, _ = Group.objects.get_or_create(name="QA")
        self.elevated_permission_level, _ = Group.objects.get_or_create(name="DBA")

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

    def test_temporary_approval_creates_team_grant_with_requested_group(self):
        valid_date = datetime.date.today() + datetime.timedelta(days=7)
        permission_request = PermissionRequest.objects.create(
            team=self.team,
            permission_level=self.permission_level,
            target_type=PermissionRequestTarget.TEAM,
            instance=None,
            access_level="",
            title="Join platform temporarily",
            reason="Need query access",
            subject_type=PermissionRequestSubject.USER,
            access_duration=PermissionRequestDuration.TEMPORARY,
            user_name=self.user.username,
            user_display=self.user.display,
            valid_date=valid_date,
            status=WorkflowStatus.WAITING,
            audit_auth_groups="",
        )

        _permission_request_audit_callback(
            permission_request.request_id,
            WorkflowStatus.PASSED,
        )

        grant = TemporaryTeamGrant.objects.get(
            user=self.user,
            team=self.team,
        )
        self.assertEqual(grant.permission_level, self.permission_level)
        self.assertEqual(grant.valid_date, valid_date)

    def _mock_auditor(self, request_id=123):
        auditor = Mock()
        auditor.workflow.request_id = request_id
        auditor.audit.current_status = WorkflowStatus.WAITING
        auditor.create_audit.return_value = None
        return auditor

    def request_team_access(self, permission_level):
        self.client.force_authenticate(self.user)
        return self.client.post(
            "/api/v1/access/request/",
            {
                "title": "Request different level",
                "reason": "Need a different team permission level",
                "target_type": PermissionRequestTarget.TEAM,
                "subject_type": PermissionRequestSubject.USER,
                "access_duration": PermissionRequestDuration.PERMANENT,
                "team_id": self.team.team_id,
                "permission_level_id": permission_level.id,
                "valid_date": datetime.date.today(),
            },
            format="json",
        )

    def test_member_cannot_request_same_permission_level(self):
        TeamMembership.objects.create(
            user=self.user,
            team=self.team,
            permission_level=self.permission_level,
        )

        response = self.request_team_access(self.permission_level)

        self.assertEqual(response.status_code, 400)

    @patch("api_access.views.async_task")
    @patch("api_access.views._sync_permission_request_approval_notifications")
    @patch("api_access.views._permission_request_audit_callback")
    @patch("api_access.views.get_auditor")
    def test_member_can_request_different_permission_level(
        self, mock_get_auditor, _callback, _sync, _async_task
    ):
        TeamMembership.objects.create(
            user=self.user,
            team=self.team,
            permission_level=self.permission_level,
        )
        mock_get_auditor.return_value = self._mock_auditor()

        response = self.request_team_access(self.elevated_permission_level)

        self.assertEqual(response.status_code, 201)

    def test_pending_request_for_same_level_is_blocked(self):
        PermissionRequest.objects.create(
            team=self.team,
            permission_level=self.permission_level,
            target_type=PermissionRequestTarget.TEAM,
            instance=None,
            access_level="",
            title="Existing request",
            reason="Need query access",
            subject_type=PermissionRequestSubject.USER,
            access_duration=PermissionRequestDuration.PERMANENT,
            user_name=self.user.username,
            user_display=self.user.display,
            valid_date=datetime.date.today(),
            status=WorkflowStatus.WAITING,
            audit_auth_groups="",
        )

        response = self.request_team_access(self.permission_level)

        self.assertEqual(response.status_code, 400)

    @patch("api_access.views.async_task")
    @patch("api_access.views._sync_permission_request_approval_notifications")
    @patch("api_access.views._permission_request_audit_callback")
    @patch("api_access.views.get_auditor")
    def test_pending_request_for_one_level_does_not_block_another_level(
        self, mock_get_auditor, _callback, _sync, _async_task
    ):
        PermissionRequest.objects.create(
            team=self.team,
            permission_level=self.permission_level,
            target_type=PermissionRequestTarget.TEAM,
            instance=None,
            access_level="",
            title="Existing request",
            reason="Need query access",
            subject_type=PermissionRequestSubject.USER,
            access_duration=PermissionRequestDuration.PERMANENT,
            user_name=self.user.username,
            user_display=self.user.display,
            valid_date=datetime.date.today(),
            status=WorkflowStatus.WAITING,
            audit_auth_groups="",
        )
        mock_get_auditor.return_value = self._mock_auditor()

        response = self.request_team_access(self.elevated_permission_level)

        self.assertEqual(response.status_code, 201)
