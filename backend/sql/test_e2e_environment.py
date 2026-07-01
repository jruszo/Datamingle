import datetime

from allauth.account.models import EmailAddress
from django.contrib.auth.models import Group
from django.core.management import call_command
from django.test import TestCase

from common.utils.const import WorkflowStatus, WorkflowType
from sql.models import (
    MailboxCategory,
    MailboxItem,
    PermissionRequest,
    PermissionRequestDuration,
    PermissionRequestSubject,
    PermissionRequestTarget,
    Team,
    TeamMembership,
    TemporaryTeamGrant,
    Users,
    WorkflowAudit,
    WorkflowAuditSetting,
    WorkflowLog,
)


class TestE2EEnvironmentSeed(TestCase):
    def test_seed_e2e_environment_creates_verified_local_users_and_access_approval_settings(
        self,
    ):
        call_command("seed_e2e_environment")
        call_command("seed_e2e_environment")

        expected_users = {
            "demo_admin": "demo-admin@datamingle.dev",
            "demo_requester": "demo-requester@datamingle.dev",
            "demo_pm": "demo-pm@datamingle.dev",
            "demo_dba": "demo-dba@datamingle.dev",
            "e2e-admin@datamingle.dev": "e2e-admin@datamingle.dev",
            "e2e-requester@datamingle.dev": "e2e-requester@datamingle.dev",
            "e2e-reviewer@datamingle.dev": "e2e-reviewer@datamingle.dev",
        }
        users = {
            user.username: user
            for user in Users.objects.filter(username__in=expected_users.keys())
        }
        self.assertEqual(set(users.keys()), set(expected_users.keys()))

        for username, email in expected_users.items():
            user = users[username]
            self.assertEqual(user.email, email)
            self.assertTrue(user.is_active)
            self.assertTrue(user.check_password("SecurePass123!"))
            self.assertTrue(
                EmailAddress.objects.filter(
                    user=user,
                    email=email,
                    primary=True,
                    verified=True,
                ).exists()
            )

        self.assertTrue(users["demo_admin"].is_superuser)
        self.assertTrue(users["e2e-admin@datamingle.dev"].is_superuser)
        self.assertIn(
            "sql.menu_queryapplylist",
            users["e2e-requester@datamingle.dev"].get_all_permissions(),
        )
        self.assertIn(
            "sql.query_applypriv",
            users["e2e-requester@datamingle.dev"].get_all_permissions(),
        )
        self.assertIn(
            "sql.menu_queryapplylist",
            users["e2e-reviewer@datamingle.dev"].get_all_permissions(),
        )
        self.assertIn(
            "sql.query_review",
            users["e2e-reviewer@datamingle.dev"].get_all_permissions(),
        )

        single_stage = Team.objects.get(team_name="Demo Workflow Single Stage")
        multi_stage = Team.objects.get(team_name="Demo Workflow Multi Stage")
        dba = Group.objects.get(name="DBA")
        pm = Group.objects.get(name="PM")

        self.assertEqual(
            WorkflowAuditSetting.objects.get(
                team_id=single_stage.team_id,
                workflow_type=WorkflowType.ACCESS_REQUEST,
            ).audit_auth_groups,
            str(dba.id),
        )
        self.assertEqual(
            WorkflowAuditSetting.objects.get(
                team_id=multi_stage.team_id,
                workflow_type=WorkflowType.ACCESS_REQUEST,
            ).audit_auth_groups,
            f"{pm.id},{dba.id}",
        )

    def test_seed_e2e_environment_cleans_stale_permission_scenario_rows(self):
        call_command("seed_e2e_environment")

        requester = Users.objects.get(username="e2e-requester@datamingle.dev")
        reviewer = Users.objects.get(username="e2e-reviewer@datamingle.dev")
        team = Team.objects.get(team_name="Demo Workflow Single Stage")
        qa = Group.objects.get(name="QA")

        TeamMembership.objects.create(user=requester, team=team, permission_level=qa)
        request = PermissionRequest.objects.create(
            team=team,
            permission_level=qa,
            target_type=PermissionRequestTarget.TEAM,
            instance=None,
            access_level="",
            title="E2E stale request",
            reason="created by test",
            subject_type=PermissionRequestSubject.USER,
            access_duration=PermissionRequestDuration.TEMPORARY,
            user_name=requester.username,
            user_display=requester.display,
            valid_date=datetime.date.today() + datetime.timedelta(days=7),
            status=WorkflowStatus.WAITING,
            audit_auth_groups=str(qa.id),
        )
        grant = TemporaryTeamGrant.objects.create(
            user=requester,
            team=team,
            permission_level=qa,
            source_request=request,
            valid_date=datetime.date.today() + datetime.timedelta(days=7),
        )
        audit = WorkflowAudit.objects.create(
            team_id=team.team_id,
            team_name=team.team_name,
            workflow_id=request.request_id,
            workflow_type=WorkflowType.ACCESS_REQUEST,
            workflow_title=request.title,
            audit_auth_groups=str(qa.id),
            current_audit=str(qa.id),
            next_audit="-1",
            current_status=WorkflowStatus.WAITING,
            create_user=requester.username,
            create_user_display=requester.display,
        )
        WorkflowLog.objects.create(
            audit_id=audit.audit_id,
            operation_type=1,
            operation_type_desc="Submit",
            operation_info="stale",
            operator=requester.username,
            operator_display=requester.display,
        )
        MailboxItem.objects.create(
            recipient=reviewer,
            category=MailboxCategory.APPROVAL_NEEDED,
            source_type="permission_request",
            source_id=request.request_id,
            title="Approval needed: E2E stale request",
            body="stale",
            action_path=f"/permission-management?requestId={request.request_id}",
            dedupe_key=f"approval_needed:permission_request:{request.request_id}",
        )

        call_command("seed_e2e_environment")

        self.assertFalse(
            PermissionRequest.objects.filter(request_id=request.request_id).exists()
        )
        self.assertFalse(
            TemporaryTeamGrant.objects.filter(grant_id=grant.grant_id).exists()
        )
        self.assertFalse(WorkflowAudit.objects.filter(audit_id=audit.audit_id).exists())
        self.assertFalse(WorkflowLog.objects.filter(audit_id=audit.audit_id).exists())
        self.assertFalse(
            MailboxItem.objects.filter(source_id=request.request_id).exists()
        )
        self.assertFalse(
            TeamMembership.objects.filter(
                user=requester,
                team=team,
            ).exists()
        )
