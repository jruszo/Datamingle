import datetime

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group, Permission
from django.test import TestCase
from django.utils import timezone
from rest_framework.test import APIClient

from common.utils.const import WorkflowStatus, WorkflowType
from sql.mailbox import (
    backfill_mailbox_notifications,
    emit_execution_finished_notifications,
    sync_approval_notifications,
    sync_execution_needed_notifications,
)
from sql.models import (
    ArchiveConfig,
    Instance,
    MailboxCategory,
    MailboxItem,
    PermissionRequest,
    PermissionRequestTarget,
    Team,
    TeamMembership,
    SqlWorkflow,
    WorkflowAudit,
)

User = get_user_model()


class MailboxApiTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.requester = User.objects.create_user(
            username="requester",
            password="secret",
            display="Requester",
            is_active=True,
        )
        self.reviewer = User.objects.create_user(
            username="reviewer",
            password="secret",
            display="Reviewer",
            is_active=True,
        )
        self.executor = User.objects.create_user(
            username="executor",
            password="secret",
            display="Executor",
            is_active=True,
        )
        self.group = Group.objects.create(name="Workflow Reviewers")
        self.qa_group, _ = Group.objects.get_or_create(name="QA")

        self.team = Team.objects.create(team_name="RG Mailbox")
        TeamMembership.objects.create(
            user=self.requester,
            team=self.team,
            permission_group=self.qa_group,
        )
        TeamMembership.objects.create(
            user=self.reviewer,
            team=self.team,
            permission_group=self.group,
        )
        TeamMembership.objects.create(
            user=self.executor,
            team=self.team,
            permission_group=self.qa_group,
        )

        self.instance = Instance.objects.create(
            instance_name="mailbox-db",
            type="master",
            db_type="mysql",
            host="127.0.0.1",
            port=3306,
            user="root",
            password="pwd",
        )
        self.instance.resource_group.add(self.team)

        self.reviewer.user_permissions.add(
            Permission.objects.get(codename="sql_review")
        )
        self.executor.user_permissions.add(
            Permission.objects.get(codename="sql_execute_for_team")
        )
        self.requester.user_permissions.add(
            Permission.objects.get(codename="sql_execute")
        )
        self.reviewer.user_permissions.add(
            Permission.objects.get(codename="query_review")
        )

    def _create_sql_workflow(self, status="workflow_manreviewing"):
        workflow = SqlWorkflow.objects.create(
            workflow_name="Mailbox Workflow",
            team_id=self.team.team_id,
            team_name=self.team.team_name,
            instance=self.instance,
            db_name="archery",
            syntax_type=1,
            engineer=self.requester.username,
            engineer_display=self.requester.display,
            status=status,
            audit_auth_groups=str(self.group.id),
        )
        WorkflowAudit.objects.create(
            team_id=self.team.team_id,
            team_name=self.team.team_name,
            workflow_id=workflow.id,
            workflow_type=WorkflowType.SQL_REVIEW,
            workflow_title=workflow.workflow_name,
            workflow_remark="",
            audit_auth_groups=str(self.group.id),
            current_audit=str(self.group.id),
            next_audit="-1",
            current_status=WorkflowStatus.WAITING,
            create_user=self.requester.username,
            create_user_display=self.requester.display,
        )
        return workflow

    def _create_permission_request(self):
        request = PermissionRequest.objects.create(
            team=self.team,
            permission_group=self.qa_group,
            target_type=PermissionRequestTarget.TEAM,
            title="Mailbox Permission Request",
            reason="Need access",
            user_name=self.requester.username,
            user_display=self.requester.display,
            valid_date=datetime.date.today() + datetime.timedelta(days=7),
            status=WorkflowStatus.WAITING,
            audit_auth_groups=str(self.group.id),
        )
        WorkflowAudit.objects.create(
            team_id=self.team.team_id,
            team_name=self.team.team_name,
            workflow_id=request.request_id,
            workflow_type=WorkflowType.ACCESS_REQUEST,
            workflow_title=request.title,
            workflow_remark=request.reason,
            audit_auth_groups=str(self.group.id),
            current_audit=str(self.group.id),
            next_audit="-1",
            current_status=WorkflowStatus.WAITING,
            create_user=self.requester.username,
            create_user_display=self.requester.display,
        )
        return request

    def _create_archive(self):
        archive = ArchiveConfig.objects.create(
            title="Mailbox Archive",
            team=self.team,
            audit_auth_groups=str(self.group.id),
            src_instance=self.instance,
            src_db_name="archery",
            src_table_name="workflow_log",
            condition="id > 0",
            mode="purge",
            no_delete=False,
            sleep=1,
            archive_method="dml",
            execution_mode="one_time",
            status=WorkflowStatus.PASSED,
            state=True,
            execution_state="idle",
            user_name=self.requester.username,
            user_display=self.requester.display,
        )
        return archive

    def test_sql_workflow_sync_creates_approval_then_execution_needed_items(self):
        workflow = self._create_sql_workflow()

        sync_approval_notifications(workflow)
        approval_items = MailboxItem.objects.filter(
            category=MailboxCategory.APPROVAL_NEEDED,
            source_id=workflow.id,
        )
        self.assertEqual(approval_items.count(), 1)
        self.assertEqual(approval_items.get().recipient, self.reviewer)

        workflow.status = "workflow_review_pass"
        workflow.save(update_fields=["status"])
        audit = workflow.get_audit()
        audit.current_status = WorkflowStatus.PASSED
        audit.current_audit = "-1"
        audit.save(update_fields=["current_status", "current_audit"])

        sync_approval_notifications(workflow)
        sync_execution_needed_notifications(workflow)

        approval_item = MailboxItem.objects.get(
            category=MailboxCategory.APPROVAL_NEEDED,
            source_id=workflow.id,
        )
        self.assertIsNotNone(approval_item.resolved_at)

        execution_items = MailboxItem.objects.filter(
            category=MailboxCategory.EXECUTION_NEEDED,
            source_id=workflow.id,
        ).order_by("recipient__username")
        self.assertEqual(
            list(execution_items.values_list("recipient__username", flat=True)),
            ["executor", "requester"],
        )

    def test_permission_request_sync_is_idempotent(self):
        permission_request = self._create_permission_request()

        sync_approval_notifications(permission_request)
        sync_approval_notifications(permission_request)

        approval_items = MailboxItem.objects.filter(
            category=MailboxCategory.APPROVAL_NEEDED,
            source_type="permission_request",
            source_id=permission_request.request_id,
        )
        self.assertEqual(approval_items.count(), 1)
        self.assertEqual(approval_items.get().recipient, self.reviewer)

    def test_archive_and_sql_emit_execution_finished_notifications(self):
        workflow = self._create_sql_workflow(status="workflow_finish")
        workflow.finish_time = timezone.now()
        workflow.save(update_fields=["finish_time"])

        emit_execution_finished_notifications(
            workflow,
            outcome="success",
            actor=self.executor,
            dedupe_suffix="sql-finish-1",
        )

        archive = self._create_archive()
        emit_execution_finished_notifications(
            archive,
            outcome="failure",
            dedupe_suffix="archive-finish-1",
        )

        sql_items = MailboxItem.objects.filter(
            category=MailboxCategory.EXECUTION_FINISHED,
            source_type="sql_workflow",
            source_id=workflow.id,
        ).order_by("recipient__username")
        archive_items = MailboxItem.objects.filter(
            category=MailboxCategory.EXECUTION_FINISHED,
            source_type="archive",
            source_id=archive.id,
        )

        self.assertEqual(
            list(sql_items.values_list("recipient__username", flat=True)),
            ["executor", "requester"],
        )
        self.assertEqual(archive_items.count(), 1)
        self.assertEqual(archive_items.get().recipient, self.requester)
        self.assertEqual(archive_items.get().metadata["outcome"], "failure")

    def test_mailbox_summary_list_and_read_actions(self):
        workflow = self._create_sql_workflow()
        sync_approval_notifications(workflow)
        item = MailboxItem.objects.get(
            category=MailboxCategory.APPROVAL_NEEDED,
            source_id=workflow.id,
        )

        self.client.force_authenticate(self.reviewer)

        summary_response = self.client.get("/api/v1/mailbox/summary/", format="json")
        self.assertEqual(summary_response.status_code, 200)
        self.assertEqual(summary_response.json()["data"]["unread_count"], 1)

        list_response = self.client.get(
            "/api/v1/mailbox/items/?state=unread",
            format="json",
        )
        self.assertEqual(list_response.status_code, 200)
        self.assertEqual(list_response.json()["data"]["count"], 1)

        read_response = self.client.post(
            f"/api/v1/mailbox/items/{item.id}/read/",
            format="json",
        )
        self.assertEqual(read_response.status_code, 200)
        item.refresh_from_db()
        self.assertFalse(item.is_unread)
        self.assertIsNotNone(item.read_at)

        second_item = MailboxItem.objects.create(
            recipient=self.reviewer,
            category=MailboxCategory.EXECUTION_FINISHED,
            source_type="sql_workflow",
            source_id=999,
            title="Another item",
            body="Finished.",
            action_path="/workflows/999",
            dedupe_key="execution_finished:sql_workflow:999:test",
        )
        read_all_response = self.client.post(
            "/api/v1/mailbox/items/read-all/",
            format="json",
        )
        self.assertEqual(read_all_response.status_code, 200)
        second_item.refresh_from_db()
        self.assertFalse(second_item.is_unread)

    def test_mailbox_list_rejects_invalid_state_filter(self):
        workflow = self._create_sql_workflow()
        sync_approval_notifications(workflow)

        self.client.force_authenticate(self.reviewer)

        response = self.client.get(
            "/api/v1/mailbox/items/?state=unred",
            format="json",
        )

        self.assertEqual(response.status_code, 400)
        error_value = response.json()["state"]
        if isinstance(error_value, list):
            error_value = error_value[0]
        self.assertEqual(
            error_value,
            "Unsupported state filter. Use one of: all, read, unread.",
        )

    def test_backfill_is_idempotent_for_active_items(self):
        workflow = self._create_sql_workflow()
        archive = self._create_archive()
        permission_request = self._create_permission_request()

        backfill_mailbox_notifications()
        backfill_mailbox_notifications()

        self.assertEqual(
            MailboxItem.objects.filter(
                category=MailboxCategory.APPROVAL_NEEDED,
                source_type="sql_workflow",
                source_id=workflow.id,
            ).count(),
            1,
        )
        self.assertEqual(
            MailboxItem.objects.filter(
                category=MailboxCategory.EXECUTION_NEEDED,
                source_type="archive",
                source_id=archive.id,
            ).count(),
            1,
        )
        self.assertEqual(
            MailboxItem.objects.filter(
                category=MailboxCategory.APPROVAL_NEEDED,
                source_type="permission_request",
                source_id=permission_request.request_id,
            ).count(),
            1,
        )
