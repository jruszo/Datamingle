from datetime import datetime, time, timedelta
from types import SimpleNamespace
from unittest.mock import patch

from django.test import TestCase

from common.utils.const import WorkflowStatus
from sql.archiver import (
    ARCHIVE_EXECUTION_SCHEDULED,
    ARCHIVE_EXECUTION_STATE_IDLE,
    ARCHIVE_EXECUTION_STATE_QUEUED,
    ARCHIVE_METHOD_PT_ARCHIVER,
    ARCHIVE_SCHEDULE_DAILY,
    ARCHIVE_SCHEDULE_WEEKLY,
    add_archive_task,
    archive,
    archive_task_callback,
    build_archive_delete_sql,
    calculate_next_archive_run,
    queue_archive_execution,
    render_archive_condition,
    _build_pt_archiver_args,
)
from sql.models import (
    Instance,
    ResourceGroup,
    ArchiveConfig,
)


class ArchiveExecutionHelpersTest(TestCase):
    def setUp(self):
        self.resource_group = ResourceGroup.objects.create(group_name="Archive Helpers")
        self.instance = Instance.objects.create(
            instance_name="archive-helper-mysql",
            type="master",
            db_type="mysql",
            host="127.0.0.1",
            port=3306,
            user="root",
            password="secret",
        )
        self.archive = ArchiveConfig.objects.create(
            title="Helper archive",
            resource_group=self.resource_group,
            audit_auth_groups="",
            src_instance=self.instance,
            src_db_name="demo_orders",
            src_table_name="orders",
            condition="created_at < {{ today }} AND updated_at >= {{ yesterday }} AND run_at < {{ tomorrow }} AND touched_at <= {{ now }}",
            mode="purge",
            no_delete=False,
            sleep=1,
            archive_method="dml",
            execution_mode="one_time",
            status=WorkflowStatus.PASSED,
            state=True,
            user_name="system",
            user_display="System",
        )

    def test_render_archive_condition_supports_datetime_variables(self):
        rendered = render_archive_condition(
            "d = {{ today }} OR y = {{ yesterday }} OR t = {{ tomorrow }} OR n = {{ now }}",
            now=datetime(2026, 4, 18, 13, 14, 15),
        )

        self.assertEqual(
            rendered,
            "d = '2026-04-18' OR y = '2026-04-17' OR t = '2026-04-19' OR n = '2026-04-18 13:14:15'",
        )

    def test_build_archive_delete_sql_enforces_single_delete_statement(self):
        self.archive.condition = "id > 10"

        delete_sql = build_archive_delete_sql(self.archive)
        self.assertEqual(delete_sql, "DELETE FROM orders WHERE id > 10")

        self.archive.condition = "id > 10; DROP TABLE orders"
        with self.assertRaises(ValueError):
            build_archive_delete_sql(self.archive)

    @patch("sql.archiver.get_engine")
    def test_pt_archiver_args_enforce_delete_only_purge_mode(self, get_engine_mock):
        fake_table = SimpleNamespace(
            options={"charset": SimpleNamespace(value="utf8mb4")}
        )
        fake_database = SimpleNamespace(tables={"orders": fake_table})
        fake_engine = SimpleNamespace(
            schema_object=SimpleNamespace(databases={"demo_orders": fake_database}),
            close=lambda: None,
        )
        get_engine_mock.return_value = fake_engine

        args = _build_pt_archiver_args(self.archive)

        self.assertEqual(
            args["where"], render_archive_condition(self.archive.condition)
        )
        self.assertTrue(args["purge"])
        self.assertNotIn("file", args)
        self.assertNotIn("dest", args)
        self.assertNotIn("no-delete", args)

    def test_calculate_next_archive_run_supports_daily_and_weekly(self):
        daily_archive = SimpleNamespace(
            execution_mode=ARCHIVE_EXECUTION_SCHEDULED,
            schedule_frequency=ARCHIVE_SCHEDULE_DAILY,
            schedule_time=time(2, 30),
            schedule_weekdays="",
        )
        daily_next_run = calculate_next_archive_run(
            daily_archive,
            from_time=datetime(2026, 4, 18, 1, 0, 0),
        )
        self.assertEqual(daily_next_run.strftime("%Y-%m-%d %H:%M"), "2026-04-18 02:30")

        weekly_archive = SimpleNamespace(
            execution_mode=ARCHIVE_EXECUTION_SCHEDULED,
            schedule_frequency=ARCHIVE_SCHEDULE_WEEKLY,
            schedule_time=time(3, 45),
            schedule_weekdays="mon,fri",
        )
        weekly_next_run = calculate_next_archive_run(
            weekly_archive,
            from_time=datetime(2026, 4, 18, 4, 0, 0),
        )
        self.assertEqual(weekly_next_run.strftime("%Y-%m-%d %H:%M"), "2026-04-20 03:45")

    def test_archive_task_callback_disables_completed_one_time_archives(self):
        task = SimpleNamespace(
            args=(self.archive.id, "scheduled"),
            success=True,
            result="done",
            error="",
        )

        archive_task_callback(task)

        self.archive.refresh_from_db()
        self.assertFalse(self.archive.state)
        self.assertIsNone(self.archive.next_run_at)

    @patch("sql.archiver.calculate_next_archive_run")
    def test_archive_task_callback_rearms_scheduled_archives_after_success(
        self, calculate_next_archive_run_mock
    ):
        next_run = datetime.now() + timedelta(days=2)
        calculate_next_archive_run_mock.return_value = next_run
        self.archive.execution_mode = ARCHIVE_EXECUTION_SCHEDULED
        self.archive.archive_method = ARCHIVE_METHOD_PT_ARCHIVER
        self.archive.schedule_frequency = ARCHIVE_SCHEDULE_DAILY
        self.archive.schedule_time = time(2, 0)
        self.archive.save(
            update_fields=[
                "execution_mode",
                "archive_method",
                "schedule_frequency",
                "schedule_time",
            ]
        )

        def fake_schedule_archive(archive_info, run_at=None):
            ArchiveConfig.objects.filter(id=archive_info.id).update(next_run_at=run_at)

        task = SimpleNamespace(
            args=(self.archive.id, "scheduled"),
            success=True,
            result="done",
            error="",
        )

        with patch(
            "sql.archiver.schedule_archive", side_effect=fake_schedule_archive
        ) as schedule_archive_mock:
            archive_task_callback(task)

        self.archive.refresh_from_db()
        self.assertEqual(
            self.archive.next_run_at.replace(tzinfo=None),
            next_run.replace(tzinfo=None),
        )
        schedule_archive_mock.assert_called_once()

    def test_archive_task_callback_stops_rescheduling_after_scheduled_failure(self):
        self.archive.execution_mode = ARCHIVE_EXECUTION_SCHEDULED
        self.archive.schedule_frequency = ARCHIVE_SCHEDULE_DAILY
        self.archive.schedule_time = time(2, 0)
        self.archive.next_run_at = datetime.now() + timedelta(days=1)
        self.archive.save(
            update_fields=[
                "execution_mode",
                "schedule_frequency",
                "schedule_time",
                "next_run_at",
            ]
        )

        task = SimpleNamespace(
            args=(self.archive.id, "scheduled"),
            success=False,
            result="",
            error="boom",
        )

        with patch("sql.archiver.schedule_archive") as schedule_archive_mock:
            archive_task_callback(task)

        self.archive.refresh_from_db()
        self.assertEqual(self.archive.consecutive_failures, 1)
        self.assertTrue(self.archive.state)
        self.assertIsNone(self.archive.next_run_at)
        schedule_archive_mock.assert_not_called()

    def test_archive_task_callback_disables_after_repeated_scheduled_failures(self):
        self.archive.execution_mode = ARCHIVE_EXECUTION_SCHEDULED
        self.archive.schedule_frequency = ARCHIVE_SCHEDULE_DAILY
        self.archive.schedule_time = time(2, 0)
        self.archive.consecutive_failures = 2
        self.archive.save(
            update_fields=[
                "execution_mode",
                "schedule_frequency",
                "schedule_time",
                "consecutive_failures",
            ]
        )

        task = SimpleNamespace(
            args=(self.archive.id, "scheduled"),
            success=False,
            result="",
            error="boom",
        )

        archive_task_callback(task)

        self.archive.refresh_from_db()
        self.assertEqual(self.archive.consecutive_failures, 3)
        self.assertFalse(self.archive.state)
        self.assertIsNone(self.archive.next_run_at)

    @patch("sql.archiver.logger")
    def test_archive_task_callback_ignores_missing_archive(self, logger_mock):
        task = SimpleNamespace(
            args=(999999, "scheduled"),
            success=False,
            result="",
            error="boom",
        )

        archive_task_callback(task)

        logger_mock.warning.assert_called_once()

    @patch("sql.archiver.async_task")
    def test_queue_archive_execution_skips_duplicate_queueing(self, async_task_mock):
        self.assertTrue(queue_archive_execution(self.archive.id, trigger="manual"))
        self.assertFalse(queue_archive_execution(self.archive.id, trigger="manual"))

        self.archive.refresh_from_db()
        self.assertEqual(
            self.archive.execution_state,
            ARCHIVE_EXECUTION_STATE_QUEUED,
        )
        async_task_mock.assert_called_once()

    @patch("sql.archiver._execute_dml_archive", return_value={"success": True})
    def test_archive_resets_execution_state_after_running(self, _execute_dml_archive):
        self.archive.execution_state = ARCHIVE_EXECUTION_STATE_QUEUED
        self.archive.save(update_fields=["execution_state"])

        archive(self.archive.id, trigger="manual")

        self.archive.refresh_from_db()
        self.assertEqual(self.archive.execution_state, ARCHIVE_EXECUTION_STATE_IDLE)
