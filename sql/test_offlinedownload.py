from unittest.mock import patch, MagicMock, Mock, mock_open
from django.test import TestCase, Client
from django.conf import settings
from django.http import HttpRequest
from datetime import datetime, date
import tempfile
import os
import shutil
import zipfile
import simplejson as json
import pandas as pd
import csv
import xml.etree.ElementTree as ET

from sql.models import SqlWorkflow, SqlWorkflowContent, Instance, Config, AuditEntry
from sql.offlinedownload import (
    OffLineDownLoad,
    save_to_format_file,
    save_csv,
    save_json,
    save_xml,
    save_xlsx,
    save_sql,
    offline_file_download,
)
from sql.engines.models import ReviewSet, ReviewResult, ResultSet
from sql.storage import DynamicStorage
from sql.tests import User


class TestOfflineDownload(TestCase):
    """
    Tests for offline download features.
    """

    def setUp(self):
        # Create test user.
        self.client = Client()
        self.superuser = User.objects.create(username="super", is_superuser=True)
        # Create test instance.
        self.instance = Instance.objects.create(
            instance_name="test_instance",
            type="master",
            db_type="mysql",
            host=settings.DATABASES["default"]["HOST"],
            port=settings.DATABASES["default"]["PORT"],
            user=settings.DATABASES["default"]["USER"],
            password=settings.DATABASES["default"]["PASSWORD"],
        )
        # Create test workflow.
        self.workflow = SqlWorkflow.objects.create(
            workflow_name="test_workflow",
            group_id=1,
            group_name="test_group",
            engineer_display="test_user",
            audit_auth_groups="test_group",
            status="workflow_finish",
            is_backup=True,
            instance=self.instance,
            db_name="test_db",
            syntax_type=1,
            is_offline_export=1,
            export_format="csv",
        )
        self.sql_content = SqlWorkflowContent.objects.create(
            workflow=self.workflow,
            sql_content="SELECT * FROM test_table",
            execute_result="",
        )
        # Configure system settings.
        Config.objects.create(item="max_export_rows", value="10000")
        Config.objects.create(item="storage_type", value="local")

    def tearDown(self):
        # Clean test data.
        SqlWorkflow.objects.all().delete()
        SqlWorkflowContent.objects.all().delete()
        Instance.objects.all().delete()
        Config.objects.all().delete()
        AuditEntry.objects.all().delete()

    @patch("sql.offlinedownload.get_engine")
    def test_pre_count_check_pass(self, mock_get_engine):
        """
        Test pre_count_check - normal pass.
        """

        # Mock database query result.
        mock_engine = MagicMock()
        mock_result_set = MagicMock()
        mock_result_set.rows = [(500,)]
        mock_result_set.error = None
        mock_engine.query.return_value = mock_result_set
        mock_get_engine.return_value = mock_engine

        # Execute test.
        offline_download = OffLineDownLoad()
        # Set workflow SQL for this test.
        self.workflow.sql_content = "SELECT * FROM test_table"
        result = offline_download.pre_count_check(self.workflow)

        # Verify result.
        self.assertEqual(result.error_count, 0)
        self.assertEqual(result.warning_count, 0)
        self.assertEqual(result.rows[0].stagestatus, "Row count completed")
        self.assertEqual(result.rows[0].affected_rows, 500)

    @patch("sql.offlinedownload.get_engine")
    def test_pre_count_check_over_limit(self, mock_get_engine):
        """
        Test pre_count_check - row count exceeds threshold.
        """

        # Mock database query result.
        mock_engine = MagicMock()
        mock_result_set = MagicMock()
        mock_result_set.rows = [(15000,)]
        mock_result_set.error = None
        mock_engine.query.return_value = mock_result_set
        mock_get_engine.return_value = mock_engine

        # Execute test.
        offline_download = OffLineDownLoad()
        self.workflow.sql_content = "SELECT * FROM test_table"
        result = offline_download.pre_count_check(self.workflow)

        # Verify result.
        self.assertEqual(result.error_count, 1)
        self.assertEqual(result.warning_count, 0)
        self.assertIn("exceeds threshold", result.rows[0].errormessage)

    @patch("sql.offlinedownload.get_engine")
    def test_pre_count_check_invalid_sql(self, mock_get_engine):
        """
        Test pre_count_check - invalid SQL statement.
        """

        # Mock get_engine return value.
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine

        offline_download = OffLineDownLoad()
        self.workflow.sql_content = "DELETE FROM test_table"
        result = offline_download.pre_count_check(self.workflow)

        # Verify result.
        self.assertEqual(result.error_count, 1)
        self.assertEqual(result.warning_count, 0)
        self.assertEqual(result.rows[0].errormessage, "Disallowed statement!")

    @patch("sql.offlinedownload.get_engine")
    @patch("sql.offlinedownload.DynamicStorage")
    @patch("sql.offlinedownload.save_to_format_file")
    @patch("builtins.open", new_callable=mock_open)
    def test_execute_offline_download_success(
        self, mock_open_file, mock_save_format, mock_storage, mock_get_engine
    ):
        """
        Test execute_offline_download - success path.
        """

        # Mock dependencies.
        mock_engine = MagicMock()
        mock_result_set = MagicMock()
        mock_result_set.error = None
        mock_result_set.column_list = ["id", "name"]
        mock_result_set.rows = [(1, "test1"), (2, "test2")]
        mock_result_set.affected_rows = 2
        mock_engine.query.return_value = mock_result_set
        mock_get_engine.return_value = mock_engine

        mock_save_format.return_value = "test_file.zip"

        mock_storage_instance = MagicMock()
        mock_storage.return_value = mock_storage_instance

        # Mock file open.
        mock_file = MagicMock()
        mock_open_file.return_value = mock_file

        # Execute test.
        offline_download = OffLineDownLoad()
        result = offline_download.execute_offline_download(self.workflow)

        # Verify result.
        self.assertEqual(result.error, None)
        self.assertEqual(result.rows[0].stagestatus, "Execution succeeded")
        self.assertIn("test_file.zip", result.rows[0].errormessage)

        # Verify workflow update.
        updated_workflow = SqlWorkflow.objects.get(id=self.workflow.id)
        self.assertEqual(updated_workflow.file_name, "test_file.zip")

    @patch("sql.offlinedownload.get_engine")
    @patch("sql.offlinedownload.DynamicStorage")
    def test_execute_offline_download_error(self, mock_storage, mock_get_engine):
        """
        Test execute_offline_download - failure path.
        """

        # Mock database query error.
        mock_engine = MagicMock()
        mock_result_set = MagicMock()
        mock_result_set.error = "Database error"
        mock_engine.query.return_value = mock_result_set
        mock_get_engine.return_value = mock_engine

        # Mock DynamicStorage.
        mock_storage_instance = MagicMock()
        mock_storage.return_value = mock_storage_instance

        # Execute test.
        offline_download = OffLineDownLoad()
        result = offline_download.execute_offline_download(self.workflow)

        # Verify result.
        self.assertIsNotNone(result.error)
        self.assertEqual(result.rows[0].stagestatus, "Aborted")
        self.assertEqual(result.rows[0].errormessage, "Database error")

    def test_save_csv(self):
        """
        Test save_csv.
        """

        # Create temporary file.
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        temp_file.close()

        # Test data.
        result = [(1, "test1"), (2, None)]
        columns = ["id", "name"]

        # Execute test.
        save_csv(temp_file.name, result, columns)

        # Verify result.
        with open(temp_file.name, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)
            self.assertEqual(rows[0], columns)
            self.assertEqual(rows[1], ["1", "test1"])
            self.assertEqual(rows[2], ["2", "null"])

        # Cleanup.
        os.unlink(temp_file.name)

    def test_save_csv_special_chars(self):
        """
        Test save_csv with special characters.
        """

        # Create temporary file.
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        temp_file.close()

        # Test data with special characters.
        result = [
            (1, "Normal, value"),
            (2, 'Value with "quotes"'),
            (3, "Line\nbreak"),
            (4, 'Comma, and quote"'),
        ]
        columns = ["id", "text"]

        # Execute test.
        save_csv(temp_file.name, result, columns)

        # Verify result.
        with open(temp_file.name, "r", encoding="utf-8") as f:
            content = f.read()
            self.assertIn('"Normal, value"', content)
            self.assertIn('"Value with ""quotes"""', content)  # CSV standard escaping.
            self.assertIn('"Line\nbreak"', content)
            self.assertIn('"Comma, and quote"""', content)

        # Cleanup.
        os.unlink(temp_file.name)

    def test_save_json(self):
        """
        Test save_json.
        """

        # Create temporary file.
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        temp_file.close()

        # Test data.
        result = [(1, "test1"), (2, "2023-01-01T00:00:00")]
        columns = ["id", "name"]

        # Execute test.
        save_json(temp_file.name, result, columns)

        # Verify result.
        with open(temp_file.name, "r", encoding="utf-8") as f:
            data = json.load(f)
            self.assertEqual(len(data), 2)
            self.assertEqual(data[0]["id"], 1)
            self.assertEqual(data[0]["name"], "test1")
            self.assertEqual(data[1]["id"], 2)
            self.assertEqual(data[1]["name"], "2023-01-01T00:00:00")

        # Cleanup.
        os.unlink(temp_file.name)

    def test_save_xml(self):
        """
        Test save_xml.
        """

        # Create temporary file.
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        temp_file.close()

        # Test data.
        result = [(1, "test1"), (2, datetime(2023, 1, 1))]
        columns = ["id", "name"]

        # Execute test.
        save_xml(temp_file.name, result, columns)

        # Verify result.
        tree = ET.parse(temp_file.name)
        root = tree.getroot()

        # Verify fields.
        fields = root.find("fields")
        self.assertEqual(len(fields), 2)
        self.assertEqual(fields[0].text, "id")
        self.assertEqual(fields[1].text, "name")

        # Verify data.
        data = root.find("data")
        self.assertEqual(len(data), 2)
        row1 = data[0]
        self.assertEqual(row1.get("id"), "1")
        self.assertEqual(row1.find("column-1").text, "1")
        self.assertEqual(row1.find("column-2").text, "test1")

        row2 = data[1]
        self.assertEqual(row2.get("id"), "2")
        self.assertEqual(row2.find("column-1").text, "2")
        self.assertEqual(row2.find("column-2").text, "2023-01-01T00:00:00")

        # Cleanup.
        os.unlink(temp_file.name)

    def test_save_xlsx(self):
        """
        Test save_xlsx.
        """

        # Create temporary file.
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        temp_file.close()

        # Test data.
        result = [(1, "test1"), (2, "test2")]
        columns = ["id", "name"]

        # Execute test.
        save_xlsx(temp_file.name, result, columns)

        # Verify result.
        df = pd.read_excel(temp_file.name)
        self.assertEqual(len(df), 2)
        self.assertEqual(list(df.columns), columns)
        self.assertEqual(df.iloc[0]["id"], 1)
        self.assertEqual(df.iloc[0]["name"], "test1")
        self.assertEqual(df.iloc[1]["id"], 2)
        self.assertEqual(df.iloc[1]["name"], "test2")

        # Cleanup.
        os.unlink(temp_file.name)

    @patch("sql.offlinedownload.pd.DataFrame")
    def test_save_xlsx_large_file(self, mock_dataframe):
        """
        Test save_xlsx when exceeding Excel row limit.
        """

        # Create temporary file.
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        temp_file.close()

        # Mock pd.DataFrame.to_excel raising an exception.
        mock_dataframe.return_value.to_excel.side_effect = ValueError(
            "Excel max rows exceeded"
        )

        # Test data (no need to generate huge dataset).
        result = [(1, "test1")]
        columns = ["id", "name"]

        # Execute test and verify exception.
        with self.assertRaises(ValueError) as context:
            save_xlsx(temp_file.name, result, columns)
        self.assertIn(
            "Excel supports at most 1048576 rows, limit exceeded!",
            str(context.exception),
        )

        # Cleanup.
        os.unlink(temp_file.name)

    def test_save_sql(self):
        """
        Test save_sql.
        """

        # Create temporary file.
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".sql")
        temp_file.close()

        # Test data.
        result = [(1, "test1"), (2, datetime(2023, 1, 1))]
        columns = ["id", "name"]

        # Execute test.
        save_sql(temp_file.name, result, columns)

        # Verify result.
        with open(temp_file.name, "r") as f:
            content = f.read()
            self.assertIn(
                "INSERT INTO your_table_name (id, name) VALUES (1, 'test1');", content
            )
            self.assertIn(
                "INSERT INTO your_table_name (id, name) VALUES (2, '2023-01-01 00:00:00');",
                content,
            )

        # Cleanup.
        os.unlink(temp_file.name)

    def test_save_sql_special_values(self):
        """
        Test save_sql with special values.
        """

        # Create temporary file.
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".sql")
        temp_file.close()

        # Test data.
        result = [(1, None), (2, ""), (3, "O'Reilly"), (4, "Special;value")]
        columns = ["id", "name"]

        # Execute test.
        save_sql(temp_file.name, result, columns)

        # Verify result.
        with open(temp_file.name, "r") as f:
            content = f.read()
            self.assertIn("(1, NULL);", content)
            self.assertIn("(2, '');", content)
            self.assertIn("(3, 'O''Reilly');", content)  # Single quote escaping.
            self.assertIn("(4, 'Special;value');", content)

        # Cleanup.
        os.unlink(temp_file.name)

    def test_save_to_format_file(self):
        """
        Test save_to_format_file.
        """

        # Create temporary directory.
        temp_dir = tempfile.mkdtemp()

        # Test data.
        result = [(1, "test1"), (2, "test2")]
        columns = ["id", "name"]

        # Test CSV format.
        csv_file_name = save_to_format_file(
            "csv", result, self.workflow, columns, temp_dir
        )
        self.assertTrue(csv_file_name.endswith(".zip"))
        # Verify ZIP contains CSV.
        zip_file_path = os.path.join(temp_dir, csv_file_name)
        with zipfile.ZipFile(zip_file_path, "r") as zipf:
            file_list = zipf.namelist()
            self.assertEqual(len(file_list), 1)
            self.assertTrue(file_list[0].endswith(".csv"))

        # Cleanup.
        shutil.rmtree(temp_dir)

    def test_save_to_format_file_unsupported(self):
        """
        Test save_to_format_file - unsupported format.
        """
        # Create temporary directory.
        temp_dir = tempfile.mkdtemp()

        # Test data.
        result = [(1, "test1"), (2, "test2")]
        columns = ["id", "name"]

        # Test unsupported format.
        with self.assertRaises(ValueError) as context:
            save_to_format_file(
                "invalid_format", result, self.workflow, columns, temp_dir
            )

        self.assertIn("Unsupported format type: invalid_format", str(context.exception))

        # Cleanup.
        shutil.rmtree(temp_dir)

    @patch("sql.offlinedownload.get_engine")
    def test_execute_offline_download_empty_result(self, mock_get_engine):
        """
        Test execute_offline_download with empty result set.
        """

        # Mock dependencies.
        mock_engine = MagicMock()
        mock_result_set = MagicMock()
        mock_result_set.error = None
        mock_result_set.column_list = ["id", "name"]
        mock_result_set.rows = []
        mock_result_set.affected_rows = 0
        mock_engine.query.return_value = mock_result_set
        mock_get_engine.return_value = mock_engine

        # Execute test.
        offline_download = OffLineDownLoad()
        result = offline_download.execute_offline_download(self.workflow)

        # Verify result.
        self.assertEqual(result.error, None)
        self.assertEqual(result.rows[0].stagestatus, "Execution succeeded")
        self.assertIn("Saved file", result.rows[0].errormessage)

    @patch("sql.offlinedownload.DynamicStorage")
    def test_offline_file_download_error(self, mock_storage):
        """
        Test error handling during file download.
        """

        # Clear existing audit logs.
        AuditEntry.objects.all().delete()

        # Configure mock.
        mock_storage_instance = MagicMock()
        mock_storage.return_value = mock_storage_instance
        mock_storage_instance.exists.return_value = False  # File does not exist.

        # Build request.
        request = HttpRequest()
        request.GET = {"file_name": "missing.zip", "workflow_id": "123"}
        request.method = "GET"
        request.user = self.superuser

        # Execute test.
        response = offline_file_download(request)

        # Verify response.
        self.assertEqual(response.status_code, 404)
        self.assertEqual(json.loads(response.content)["error"], "File does not exist")

        # Verify audit log.
        audit_entry = AuditEntry.objects.last()
        self.assertIsNotNone(audit_entry)
        self.assertEqual(audit_entry.action, "Offline download")
        self.assertIn(
            "Workflow ID: 123, file: missing.zip, error: file does not exist.",
            audit_entry.extra_info,
        )
        self.assertEqual(audit_entry.user_id, self.superuser.id)
