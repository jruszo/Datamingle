from unittest.mock import patch, MagicMock, call
import unittest
from parameterized import parameterized
from sql.storage import DynamicStorage
import json


class TestDynamicStorage(unittest.TestCase):
    """
    Tests behavior of the DynamicStorage class.
    """

    # Mapping from storage type to backend class.
    storage_classes = {
        "local": "FileSystemStorage",
        "sftp": "SFTPStorage",
        "s3c": "S3Boto3Storage",
        "azure": "AzureStorage",
    }

    def setUp(self):
        """Shared configuration data."""
        self.local_config = {
            "storage_type": "local",
            "local_path": "/tmp/files/",
        }

        self.sftp_config = {
            "storage_type": "sftp",
            "sftp_host": "sftp.example.com",
            "sftp_user": "user",
            "sftp_password": "pass",
            "sftp_port": 22,
            "sftp_path": "/uploads/",
        }

        self.s3c_config = {
            "storage_type": "s3c",
            "s3c_access_key_id": "AKIA...",
            "s3c_access_key_secret": "secret",
            "s3c_endpoint": "http://s3.example.com",
            "s3c_bucket_name": "my-bucket",
            "s3c_region": "us-east-1",
            "s3c_path": "data/",
        }

        self.azure_config = {
            "storage_type": "azure",
            "azure_account_name": "myaccount",
            "azure_account_key": "azurekey",
            "azure_container": "container",
            "azure_path": "azure-data/",
        }

    @parameterized.expand(
        [
            (
                "local",
                "FileSystemStorage",
                {
                    "location": "/tmp/files/",
                    "base_url": "/tmp/files/",
                },
            ),
            (
                "sftp",
                "SFTPStorage",
                {
                    "host": "sftp.example.com",
                    "params": {
                        "username": "user",
                        "password": "pass",
                        "port": 22,
                    },
                    "root_path": "/uploads/",
                },
            ),
            (
                "s3c",
                "S3Boto3Storage",
                {
                    "access_key": "AKIA...",
                    "secret_key": "secret",
                    "bucket_name": "my-bucket",
                    "region_name": "us-east-1",
                    "endpoint_url": "http://s3.example.com",
                    "location": "data/",
                    "file_overwrite": False,
                },
            ),
            (
                "azure",
                "AzureStorage",
                {
                    "account_name": "myaccount",
                    "account_key": "azurekey",
                    "azure_container": "container",
                    "location": "azure-data/",
                },
            ),
        ]
    )
    def test_storage_initialization(self, storage_type, storage_class, expected_kwargs):
        """Parameterized test for storage backend initialization."""
        config_dict = getattr(self, f"{storage_type}_config")

        with patch(f"sql.storage.{storage_class}") as mock_storage:
            DynamicStorage(config_dict=config_dict)

            # Verify the correct storage class was called.
            mock_storage.assert_called_once()

            # Verify call arguments.
            actual_kwargs = mock_storage.call_args[1]
            self.assertDictEqual(actual_kwargs, expected_kwargs)

    @parameterized.expand(
        [
            ("save", "test.txt", "content"),
            ("open", "test.txt", "rb"),
            ("delete", "test.txt", None),
            ("exists", "test.txt", None),
            ("size", "test.txt", None),
        ]
    )
    def test_method_proxying(self, method, filename, content):
        """Test method proxying to the underlying storage."""
        mock_storage = MagicMock()
        with patch.object(DynamicStorage, "_init_storage", return_value=mock_storage):
            storage = DynamicStorage(config_dict=self.local_config)

            # Call proxied method.
            method_call = getattr(storage, method)
            if content:
                method_call(filename, content)
            else:
                method_call(filename)

            # Verify the underlying method was called.
            underlying_call = getattr(mock_storage, method)
            if content:
                underlying_call.assert_called_once_with(filename, content)
            else:
                underlying_call.assert_called_once_with(filename)

    def test_close_behavior(self):
        """Test two close() scenarios."""
        # Scenario 1: underlying storage has close().
        mock_storage_with_close = MagicMock()
        mock_storage_with_close.close = MagicMock()

        with patch.object(
            DynamicStorage, "_init_storage", return_value=mock_storage_with_close
        ):
            storage = DynamicStorage(config_dict=self.sftp_config)
            storage.close()
            mock_storage_with_close.close.assert_called_once()

        # Scenario 2: underlying storage has no close().
        mock_storage_without_close = MagicMock()
        delattr(mock_storage_without_close, "close")

        with patch.object(
            DynamicStorage, "_init_storage", return_value=mock_storage_without_close
        ):
            storage = DynamicStorage(config_dict=self.local_config)
            try:
                storage.close()  # Should not raise.
            except Exception as e:
                self.fail(f"close() raised {e}")

    def test_storage_operation_exceptions(self):
        """Test exception handling for storage operations."""
        for method in ["open", "save", "delete", "exists", "size"]:
            with self.subTest(method=method):
                with patch.object(DynamicStorage, "_init_storage") as mock_init:
                    mock_storage = MagicMock()
                    mock_init.return_value = mock_storage

                    # Simulate an exception from the underlying storage.
                    getattr(mock_storage, method).side_effect = Exception(
                        "storage error"
                    )

                    storage = DynamicStorage(config_dict=self.local_config)

                    with self.assertRaises(Exception) as context:
                        if method == "save":
                            getattr(storage, method)("test.txt", "content")
                        else:
                            getattr(storage, method)("test.txt")

                    self.assertIn("storage error", str(context.exception))

    def test_init_unsupported_storage_type(self):
        """Test ValueError for unsupported storage type."""
        config = {"storage_type": "unsupported"}
        with self.assertRaises(ValueError) as context:
            DynamicStorage(config_dict=config)
        self.assertIn("Unsupported storage type", str(context.exception))

    def test_check_connection_local(self):
        """Test local storage connection check."""
        storage = DynamicStorage(config_dict=self.local_config)
        success, msg = storage.check_connection()
        self.assertTrue(success)
        self.assertEqual(msg, "Local storage connection succeeded")

    def test_check_connection_sftp(self):
        """Test SFTP connection check."""
        with patch("sql.storage.SFTPStorage") as mock_sftp_class:
            # Full context manager mock.
            mock_context = MagicMock()
            mock_context.listdir.return_value = ([], [])

            mock_sftp_instance = MagicMock()
            mock_sftp_instance.__enter__.return_value = mock_context
            mock_sftp_instance.__exit__.return_value = None
            mock_sftp_class.return_value = mock_sftp_instance

            # Success path.
            storage = DynamicStorage(config_dict=self.sftp_config)
            success, msg = storage.check_connection()
            mock_context.listdir.assert_called_once_with(".")
            self.assertTrue(success)
            self.assertEqual(msg, "SFTP connection succeeded")

            # Failure path.
            mock_context.listdir.side_effect = Exception("SFTP connection failure")
            success, msg = storage.check_connection()
            self.assertFalse(success)
            self.assertIn("SFTP connection failure", msg)

    def test_check_connection_s3c(self):
        """Test S3-compatible storage connection check."""
        with patch("sql.storage.S3Boto3Storage") as mock_s3_class:
            mock_s3_instance = MagicMock()
            mock_s3_class.return_value = mock_s3_instance

            # Set bucket_name attribute.
            mock_s3_instance.bucket_name = "my-bucket"

            # Success path.
            mock_client = MagicMock()
            mock_s3_instance.connection.meta.client = mock_client

            storage = DynamicStorage(config_dict=self.s3c_config)
            success, msg = storage.check_connection()
            mock_client.head_bucket.assert_called_once_with(Bucket="my-bucket")
            self.assertTrue(success)
            self.assertEqual(msg, "S3 storage connection succeeded")

            # Failure path.
            mock_client.head_bucket.side_effect = Exception("Bucket does not exist")
            success, msg = storage.check_connection()
            self.assertFalse(success)
            self.assertIn("Bucket does not exist", msg)

    def test_check_connection_azure(self):
        """Test Azure Blob storage connection check."""
        with patch("sql.storage.AzureStorage") as mock_azure_class:
            mock_azure_instance = MagicMock()
            mock_azure_class.return_value = mock_azure_instance

            # Success path.
            mock_client = MagicMock()
            mock_azure_instance.client = mock_client

            storage = DynamicStorage(config_dict=self.azure_config)
            success, msg = storage.check_connection()
            mock_client.get_container_properties.assert_called_once()
            self.assertTrue(success)
            self.assertEqual(msg, "Azure Blob storage connection succeeded")

            # Failure path.
            mock_client.get_container_properties.side_effect = Exception(
                "Container does not exist"
            )
            success, msg = storage.check_connection()
            self.assertFalse(success)
            self.assertIn("Container does not exist", msg)

    @parameterized.expand(
        [
            ("sftp", "sftp_custom_params", '{"timeout": 30}'),
            ("s3c", "s3c_custom_params", '{"addressing_style": "virtual"}'),
            ("azure", "azure_custom_params", '{"max_connections": 10}'),
        ]
    )
    def test_custom_json_params(self, storage_type, param_name, json_value):
        """Test handling of custom JSON parameters."""
        config_dict = getattr(self, f"{storage_type}_config").copy()
        config_dict[param_name] = json_value

        with patch(f"sql.storage.{self.storage_classes[storage_type]}") as mock_storage:
            DynamicStorage(config_dict=config_dict)
            call_args = mock_storage.call_args[1]
            expected_value = json.loads(json_value)
            for key, value in expected_value.items():
                self.assertEqual(call_args[key], value)

    @parameterized.expand(
        [
            ("sftp", "sftp_custom_params"),
            ("s3c", "s3c_custom_params"),
            ("azure", "azure_custom_params"),
        ]
    )
    def test_invalid_json_params(self, storage_type, param_name):
        """Test handling of invalid JSON parameters."""
        config_dict = getattr(self, f"{storage_type}_config").copy()
        config_dict[param_name] = "invalid{json"

        with patch(f"sql.storage.{self.storage_classes[storage_type]}") as mock_storage:
            try:
                storage = DynamicStorage(config_dict=config_dict)
                # Verify base parameters remain correctly set.
                if storage_type == "sftp":
                    self.assertEqual(storage.sftp_path, self.sftp_config["sftp_path"])
                elif storage_type == "s3c":
                    self.assertEqual(
                        storage.s3c_bucket_name, self.s3c_config["s3c_bucket_name"]
                    )
                else:
                    self.assertEqual(
                        storage.azure_container, self.azure_config["azure_container"]
                    )
            except json.JSONDecodeError:
                self.fail("Invalid JSON should be handled gracefully")

    @parameterized.expand(
        [
            ("sftp", "sftp_password", ""),
            ("s3c", "s3c_access_key_secret", ""),
            ("azure", "azure_account_key", ""),
        ]
    )
    def test_empty_config_values(self, storage_type, param_name, empty_value):
        """Test handling of empty configuration values."""
        config_dict = getattr(self, f"{storage_type}_config").copy()
        config_dict[param_name] = empty_value

        with patch(f"sql.storage.{self.storage_classes[storage_type]}") as mock_storage:
            DynamicStorage(config_dict=config_dict)
            call_args = mock_storage.call_args[1]

            if storage_type == "sftp":
                self.assertEqual(call_args["params"]["password"], "")
            elif storage_type == "s3c":
                self.assertEqual(call_args["secret_key"], "")
            else:
                self.assertEqual(call_args["account_key"], "")
