import logging
from django.core.files.storage import FileSystemStorage
from storages.backends.s3boto3 import S3Boto3Storage
from storages.backends.azure_storage import AzureStorage
from storages.backends.sftpstorage import SFTPStorage
from common.config import SysConfig
import json

logger = logging.getLogger("default")


class DynamicStorage:
    """Dynamic storage adapter selecting backend from configuration."""

    def __init__(self, storage_type=None, config_dict=None):
        """Upload and download files using configured storage service."""

        # Get system configuration.
        self.config = config_dict or SysConfig()

        # Storage type.
        self.storage_type = self.config.get("storage_type", "local")

        # Local storage configuration.
        self.local_path = self.config.get("local_path", "downloads/DataExportFile/")

        # SFTP storage configuration.
        self.sftp_host = self.config.get("sftp_host", "")
        self.sftp_user = self.config.get("sftp_user", "")
        self.sftp_password = self.config.get("sftp_password", "")
        self.sftp_port = self.config.get("sftp_port", "")
        self.sftp_path = self.config.get("sftp_path", "")
        self.sftp_custom_params_str = self.config.get("sftp_custom_params", "")

        # S3-compatible storage configuration.
        self.s3c_access_key_id = self.config.get("s3c_access_key_id", "")
        self.s3c_access_key_secret = self.config.get("s3c_access_key_secret", "")
        self.s3c_endpoint = self.config.get("s3c_endpoint", "")
        self.s3c_bucket_name = self.config.get("s3c_bucket_name", "")
        self.s3c_region = self.config.get("s3c_region", "")
        self.s3c_path = self.config.get("s3c_path", "")
        self.s3c_custom_params_str = self.config.get("s3c_custom_params", "")

        # Azure Blob storage configuration.
        self.azure_account_name = self.config.get("azure_account_name", "")
        self.azure_account_key = self.config.get("azure_account_key", "")
        self.azure_container = self.config.get("azure_container", "")
        self.azure_path = self.config.get("azure_path", "")
        self.azure_custom_params_str = self.config.get("azure_custom_params", "")

        self.storage = self._init_storage()

        self.open = self.storage.open
        self.exists = self.storage.exists
        self.size = self.storage.size
        self.delete = self.storage.delete
        self.url = self.storage.url
        self.save = self.storage.save

        if hasattr(self.storage, "close"):
            self.close = self.storage.close
        else:
            self.close = lambda: None

    def _init_storage(self):
        """Initialize storage backend according to configuration."""
        storage_backends = {
            "local": self._init_local_storage,
            "sftp": self._init_sftp_storage,
            "s3c": self._init_s3c_storage,
            "azure": self._init_azure_storage,
        }

        init_func = storage_backends.get(self.storage_type)
        if init_func:
            return init_func()
        raise ValueError(f"Unsupported storage type: {self.storage_type}")

    def _init_local_storage(self):
        # Base parameters.
        local_params = {
            "location": str(self.local_path),
            "base_url": f"{self.local_path}",
        }

        return FileSystemStorage(**local_params)

    def _init_sftp_storage(self):
        # Base parameters.
        sftp_params = {
            "host": self.sftp_host,
            "params": {
                "username": self.sftp_user,
                "password": self.sftp_password,
                "port": self.sftp_port,
            },
            "root_path": self.sftp_path,
        }

        if self.sftp_custom_params_str.strip():
            try:
                self.sftp_custom_params = json.loads(self.sftp_custom_params_str)
            except json.JSONDecodeError:
                # Use empty dict for invalid JSON.
                self.sftp_custom_params = {}
        else:
            # Empty string means empty dict.
            self.sftp_custom_params = {}

        # Apply custom parameters.
        sftp_params.update(self.sftp_custom_params)

        return SFTPStorage(**sftp_params)

    def _init_s3c_storage(self):
        """
        S3-compatible storage.
        Verified with Alibaba Cloud OSS:
        - ``addressing_style`` must be ``virtual`` or connection fails.
        - ``endpoint`` must use ``http://``; otherwise save may fail with
          aws-chunked encoding and x-amz-content-sha256 related errors.
        """

        # Base parameters.
        s3c_params = {
            "access_key": self.s3c_access_key_id,
            "secret_key": self.s3c_access_key_secret,
            "bucket_name": self.s3c_bucket_name,
            **({"region_name": self.s3c_region} if self.s3c_region else {}),
            "endpoint_url": self.s3c_endpoint,
            "location": self.s3c_path,
            "file_overwrite": False,
        }

        if self.s3c_custom_params_str.strip():
            try:
                self.s3c_custom_params = json.loads(self.s3c_custom_params_str)
            except json.JSONDecodeError:
                # Use empty dict for invalid JSON.
                self.s3c_custom_params = {}
        else:
            # Empty string means empty dict.
            self.s3c_custom_params = {}

        # Apply custom parameters.
        s3c_params.update(self.s3c_custom_params)

        return S3Boto3Storage(**s3c_params)

    def _init_azure_storage(self):
        # Base parameters.
        azure_params = {
            "account_name": self.azure_account_name,
            "account_key": self.azure_account_key,
            "azure_container": self.azure_container,
            "location": self.azure_path,
        }

        if self.azure_custom_params_str.strip():
            try:
                self.azure_custom_params = json.loads(self.azure_custom_params_str)
            except json.JSONDecodeError:
                # Use empty dict for invalid JSON.
                self.azure_custom_params = {}
        else:
            # Empty string means empty dict.
            self.azure_custom_params = {}

        # Apply custom parameters.
        azure_params.update(self.azure_custom_params)

        return AzureStorage(**azure_params)

    def check_connection(self):
        """Check storage connection and return ``(status, error_message)``."""
        # Local storage is considered available by default.
        if self.storage_type == "local":
            return True, "Local storage connection succeeded"

        connection_checks = {
            "sftp": self._check_sftp_connection,
            "s3c": self._check_s3c_connection,
            "azure": self._check_azure_connection,
        }

        check_func = connection_checks.get(self.storage_type)
        if check_func:
            return check_func()

        # Unsupported storage type.
        return False, f"Unsupported storage type: {self.storage_type}"

    def _check_sftp_connection(self):
        """Check SFTP connection."""
        try:
            with self.storage as s:
                s.listdir(".")
            return True, "SFTP connection succeeded"
        except Exception as e:
            return False, f"SFTP connection failed: {str(e)}"

    def _check_s3c_connection(self):
        """Check S3-compatible storage connection."""
        try:
            client = self.storage.connection.meta.client
            client.head_bucket(Bucket=self.storage.bucket_name)
            return True, "S3 storage connection succeeded"
        except Exception as e:
            return False, f"S3 storage connection failed: {str(e)}"

    def _check_azure_connection(self):
        """Check Azure Blob storage connection."""
        try:
            container_client = self.storage.client
            container_client.get_container_properties()
            return True, "Azure Blob storage connection succeeded"
        except Exception as e:
            return False, f"Azure Blob storage connection failed: {str(e)}"
