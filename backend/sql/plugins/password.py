"""
This module contains plugins related to password retrieval.

Plugins provide methods to get usernames and passwords for database connections.
When used together with Instance, plugins can read instance details via self.

Including name, type, and other fields, these values can be used to retrieve
database credentials.
"""

import time
import requests


class DummyMixin:
    """Mixin template that provides basic methods for other mixins.
    By default, username and password are read directly from schema.
    """

    def get_username_password(self):
        return self.user, self.password


password_cache = {
    "instance_name": {
        "username": "username",
        "password": "password",
        "expires_at": "1740557906.15272",
    }
}


class VaultMixin(DummyMixin):
    """
    Used together with sqlinstance.
    Fetches username and password from Vault using a localhost vault service.
    No token is used, which is suitable for vault-proxy deployments.
    For other deployment or secret strategies, inherit this mixin and override config.
    """

    vault_server = "localhost:8200"
    vault_token = ""

    def get_username_password(self):
        if self.instance_name in password_cache:
            if password_cache[self.instance_name]["expires_at"] > time.time():
                return (
                    password_cache[self.instance_name]["username"],
                    password_cache[self.instance_name]["password"],
                )

        vault_role = f"{self.instance_name}-archery-rw"
        response = requests.get(
            f"http://{self.vault_server}/v1/database/static-creds/{vault_role}",
            headers={"X-Vault-Token": self.vault_token},
        )
        response.raise_for_status()
        data = response.json()["data"]
        password_cache[self.instance_name] = {
            "username": data["username"],
            "password": data["password"],
            "expires_at": time.time() + data["ttl"] - 60,
        }
        return data["username"], data["password"]
