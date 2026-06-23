"""Supplementary tests for models.py."""

from django.conf import settings


def test_password_mixin_import_error():
    settings.PASSWORD_MIXIN_PATH = "sql.not_found:ErrorMixin"
    from sql.models import PasswordMixin

    assert PasswordMixin.__name__ == "DummyMixin"
