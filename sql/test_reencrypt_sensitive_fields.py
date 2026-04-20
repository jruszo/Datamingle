import importlib

from django.core.management import call_command
from django.db import connection
from django.test import SimpleTestCase, TestCase, override_settings

from common.encryption import (
    ENCRYPTED_VALUE_PREFIX,
    generate_field_encryption_key,
    get_multi_fernet,
)
from common.test_fixtures import (
    LEGACY_MIRAGE_CBC_CIPHERTEXTS,
    LEGACY_MIRAGE_CBC_IV,
    LEGACY_MIRAGE_CIPHERTEXTS,
    LEGACY_MIRAGE_SECRET_KEY,
)
from sql.models import Instance


def _encrypt_legacy_mirage(value, secret_key):
    # Pinned ciphertexts captured from the legacy Mirage wire format.
    assert secret_key == LEGACY_MIRAGE_SECRET_KEY
    return LEGACY_MIRAGE_CIPHERTEXTS[value]


@override_settings(
    FIELD_ENCRYPTION_KEYS=generate_field_encryption_key(),
    SECRET_KEY=LEGACY_MIRAGE_SECRET_KEY,
)
class ReencryptSensitiveFieldsCommandTest(TestCase):
    def setUp(self):
        get_multi_fernet.cache_clear()
        self.instance = Instance.objects.create(
            instance_name="legacy-instance",
            type="master",
            db_type="mysql",
            host="127.0.0.1",
            port=3306,
            user="root",
            password="plain-password",
        )

        legacy_user = _encrypt_legacy_mirage("legacy-root", LEGACY_MIRAGE_SECRET_KEY)
        legacy_password = _encrypt_legacy_mirage(
            "legacy-password", LEGACY_MIRAGE_SECRET_KEY
        )

        with connection.cursor() as cursor:
            cursor.execute(
                "UPDATE sql_instance SET user=%s, password=%s WHERE id=%s",
                [legacy_user, legacy_password, self.instance.id],
            )

    def tearDown(self):
        get_multi_fernet.cache_clear()

    def test_command_rewrites_legacy_values_to_new_prefix(self):
        call_command("reencrypt_sensitive_fields", batch_size=10)

        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT user, password FROM sql_instance WHERE id=%s",
                [self.instance.id],
            )
            raw_user, raw_password = cursor.fetchone()

        self.assertTrue(raw_user.startswith(ENCRYPTED_VALUE_PREFIX))
        self.assertTrue(raw_password.startswith(ENCRYPTED_VALUE_PREFIX))

        self.instance.refresh_from_db()

        self.assertEqual(self.instance.user, "legacy-root")
        self.assertEqual(self.instance.password, "legacy-password")


MIGRATION_0003 = importlib.import_module(
    "sql.migrations.0003_remove_resourcegroup_ding_webhook_and_more"
)


class Migration0003MirageDecryptTest(SimpleTestCase):
    @override_settings(
        MIRAGE_SECRET_KEY=LEGACY_MIRAGE_SECRET_KEY,
        MIRAGE_CIPHER_MODE="CBC",
        MIRAGE_CIPHER_IV=LEGACY_MIRAGE_CBC_IV,
    )
    def test_decrypt_mirage_supports_configured_cbc_mode(self):
        plaintext = MIGRATION_0003._decrypt_mirage(
            LEGACY_MIRAGE_CBC_CIPHERTEXTS["legacy-cbc-user"]
        )

        self.assertEqual(plaintext, "legacy-cbc-user")
