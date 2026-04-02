import base64

from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from django.conf import settings
from django.core.management import call_command
from django.db import connection
from django.test import TestCase, override_settings
from django.utils.encoding import force_bytes, force_str

from common.encryption import (
    ENCRYPTED_VALUE_PREFIX,
    generate_field_encryption_key,
    get_multi_fernet,
)
from sql.models import CloudAccessKey, Instance


def _encrypt_legacy_mirage(value, secret_key):
    cipher_key = base64.urlsafe_b64encode(force_bytes(secret_key))[:32]
    cipher = Cipher(algorithms.AES(cipher_key), modes.ECB()).encryptor()
    padder = padding.PKCS7(algorithms.AES(cipher_key).block_size).padder()
    padded = padder.update(force_bytes(value)) + padder.finalize()
    encrypted = cipher.update(padded) + cipher.finalize()
    return force_str(base64.urlsafe_b64encode(encrypted))


@override_settings(FIELD_ENCRYPTION_KEYS=generate_field_encryption_key())
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
        self.access_key = CloudAccessKey.objects.create(
            type="aliyun",
            key_id="plain-ak",
            key_secret="plain-sk",
        )

        legacy_user = _encrypt_legacy_mirage("legacy-root", settings.SECRET_KEY)
        legacy_password = _encrypt_legacy_mirage("legacy-password", settings.SECRET_KEY)
        legacy_key_id = _encrypt_legacy_mirage("legacy-ak", settings.SECRET_KEY)
        legacy_key_secret = _encrypt_legacy_mirage("legacy-sk", settings.SECRET_KEY)

        with connection.cursor() as cursor:
            cursor.execute(
                "UPDATE sql_instance SET user=%s, password=%s WHERE id=%s",
                [legacy_user, legacy_password, self.instance.id],
            )
            cursor.execute(
                "UPDATE cloud_access_key SET key_id=%s, key_secret=%s WHERE id=%s",
                [legacy_key_id, legacy_key_secret, self.access_key.id],
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
            cursor.execute(
                "SELECT key_id, key_secret FROM cloud_access_key WHERE id=%s",
                [self.access_key.id],
            )
            raw_key_id, raw_key_secret = cursor.fetchone()

        self.assertTrue(raw_user.startswith(ENCRYPTED_VALUE_PREFIX))
        self.assertTrue(raw_password.startswith(ENCRYPTED_VALUE_PREFIX))
        self.assertTrue(raw_key_id.startswith(ENCRYPTED_VALUE_PREFIX))
        self.assertTrue(raw_key_secret.startswith(ENCRYPTED_VALUE_PREFIX))

        self.instance.refresh_from_db()
        self.access_key.refresh_from_db()

        self.assertEqual(self.instance.user, "legacy-root")
        self.assertEqual(self.instance.password, "legacy-password")
        self.assertEqual(self.access_key.key_id, "legacy-ak")
        self.assertEqual(self.access_key.key_secret, "legacy-sk")
