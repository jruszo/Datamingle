import os
from unittest.mock import patch

from django.test import SimpleTestCase, override_settings

from common.encryption import (
    DecryptionError,
    ENCRYPTED_VALUE_PREFIX,
    decrypt_value,
    encrypt_value,
    generate_field_encryption_key,
    get_multi_fernet,
)
from common.test_fixtures import LEGACY_MIRAGE_CIPHERTEXTS, LEGACY_MIRAGE_SECRET_KEY
from common.utils.aes_decryptor import Prpcrypt


def _encrypt_legacy_mirage(value, secret_key):
    assert secret_key == LEGACY_MIRAGE_SECRET_KEY
    return LEGACY_MIRAGE_CIPHERTEXTS[value]


class EncryptionHelpersTest(SimpleTestCase):
    def tearDown(self):
        get_multi_fernet.cache_clear()

    @override_settings(FIELD_ENCRYPTION_KEYS=generate_field_encryption_key())
    def test_round_trip_encrypts_with_prefix(self):
        get_multi_fernet.cache_clear()

        encrypted = encrypt_value("top-secret")

        self.assertTrue(encrypted.startswith(ENCRYPTED_VALUE_PREFIX))
        self.assertEqual(decrypt_value(encrypted), "top-secret")

    @override_settings(FIELD_ENCRYPTION_KEYS=generate_field_encryption_key())
    def test_prefix_looking_plaintext_is_still_encrypted(self):
        get_multi_fernet.cache_clear()

        encrypted = encrypt_value("enc1:not-a-token")

        self.assertNotEqual(encrypted, "enc1:not-a-token")
        self.assertEqual(decrypt_value(encrypted), "enc1:not-a-token")

    @override_settings(FIELD_ENCRYPTION_KEYS="")
    def test_encrypt_requires_configured_keys(self):
        get_multi_fernet.cache_clear()

        with patch.dict(os.environ, {"FIELD_ENCRYPTION_KEYS": ""}, clear=False):
            with self.assertRaises(ValueError):
                encrypt_value("top-secret")

    @override_settings(FIELD_ENCRYPTION_KEYS=generate_field_encryption_key())
    def test_decrypts_legacy_hex_ciphertext(self):
        get_multi_fernet.cache_clear()

        legacy = Prpcrypt().encrypt("legacy-password")

        self.assertEqual(decrypt_value(legacy), "legacy-password")

    @override_settings(
        FIELD_ENCRYPTION_KEYS=generate_field_encryption_key(),
        SECRET_KEY=LEGACY_MIRAGE_SECRET_KEY,
    )
    def test_decrypts_legacy_mirage_ciphertext(self):
        get_multi_fernet.cache_clear()

        legacy = _encrypt_legacy_mirage("legacy-user", LEGACY_MIRAGE_SECRET_KEY)

        self.assertEqual(decrypt_value(legacy), "legacy-user")

    def test_decrypt_legacy_value_raises_for_unknown_ciphertext(self):
        get_multi_fernet.cache_clear()

        with self.assertRaises(DecryptionError):
            decrypt_value("not-a-legacy-ciphertext")
