import base64

from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from django.conf import settings
from django.test import SimpleTestCase, override_settings
from django.utils.encoding import force_bytes, force_str

from common.encryption import (
    ENCRYPTED_VALUE_PREFIX,
    decrypt_value,
    encrypt_value,
    generate_field_encryption_key,
    get_multi_fernet,
)
from common.utils.aes_decryptor import Prpcrypt


def _encrypt_legacy_mirage(value, secret_key):
    cipher_key = base64.urlsafe_b64encode(force_bytes(secret_key))[:32]
    cipher = Cipher(algorithms.AES(cipher_key), modes.ECB()).encryptor()
    padder = padding.PKCS7(algorithms.AES(cipher_key).block_size).padder()
    padded = padder.update(force_bytes(value)) + padder.finalize()
    encrypted = cipher.update(padded) + cipher.finalize()
    return force_str(base64.urlsafe_b64encode(encrypted))


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
    def test_decrypts_legacy_hex_ciphertext(self):
        get_multi_fernet.cache_clear()

        legacy = Prpcrypt().encrypt("legacy-password")

        self.assertEqual(decrypt_value(legacy), "legacy-password")

    @override_settings(FIELD_ENCRYPTION_KEYS=generate_field_encryption_key())
    def test_decrypts_legacy_mirage_ciphertext(self):
        get_multi_fernet.cache_clear()

        legacy = _encrypt_legacy_mirage("legacy-user", settings.SECRET_KEY)

        self.assertEqual(decrypt_value(legacy), "legacy-user")
