import base64
import hashlib
import os
import re
from functools import lru_cache

from cryptography.fernet import Fernet, InvalidToken, MultiFernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from django.conf import settings
from django.utils.encoding import force_bytes, force_str

from common.utils.aes_decryptor import Prpcrypt

ENCRYPTED_VALUE_PREFIX = "enc1:"
HEX_RE = re.compile(r"^[0-9a-fA-F]+$")


def generate_field_encryption_key():
    return Fernet.generate_key().decode("ascii")


def is_encrypted_value(value):
    return isinstance(value, str) and value.startswith(ENCRYPTED_VALUE_PREFIX)


def encrypt_value(value):
    if value is None:
        return None
    if is_encrypted_value(value):
        decrypt_value(value)
        return value
    token = get_multi_fernet().encrypt(force_bytes(value))
    return f"{ENCRYPTED_VALUE_PREFIX}{force_str(token)}"


def decrypt_value(value):
    if value is None:
        return None
    if not isinstance(value, str):
        return value
    if is_encrypted_value(value):
        token = value[len(ENCRYPTED_VALUE_PREFIX) :]
        return force_str(get_multi_fernet().decrypt(force_bytes(token)))
    return decrypt_legacy_value(value)


def decrypt_legacy_value(value):
    if value in ("", None):
        return value

    mirage_value = _try_decrypt_mirage(value)
    if mirage_value is not None:
        return mirage_value

    old_value = _try_decrypt_old_hex(value)
    if old_value is not None:
        return old_value

    return value


@lru_cache(maxsize=1)
def get_multi_fernet():
    keys = _load_field_encryption_keys()
    if not keys:
        raise ValueError(
            "FIELD_ENCRYPTION_KEYS must be configured before writing encrypted values."
        )
    return MultiFernet([Fernet(key) for key in keys])


def _load_field_encryption_keys():
    raw_keys = getattr(settings, "FIELD_ENCRYPTION_KEYS", "") or os.environ.get(
        "FIELD_ENCRYPTION_KEYS", ""
    )
    keys = []
    for raw_key in raw_keys.split(","):
        key = raw_key.strip()
        if not key:
            continue
        try:
            Fernet(key)
        except Exception as exc:
            raise ValueError("Invalid FIELD_ENCRYPTION_KEYS entry.") from exc
        keys.append(key.encode("ascii"))

    legacy_key = _derive_secret_key_fernet_key()
    if not keys:
        return [legacy_key]
    if legacy_key not in keys:
        keys.append(legacy_key)
    return keys


def _derive_secret_key_fernet_key():
    secret_key = getattr(settings, "SECRET_KEY", "") or os.environ.get("SECRET_KEY", "")
    if not secret_key:
        raise ValueError(
            "SECRET_KEY or FIELD_ENCRYPTION_KEYS must be configured for encryption."
        )
    digest = hashlib.sha256(force_bytes(secret_key)).digest()
    return base64.urlsafe_b64encode(digest)


def _try_decrypt_mirage(value):
    try:
        key = getattr(settings, "MIRAGE_SECRET_KEY", None) or getattr(
            settings, "SECRET_KEY"
        )
        if not key:
            return None
        cipher_key = base64.urlsafe_b64encode(force_bytes(key))[:32]
        if len(cipher_key) not in (16, 24, 32):
            return None
        cipher_mode = getattr(settings, "MIRAGE_CIPHER_MODE", "ECB")
        iv = force_bytes(getattr(settings, "MIRAGE_CIPHER_IV", "1234567890abcdef"))
        encrypted = base64.urlsafe_b64decode(force_bytes(value))
        if cipher_mode == "CBC":
            mode = modes.CBC(iv)
        else:
            mode = modes.ECB()
        decryptor = Cipher(
            algorithms.AES(cipher_key), mode, default_backend()
        ).decryptor()
        unpadder = padding.PKCS7(algorithms.AES(cipher_key).block_size).unpadder()
        plaintext = decryptor.update(encrypted) + decryptor.finalize()
        unpadded = unpadder.update(plaintext) + unpadder.finalize()
        return force_str(unpadded)
    except Exception:
        return None


def _try_decrypt_old_hex(value):
    if len(value) < 32 or len(value) % 2 != 0 or HEX_RE.fullmatch(value) is None:
        return None
    try:
        return Prpcrypt().decrypt(value)
    except Exception:
        return None
