from django.core.validators import MaxLengthValidator
from django.db import models

from common.encryption import decrypt_value, encrypt_value


class EncryptedMixin(models.Field):
    def __init__(self, *args, **kwargs):
        self._plaintext_max_length = kwargs.get("max_length")
        super().__init__(*args, **kwargs)
        if self._plaintext_max_length:
            self.validators.append(MaxLengthValidator(self._plaintext_max_length))

    def get_prep_value(self, value):
        value = super().get_prep_value(value)
        if value is None:
            return None
        return encrypt_value(value)

    def from_db_value(self, value, expression, connection):
        if value is None:
            return None
        if value == "":
            return value
        return decrypt_value(value)

    def to_python(self, value):
        return super().to_python(value)


class EncryptedTextField(EncryptedMixin, models.TextField):
    pass


class EncryptedCharField(EncryptedMixin, models.TextField):
    pass
