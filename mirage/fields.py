from django.db import models


class EncryptedCharField(models.CharField):
    pass


class EncryptedTextField(models.TextField):
    pass


__all__ = ["EncryptedCharField", "EncryptedTextField"]
