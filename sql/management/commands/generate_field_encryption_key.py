from django.core.management.base import BaseCommand

from common.encryption import generate_field_encryption_key


class Command(BaseCommand):
    help = "Generate a FIELD_ENCRYPTION_KEYS Fernet key."

    def handle(self, *args, **options):
        self.stdout.write(generate_field_encryption_key())
