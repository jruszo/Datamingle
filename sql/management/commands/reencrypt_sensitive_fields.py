from django.core.management.base import BaseCommand

from sql.models import (
    Config,
    Instance,
    InstanceAccount,
    TwoFactorAuthConfig,
)

MODEL_FIELDS = (
    (Instance, ("user", "password")),
    (InstanceAccount, ("password",)),
    (Config, ("value",)),
    (TwoFactorAuthConfig, ("phone", "secret_key")),
)


class Command(BaseCommand):
    help = "Rewrite sensitive fields into the current encrypted format."

    def add_arguments(self, parser):
        parser.add_argument("--batch-size", type=int, default=500)

    def handle(self, *args, **options):
        batch_size = options["batch_size"]
        total_rows = 0

        for model, fields in MODEL_FIELDS:
            queryset = model.objects.order_by("pk").iterator(chunk_size=batch_size)
            model_count = 0
            for obj in queryset:
                update_fields = [
                    field for field in fields if getattr(obj, field) not in (None, "")
                ]
                if not update_fields:
                    continue
                obj.save(update_fields=update_fields)
                model_count += 1
            total_rows += model_count
            self.stdout.write(f"{model.__name__}: rewritten {model_count} rows")

        self.stdout.write(self.style.SUCCESS(f"Rewrote {total_rows} rows in total"))
