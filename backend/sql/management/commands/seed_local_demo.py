from django.core.management.base import BaseCommand

from sql.local_demo import seed_local_demo


class Command(BaseCommand):
    help = "Seed idempotent local demo users, groups, resource groups, and instances."

    def handle(self, *args, **options):
        self.stdout.write("Seeding local demo environment")
        summary = seed_local_demo(self.stdout.write)
        self.stdout.write(
            self.style.SUCCESS(
                "Seeded demo data: {} auth groups, {} resource groups, {} users, {} instances".format(
                    len(summary["auth_groups"]),
                    len(summary["resource_groups"]),
                    len(summary["users"]),
                    len(summary["instances"]),
                )
            )
        )
