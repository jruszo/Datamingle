from django.core.management.base import BaseCommand

from sql.e2e_environment import seed_e2e_environment


class Command(BaseCommand):
    help = "Seed reproducible local E2E users and workflow fixtures."

    def handle(self, *args, **options):
        self.stdout.write("Seeding local E2E environment")
        summary = seed_e2e_environment(self.stdout.write)
        self.stdout.write(
            self.style.SUCCESS(
                "Seeded E2E data: {} users, {} scenario users".format(
                    len(summary["users"]),
                    len(summary["scenario_users"]),
                )
            )
        )
