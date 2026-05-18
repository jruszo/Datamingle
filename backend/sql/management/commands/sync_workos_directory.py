from django.core.management.base import BaseCommand, CommandError

from common.authenticate.workos_directory import sync_directory


class Command(BaseCommand):
    help = "Reconcile Datamingle users and resource groups from a WorkOS Directory Sync directory."

    def add_arguments(self, parser):
        parser.add_argument(
            "--directory-id",
            required=True,
            help="WorkOS directory ID to sync, for example directory_01ECAZ...",
        )

    def handle(self, *args, **options):
        directory_id = options["directory_id"].strip()
        if not directory_id:
            raise CommandError("--directory-id cannot be blank.")

        result = sync_directory(directory_id)
        self.stdout.write(
            self.style.SUCCESS(
                "Synced WorkOS directory "
                f"{directory_id}: {result['users']} users, {result['groups']} groups, "
                f"{result['stale_users']} stale users, {result['stale_groups']} stale groups."
            )
        )
