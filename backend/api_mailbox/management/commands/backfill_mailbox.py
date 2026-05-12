from django.core.management.base import BaseCommand

from sql.mailbox import backfill_mailbox_notifications


class Command(BaseCommand):
    help = "Backfill active mailbox notifications for pending approvals and execution-needed items."

    def handle(self, *args, **options):
        backfill_mailbox_notifications()
        self.stdout.write(
            self.style.SUCCESS("Mailbox backfill completed successfully.")
        )
