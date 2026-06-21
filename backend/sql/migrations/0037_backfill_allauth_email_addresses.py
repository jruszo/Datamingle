from django.db import migrations


def backfill_email_addresses(apps, schema_editor):
    Users = apps.get_model("sql", "Users")
    EmailAddress = apps.get_model("account", "EmailAddress")
    db_alias = schema_editor.connection.alias
    seen_emails = set()

    users = (
        Users.objects.using(db_alias)
        .exclude(email__isnull=True)
        .exclude(email="")
        .order_by("id")
    )
    for user in users:
        email = str(user.email).strip().lower()
        if not email or email in seen_emails:
            continue
        seen_emails.add(email)

        existing = (
            EmailAddress.objects.using(db_alias).filter(email__iexact=email).first()
        )
        if existing:
            if existing.user_id == user.pk:
                existing.email = email
                existing.verified = True
                existing.primary = True
                existing.save(
                    using=db_alias,
                    update_fields=["email", "verified", "primary"],
                )
            continue

        has_primary = (
            EmailAddress.objects.using(db_alias)
            .filter(
                user_id=user.pk,
                primary=True,
            )
            .exists()
        )
        EmailAddress.objects.using(db_alias).create(
            user_id=user.pk,
            email=email,
            verified=True,
            primary=not has_primary,
        )


class Migration(migrations.Migration):
    dependencies = [
        ("account", "0001_initial"),
        ("sql", "0036_restrict_team_permission_levels"),
    ]

    operations = [
        migrations.RunPython(backfill_email_addresses, migrations.RunPython.noop),
    ]
