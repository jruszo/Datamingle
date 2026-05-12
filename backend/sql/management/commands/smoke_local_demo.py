from django.core.management.base import BaseCommand, CommandError
from rest_framework.test import APIClient

from sql.engines import get_engine
from sql.local_demo import (
    DEMO_INSTANCES,
    DEMO_RESOURCE_GROUPS,
    DEMO_USERS,
    managed_demo_instance_names,
    managed_demo_resource_group_names,
    managed_demo_usernames,
)
from sql.models import Instance, ResourceGroup, Users


def _response_data(response):
    payload = response.json()
    return payload.get("data", payload)


class Command(BaseCommand):
    help = "Run manual local-demo smoke checks without reseeding data."

    def handle(self, *args, **options):
        self.stdout.write("Running local demo smoke checks")

        users = {
            user.username: user
            for user in Users.objects.filter(username__in=managed_demo_usernames())
        }
        missing_users = sorted(set(managed_demo_usernames()) - set(users.keys()))
        if missing_users:
            raise CommandError(
                "Missing demo users: {}".format(", ".join(missing_users))
            )

        resource_groups = {
            group.group_name: group
            for group in ResourceGroup.objects.filter(
                group_name__in=managed_demo_resource_group_names(), is_deleted=0
            )
        }
        missing_groups = sorted(
            set(managed_demo_resource_group_names()) - set(resource_groups.keys())
        )
        if missing_groups:
            raise CommandError(
                "Missing demo resource groups: {}".format(", ".join(missing_groups))
            )

        instances = {
            instance.instance_name: instance
            for instance in Instance.objects.filter(
                instance_name__in=managed_demo_instance_names()
            ).prefetch_related("resource_group", "instance_tag")
        }
        missing_instances = sorted(
            set(managed_demo_instance_names()) - set(instances.keys())
        )
        if missing_instances:
            raise CommandError(
                "Missing demo instances: {}".format(", ".join(missing_instances))
            )

        client = APIClient()
        for username in DEMO_USERS.keys():
            client.force_authenticate(user=users[username])
            response = client.get("/api/v1/me/", format="json")
            if response.status_code != 200:
                raise CommandError(
                    f"Failed to load current-user context for {username}: {response.content}"
                )
            self.stdout.write(f"Current-user context OK: {username}")
        client.force_authenticate(user=None)

        requester = users["demo_requester"]
        client.force_authenticate(user=requester)
        response = client.get("/api/v1/workflow/submission-metadata/", format="json")
        if response.status_code != 200:
            raise CommandError(
                f"Submission metadata failed: {response.status_code} {response.content}"
            )
        metadata = _response_data(response)
        if len(metadata.get("instances", [])) < len(DEMO_INSTANCES):
            raise CommandError("Submission metadata is missing seeded demo instances.")
        self.stdout.write("Workflow submission metadata OK")

        response = client.get(
            "/api/v1/workflow/export/submission-metadata/", format="json"
        )
        if response.status_code != 200:
            raise CommandError(
                f"Export submission metadata failed: {response.status_code} {response.content}"
            )
        export_metadata = _response_data(response)
        if len(export_metadata.get("instances", [])) < 1:
            raise CommandError(
                "Export submission metadata is missing readable demo instances."
            )
        self.stdout.write("Export submission metadata OK")

        for resource_group_config in DEMO_RESOURCE_GROUPS.values():
            resource_group = resource_groups[resource_group_config["group_name"]]
            response = client.get(
                "/api/v1/workflow/approval-preview/",
                {"group_id": resource_group.group_id},
                format="json",
            )
            if response.status_code != 200:
                raise CommandError(
                    f"Approval preview failed for {resource_group.group_name}: {response.content}"
                )
            preview = _response_data(response)
            expected_display = " -> ".join(resource_group_config["approval_groups"])
            if preview.get("display") != expected_display:
                raise CommandError(
                    "Unexpected approval preview for {}: expected '{}', got '{}'".format(
                        resource_group.group_name,
                        expected_display,
                        preview.get("display"),
                    )
                )
            self.stdout.write(f"Approval preview OK: {resource_group.group_name}")

        dba_user = users["demo_dba"]
        if not dba_user.has_perm("sql.offline_download"):
            raise CommandError("Demo DBA is missing offline_download permission.")
        self.stdout.write("Export download permission OK: demo_dba")

        for instance_config in DEMO_INSTANCES.values():
            instance = instances[instance_config["instance_name"]]
            engine = get_engine(instance=instance)
            connection_result = engine.test_connection()
            if getattr(connection_result, "error", ""):
                raise CommandError(
                    f"Connection failed for {instance.instance_name}: {connection_result.error}"
                )
            response = client.get(
                "/api/v1/instance/resource/",
                {"instance_id": instance.id, "resource_type": "database"},
                format="json",
            )
            if response.status_code != 200:
                raise CommandError(
                    f"Database listing failed for {instance.instance_name}: {response.content}"
                )
            payload = _response_data(response)
            db_names = set(str(item) for item in payload.get("result", []))
            expected_db_names = set(instance_config["databases"])
            if not expected_db_names.issubset(db_names):
                raise CommandError(
                    "Database list for {} is missing expected names: {}".format(
                        instance.instance_name,
                        ", ".join(sorted(expected_db_names - db_names)),
                    )
                )
            self.stdout.write(f"Instance connectivity OK: {instance.instance_name}")

        self.stdout.write(self.style.SUCCESS("Local demo smoke checks passed"))
