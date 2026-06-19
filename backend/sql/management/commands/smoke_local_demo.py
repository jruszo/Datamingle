from django.core.management.base import BaseCommand, CommandError
from sql.engines import get_engine
from sql.local_demo import (
    DEMO_INSTANCES,
    DEMO_TEAMS,
    managed_demo_instance_names,
    managed_demo_team_names,
    managed_demo_usernames,
)
from common.utils.const import WorkflowType
from sql.utils.sql_utils import filter_db_list
from sql.models import Instance, Team, Users, WorkflowAuditSetting
from sql.utils.team import permission_group_label, normalize_permission_group_sequence


class Command(BaseCommand):
    help = "Run manual local-demo smoke checks without reseeding data."

    def handle(self, *args, **options):
        self.stdout.write("Running local demo smoke checks")

        legacy_users = Users.objects.filter(username__in=managed_demo_usernames())
        if legacy_users.exists():
            raise CommandError(
                "Legacy seeded demo users still exist: {}".format(
                    ", ".join(sorted(legacy_users.values_list("username", flat=True)))
                )
            )
        self.stdout.write("Legacy demo users removed")

        teams = {
            group.team_name: group
            for group in Team.objects.filter(
                team_name__in=managed_demo_team_names(), is_deleted=0
            )
        }
        missing_groups = sorted(set(managed_demo_team_names()) - set(teams.keys()))
        if missing_groups:
            raise CommandError(
                "Missing demo teams: {}".format(", ".join(missing_groups))
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

        for team_config in DEMO_TEAMS.values():
            team = teams[team_config["team_name"]]
            expected_display = " -> ".join(
                permission_group_label(role)
                for role in normalize_permission_group_sequence(
                    team_config["approval_groups"]
                )
            )
            audit_setting = WorkflowAuditSetting.objects.filter(
                team_id=team.team_id,
                workflow_type=WorkflowType.SQL_REVIEW,
            ).first()
            actual_display = ""
            if audit_setting:
                actual_display = " -> ".join(
                    permission_group_label(role)
                    for role in normalize_permission_group_sequence(
                        audit_setting.audit_auth_groups
                    )
                )
            if actual_display != expected_display:
                raise CommandError(
                    "Unexpected approval preview for {}: expected '{}', got '{}'".format(
                        team.team_name,
                        expected_display,
                        actual_display,
                    )
                )
            self.stdout.write(f"Approval preview OK: {team.team_name}")

        for instance_config in DEMO_INSTANCES.values():
            instance = instances[instance_config["instance_name"]]
            engine = get_engine(instance=instance)
            connection_result = engine.test_connection()
            if getattr(connection_result, "error", ""):
                raise CommandError(
                    f"Connection failed for {instance.instance_name}: {connection_result.error}"
                )
            databases = engine.get_all_databases()
            db_names = filter_db_list(
                db_list=databases.rows,
                db_name_regex=engine.instance.show_db_name_regex,
                is_match_regex=True,
            )
            db_names = filter_db_list(
                db_list=db_names,
                db_name_regex=engine.instance.denied_db_name_regex,
                is_match_regex=False,
            )
            db_names = set(str(item) for item in db_names)
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
