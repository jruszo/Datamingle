import datetime
import os

from allauth.account.models import EmailAddress
from django.contrib.auth.models import Group
from django.contrib.auth.models import Permission
from django.core.management.base import CommandError
from django.db import transaction
from django.db.models import Q

from common.utils.const import WorkflowType
from sql.local_demo import DEMO_TEAMS, seed_local_demo
from sql.models import (
    MailboxItem,
    MailboxSourceType,
    PermanentTeamGrant,
    PermissionRequest,
    Team,
    TeamMembership,
    TemporaryInstanceGrant,
    TemporaryTeamGrant,
    Users,
    WorkflowAudit,
    WorkflowAuditDetail,
    WorkflowAuditSetting,
    WorkflowLog,
)
from sql.utils.team import normalize_permission_group_sequence

E2E_PASSWORD = "SecurePass123!"
LOCAL_DEMO_SEED_ENV = "RUN_LOCAL_DEMO_SEED"

E2E_USERS = (
    {
        "username": "demo_admin",
        "email": "demo-admin@datamingle.dev",
        "display": "Demo Admin",
        "is_superuser": True,
        "direct_permissions": (),
        "memberships": (),
    },
    {
        "username": "demo_requester",
        "email": "demo-requester@datamingle.dev",
        "display": "Demo Requester",
        "is_superuser": False,
        "direct_permissions": (),
        "memberships": (
            ("Demo Workflow Single Stage", "RD"),
            ("Demo Workflow Multi Stage", "RD"),
        ),
    },
    {
        "username": "demo_pm",
        "email": "demo-pm@datamingle.dev",
        "display": "Demo PM",
        "is_superuser": False,
        "direct_permissions": (),
        "memberships": (("Demo Workflow Multi Stage", "PM"),),
    },
    {
        "username": "demo_dba",
        "email": "demo-dba@datamingle.dev",
        "display": "Demo DBA",
        "is_superuser": False,
        "direct_permissions": (),
        "memberships": (
            ("Demo Workflow Single Stage", "DBA"),
            ("Demo Workflow Multi Stage", "DBA"),
        ),
    },
    {
        "username": "e2e-admin@datamingle.dev",
        "email": "e2e-admin@datamingle.dev",
        "display": "E2E Admin",
        "is_superuser": True,
        "direct_permissions": (),
        "memberships": (),
    },
    {
        "username": "e2e-requester@datamingle.dev",
        "email": "e2e-requester@datamingle.dev",
        "display": "E2E Requester",
        "is_superuser": False,
        "direct_permissions": (
            "menu_queryapplylist",
            "query_applypriv",
        ),
        "memberships": (),
    },
    {
        "username": "e2e-reviewer@datamingle.dev",
        "email": "e2e-reviewer@datamingle.dev",
        "display": "E2E Reviewer",
        "is_superuser": False,
        "direct_permissions": (
            "menu_queryapplylist",
            "query_review",
        ),
        "memberships": (),
    },
)

E2E_SCENARIO_USERNAMES = (
    "e2e-requester@datamingle.dev",
    "e2e-reviewer@datamingle.dev",
)
E2E_USER_MANAGEMENT_EMAIL_PREFIX = "e2e-user-mgmt-"
E2E_USER_MANAGEMENT_EMAIL_SUFFIX = "@datamingle.dev"


def seed_e2e_environment(write_line=None):
    """Seed a reproducible local E2E environment after the Docker reset."""

    _ensure_local_e2e_seed_enabled()

    def log(message):
        if write_line:
            write_line(message)

    with transaction.atomic():
        seed_local_demo(write_line=write_line)
        _cleanup_permission_scenario(log)
        _cleanup_user_management_scenario(log)
        users = _seed_users(log)
        _seed_demo_memberships(users, log)
        _seed_access_request_audit_settings(log)

    return {
        "users": [user_config["username"] for user_config in E2E_USERS],
        "scenario_users": list(E2E_SCENARIO_USERNAMES),
    }


def _cleanup_permission_scenario(log):
    scenario_users = list(Users.objects.filter(username__in=E2E_SCENARIO_USERNAMES))
    request_ids = list(
        PermissionRequest.objects.filter(
            user_name__in=E2E_SCENARIO_USERNAMES
        ).values_list("request_id", flat=True)
    )
    audit_ids = list(
        WorkflowAudit.objects.filter(
            workflow_type=WorkflowType.ACCESS_REQUEST,
            workflow_id__in=request_ids,
        ).values_list("audit_id", flat=True)
    )

    MailboxItem.objects.filter(
        source_type=MailboxSourceType.PERMISSION_REQUEST,
        source_id__in=request_ids,
    ).delete()
    WorkflowLog.objects.filter(audit_id__in=audit_ids).delete()
    WorkflowAuditDetail.objects.filter(audit_id__in=audit_ids).delete()
    WorkflowAudit.objects.filter(audit_id__in=audit_ids).delete()
    grant_filter = _scenario_grant_filter(scenario_users, request_ids)
    if grant_filter:
        TemporaryTeamGrant.objects.filter(grant_filter).delete()
        TemporaryInstanceGrant.objects.filter(grant_filter).delete()
        PermanentTeamGrant.objects.filter(grant_filter).delete()
    PermissionRequest.objects.filter(request_id__in=request_ids).delete()
    TeamMembership.objects.filter(user__in=scenario_users).delete()

    log(
        "E2E permission scenario reset: {} requests, {} users".format(
            len(request_ids),
            len(scenario_users),
        )
    )


def _cleanup_user_management_scenario(log):
    user_filter = _user_management_e2e_user_filter()
    user_ids = list(Users.objects.filter(user_filter).values_list("id", flat=True))

    TeamMembership.objects.filter(user_id__in=user_ids).delete()
    EmailAddress.objects.filter(
        Q(
            email__startswith=E2E_USER_MANAGEMENT_EMAIL_PREFIX,
            email__endswith=E2E_USER_MANAGEMENT_EMAIL_SUFFIX,
        )
        | Q(user_id__in=user_ids)
    ).delete()
    Users.objects.filter(id__in=user_ids).delete()

    log("E2E user management reset: {} users".format(len(user_ids)))


def _ensure_local_e2e_seed_enabled():
    if os.environ.get(LOCAL_DEMO_SEED_ENV) != "1":
        raise CommandError(
            "seed_e2e_environment can only run when " f"{LOCAL_DEMO_SEED_ENV}=1 is set."
        )


def _user_management_e2e_user_filter():
    return Q(
        username__startswith=E2E_USER_MANAGEMENT_EMAIL_PREFIX,
        username__endswith=E2E_USER_MANAGEMENT_EMAIL_SUFFIX,
    ) | Q(
        email__startswith=E2E_USER_MANAGEMENT_EMAIL_PREFIX,
        email__endswith=E2E_USER_MANAGEMENT_EMAIL_SUFFIX,
    )


def _scenario_grant_filter(scenario_users, request_ids):
    grant_filter = Q()
    if scenario_users:
        grant_filter |= Q(user__in=scenario_users)
    if request_ids:
        grant_filter |= Q(source_request_id__in=request_ids)
    return grant_filter


def _seed_users(log):
    users = {}
    for user_config in E2E_USERS:
        user = _upsert_user(
            username=user_config["username"],
            email=user_config["email"],
            display=user_config["display"],
            is_superuser=user_config["is_superuser"],
            direct_permission_codenames=user_config["direct_permissions"],
        )
        users[user.username] = user
        log(f"E2E user seeded: {user.username}")
    return users


def _upsert_user(username, email, display, is_superuser, direct_permission_codenames):
    user, _ = Users.objects.get_or_create(username=username)
    user.email = email
    user.display = display
    user.is_active = True
    user.is_staff = is_superuser
    user.is_superuser = is_superuser
    if not user.check_password(E2E_PASSWORD):
        user.set_password(E2E_PASSWORD)
    user.save()

    EmailAddress.objects.filter(user=user).exclude(email=email).delete()
    EmailAddress.objects.update_or_create(
        user=user,
        email=email,
        defaults={
            "primary": True,
            "verified": True,
        },
    )
    direct_permission_codenames = tuple(direct_permission_codenames)
    permissions = Permission.objects.filter(
        content_type__app_label="sql",
        codename__in=direct_permission_codenames,
    )
    resolved_codenames = set(permissions.values_list("codename", flat=True))
    missing_codenames = sorted(set(direct_permission_codenames) - resolved_codenames)
    if missing_codenames:
        raise CommandError(
            "Missing E2E direct permissions: {}".format(", ".join(missing_codenames))
        )
    user.user_permissions.set(permissions)
    return user


def _seed_demo_memberships(users, log):
    e2e_usernames = {user_config["username"] for user_config in E2E_USERS}
    TeamMembership.objects.filter(user__username__in=e2e_usernames).delete()

    for user_config in E2E_USERS:
        user = users[user_config["username"]]
        for team_name, level_name in user_config["memberships"]:
            team = Team.objects.get(team_name=team_name, is_deleted=0)
            permission_level = Group.objects.get(name=level_name)
            TeamMembership.objects.update_or_create(
                user=user,
                team=team,
                defaults={"permission_level": permission_level},
            )
            log(f"E2E membership seeded: {user.username} -> {team_name} / {level_name}")


def _seed_access_request_audit_settings(log):
    for config in DEMO_TEAMS.values():
        team = Team.objects.get(team_name=config["team_name"], is_deleted=0)
        audit_auth_groups = ",".join(
            str(group_id)
            for group_id in normalize_permission_group_sequence(
                config["approval_groups"]
            )
        )
        WorkflowAuditSetting.objects.update_or_create(
            team_id=team.team_id,
            workflow_type=WorkflowType.ACCESS_REQUEST,
            defaults={
                "team_name": team.team_name,
                "audit_auth_groups": audit_auth_groups,
            },
        )
        log(
            "E2E access request approval setting: {} -> {}".format(
                team.team_name,
                " -> ".join(config["approval_groups"]),
            )
        )
