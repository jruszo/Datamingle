from collections import OrderedDict

from django.contrib.auth.models import Permission

TEAM_PERMISSION_CATEGORIES = OrderedDict(
    {
        "Query": (
            "menu_query",
            "menu_sqlquery",
            "query_submit",
            "query_applypriv",
            "query_review",
            "query_download",
        ),
        "SQL workflows": (
            "menu_sqlcheck",
            "menu_sqlworkflow",
            "sql_submit",
            "sql_review",
            "sql_execute",
            "sql_execute_for_team",
        ),
        "Data export": (
            "menu_sqlexportworkflow",
            "sqlexport_submit",
            "offline_download",
        ),
        "Archives": (
            "menu_archive",
            "archive_apply",
            "archive_review",
            "archive_mgt",
        ),
        "Team management": (
            "view_team",
            "change_team",
        ),
    }
)

TEAM_PERMISSION_CODES = frozenset(
    code for codes in TEAM_PERMISSION_CATEGORIES.values() for code in codes
)


def assignable_team_permissions():
    return Permission.objects.filter(
        content_type__app_label="sql",
        codename__in=TEAM_PERMISSION_CODES,
    ).select_related("content_type")


def permission_catalog():
    permissions = {
        permission.codename: permission
        for permission in assignable_team_permissions().order_by("name", "codename")
    }
    return [
        {
            "category": category,
            "permissions": [
                {
                    "code": f"sql.{code}",
                    "codename": code,
                    "name": permissions[code].name,
                }
                for code in codes
                if code in permissions
            ],
        }
        for category, codes in TEAM_PERMISSION_CATEGORIES.items()
    ]


def normalize_permission_codes(permission_codes):
    normalized = {
        str(code).split(".", 1)[-1].strip() for code in permission_codes or []
    }
    invalid = normalized - TEAM_PERMISSION_CODES
    if invalid:
        raise ValueError(
            "Unsupported team permissions: {}".format(", ".join(sorted(invalid)))
        )
    return normalized
