import base64
import json
import logging
from dataclasses import dataclass
from typing import Tuple

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured, SuspiciousOperation

logger = logging.getLogger(__name__)


@dataclass
class WorkOSAuthenticationResult:
    user_id: str
    email: str
    first_name: str
    last_name: str
    profile_picture_url: str
    organization_id: str
    session_id: str
    role_slugs: Tuple[str, ...] = ()

    @property
    def display_name(self):
        return " ".join(
            value
            for value in (self.first_name.strip(), self.last_name.strip())
            if value
        ).strip()


def _dynamic_import_workos():
    try:
        from workos import WorkOSClient  # type: ignore
    except ImportError as exc:
        raise ImproperlyConfigured(
            "The 'workos' package is required for Datamingle authentication."
        ) from exc
    return WorkOSClient


def _get_attr(source, name, default=""):
    if isinstance(source, dict):
        return source.get(name, default)
    return getattr(source, name, default)


def _list_response_items(response):
    data = _get_attr(response, "data", None)
    if data is not None:
        return list(data)
    if isinstance(response, (list, tuple)):
        return list(response)
    return []


def _list_response_next_after(response, items):
    metadata = _get_attr(response, "list_metadata", None)
    after = _get_attr(metadata, "after", "") if metadata else ""
    if after:
        return str(after)

    has_more = bool(_get_attr(metadata, "has_more", False)) if metadata else False
    if has_more and items:
        return str(_get_attr(items[-1], "id", "") or "")
    return ""


def _decode_workos_access_token(token):
    segments = token.split(".")
    if len(segments) != 3:
        raise SuspiciousOperation("WorkOS access token is not a JWT.")

    payload = segments[1]
    payload += "=" * ((4 - len(payload) % 4) % 4)
    try:
        decoded = base64.urlsafe_b64decode(payload.encode("utf-8"))
        return json.loads(decoded.decode("utf-8"))
    except (ValueError, json.JSONDecodeError) as exc:
        raise SuspiciousOperation("Unable to decode the WorkOS access token.") from exc


def _normalized_claim_values(value):
    if not value:
        return []
    if isinstance(value, str):
        return [value.strip().lower()] if value.strip() else []
    if isinstance(value, (list, tuple, set)):
        values = []
        for item in value:
            values.extend(_normalized_claim_values(item))
        return values
    return []


def _normalized_role_values(value):
    if isinstance(value, (list, tuple, set)):
        values = []
        for item in value:
            values.extend(_normalized_role_values(item))
        return values

    if isinstance(value, dict):
        values = []
        for key in ("slug", "role", "name"):
            values.extend(_normalized_role_values(value.get(key)))
        return values

    for attribute_name in ("slug", "role", "name"):
        if hasattr(value, attribute_name):
            return _normalized_role_values(getattr(value, attribute_name))

    return _normalized_claim_values(value)


def _workos_role_slugs(access_token_payload):
    role_slugs = set()
    for claim_name in ("role", "roles"):
        role_slugs.update(_normalized_role_values(access_token_payload.get(claim_name)))

    organization_membership = access_token_payload.get("organization_membership")
    if isinstance(organization_membership, dict):
        for claim_name in ("role", "roles"):
            role_slugs.update(
                _normalized_role_values(organization_membership.get(claim_name))
            )

    return tuple(sorted(role_slugs))


def _membership_role_slugs(membership):
    status = str(_get_attr(membership, "status", "active") or "").strip().lower()
    if status and status != "active":
        return ()

    role_slugs = set()
    for attribute_name in ("role", "roles"):
        role_slugs.update(
            _normalized_role_values(_get_attr(membership, attribute_name))
        )
    return tuple(sorted(role_slugs))


def _require_workos_settings():
    missing = [
        key
        for key, value in (
            ("WORKOS_API_KEY", settings.WORKOS_API_KEY),
            ("WORKOS_CLIENT_ID", settings.WORKOS_CLIENT_ID),
            ("WORKOS_ORGANIZATION_ID", settings.WORKOS_ORGANIZATION_ID),
        )
        if not value
    ]
    if missing:
        raise ImproperlyConfigured(
            "Missing required WorkOS settings: " + ", ".join(missing)
        )


class WorkOSAuthClient:
    def __init__(self):
        _require_workos_settings()
        workos_client = _dynamic_import_workos()
        self.client = workos_client(
            api_key=settings.WORKOS_API_KEY,
            client_id=settings.WORKOS_CLIENT_ID,
            base_url=settings.WORKOS_BASE_URL,
        )

    def get_authorization_url(self, state, redirect_uri):
        return self.client.user_management.get_authorization_url(
            provider="authkit",
            organization_id=settings.WORKOS_ORGANIZATION_ID,
            redirect_uri=redirect_uri,
            state=state,
        )

    def authenticate_with_code(self, code, ip_address="", user_agent=""):
        response = self.client.user_management.authenticate_with_code(
            code=code,
            ip_address=ip_address,
            user_agent=user_agent,
        )
        user = _get_attr(response, "user")
        organization_id = _get_attr(response, "organization_id")
        access_token = _get_attr(response, "access_token")

        if not user or not access_token:
            raise SuspiciousOperation(
                "WorkOS did not return a valid authentication payload."
            )

        access_token_payload = _decode_workos_access_token(access_token)
        session_id = access_token_payload.get("sid", "")
        if not session_id:
            raise SuspiciousOperation(
                "WorkOS access token did not contain a session ID."
            )

        email = (_get_attr(user, "email") or "").strip().lower()
        if not email:
            raise SuspiciousOperation("WorkOS did not return an email address.")

        token_role_slugs = set(_workos_role_slugs(access_token_payload))
        membership_role_slugs = set(
            self._organization_membership_role_slugs(
                user_id=str(_get_attr(user, "id")),
                organization_id=str(organization_id or ""),
                organization_membership_id=str(
                    access_token_payload.get("organization_membership_id") or ""
                ),
            )
        )
        role_slugs = tuple(sorted(membership_role_slugs or token_role_slugs))

        return WorkOSAuthenticationResult(
            user_id=str(_get_attr(user, "id")),
            email=email,
            first_name=str(_get_attr(user, "first_name", "") or ""),
            last_name=str(_get_attr(user, "last_name", "") or ""),
            profile_picture_url=str(
                _get_attr(user, "profile_picture_url", "") or ""
            ).strip(),
            organization_id=str(organization_id or ""),
            session_id=str(session_id),
            role_slugs=role_slugs,
        )

    def _organization_membership_role_slugs(
        self, user_id, organization_id, organization_membership_id=""
    ):
        user_management = getattr(self.client, "user_management", None)
        if user_management is None:
            return ()

        try:
            if organization_membership_id and hasattr(
                user_management, "get_organization_membership"
            ):
                return _membership_role_slugs(
                    user_management.get_organization_membership(
                        organization_membership_id
                    )
                )

            if not hasattr(user_management, "list_organization_memberships"):
                return ()

            response = user_management.list_organization_memberships(
                user_id=user_id,
                organization_id=organization_id,
                statuses=["active"],
                limit=100,
                order="asc",
            )
            role_slugs = set()
            for membership in _list_response_items(response):
                role_slugs.update(_membership_role_slugs(membership))
            return tuple(sorted(role_slugs))
        except Exception:
            logger.warning(
                "Unable to refresh WorkOS organization membership roles for user %s.",
                user_id,
                exc_info=True,
            )
            return ()

    def get_logout_url(self, session_id, return_to):
        return self.client.user_management.get_logout_url(
            session_id=session_id,
            return_to=return_to,
        )

    def get_user(self, user_id):
        return self.client.user_management.get_user(user_id)

    def update_user_profile(self, user_id, first_name, last_name):
        return self.client.user_management.update_user(
            user_id=user_id,
            first_name=first_name,
            last_name=last_name,
        )

    def list_sessions(self, user_id):
        return self.client.user_management.list_sessions(user_id=user_id, limit=100)

    def revoke_session(self, session_id):
        return self.client.user_management.revoke_session(session_id=session_id)

    def send_invitation(
        self,
        email,
        inviter_user_id="",
        expires_in_days=None,
        role_slug="",
    ):
        return self.client.user_management.send_invitation(
            email=email,
            organization_id=settings.WORKOS_ORGANIZATION_ID,
            expires_in_days=expires_in_days,
            inviter_user_id=inviter_user_id or None,
            role_slug=role_slug or None,
        )

    def _paginate_directory_list(self, list_method, **params):
        items = []
        after = ""
        while True:
            request_params = {**params, "limit": 100, "order": "asc"}
            if after:
                request_params["after"] = after

            response = list_method(**request_params)
            page_items = _list_response_items(response)
            items.extend(page_items)

            next_after = _list_response_next_after(response, page_items)
            if not next_after or next_after == after:
                break
            after = next_after
        return items

    def list_directory_users(self, directory_id):
        return self._paginate_directory_list(
            self.client.directory_sync.list_users,
            directory_id=directory_id,
        )

    def list_directory_groups(self, directory_id):
        return self._paginate_directory_list(
            self.client.directory_sync.list_groups,
            directory_id=directory_id,
        )

    def list_directory_groups_for_user(self, directory_user_id):
        return self._paginate_directory_list(
            self.client.directory_sync.list_groups,
            user_id=directory_user_id,
        )
