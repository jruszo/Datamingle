import base64
import json
from dataclasses import dataclass

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured, SuspiciousOperation


@dataclass
class WorkOSAuthenticationResult:
    user_id: str
    email: str
    first_name: str
    last_name: str
    profile_picture_url: str
    organization_id: str
    session_id: str

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
        )

    def get_logout_url(self, session_id, return_to):
        return self.client.user_management.get_logout_url(
            session_id=session_id,
            return_to=return_to,
        )
