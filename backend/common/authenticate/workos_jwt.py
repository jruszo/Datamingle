import logging
from functools import lru_cache

import jwt
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from rest_framework import authentication
from rest_framework import exceptions

from sql.models import Users

logger = logging.getLogger(__name__)


def _setting(name, default=""):
    return str(getattr(settings, name, default) or "").strip()


def _normalized_url(value):
    return str(value or "").strip().rstrip("/")


def workos_jwks_url():
    configured_url = _setting("WORKOS_JWKS_URL")
    if configured_url:
        return configured_url

    client_id = _setting("WORKOS_CLIENT_ID")
    if not client_id:
        raise ImproperlyConfigured("WORKOS_CLIENT_ID is required for WorkOS JWT auth.")

    base_url = _normalized_url(_setting("WORKOS_BASE_URL", "https://api.workos.com"))
    return f"{base_url}/sso/jwks/{client_id}"


@lru_cache(maxsize=8)
def _jwk_client(jwks_url):
    return jwt.PyJWKClient(jwks_url)


class WorkOSJWTVerifier:
    def __init__(self, jwks_url=None):
        self.jwks_url = jwks_url or workos_jwks_url()

    def verify(self, token):
        try:
            signing_key = _jwk_client(self.jwks_url).get_signing_key_from_jwt(token).key
            payload = jwt.decode(
                token,
                signing_key,
                algorithms=["RS256"],
                options={"verify_aud": False},
            )
        except jwt.ExpiredSignatureError as exc:
            raise exceptions.AuthenticationFailed(
                "WorkOS access token expired."
            ) from exc
        except jwt.InvalidTokenError as exc:
            raise exceptions.AuthenticationFailed(
                "Invalid WorkOS access token."
            ) from exc
        except Exception as exc:
            logger.warning("WorkOS JWT verification failed.", exc_info=True)
            raise exceptions.AuthenticationFailed(
                "Unable to verify WorkOS access token."
            ) from exc

        self._validate_claims(payload)
        return payload

    def _validate_claims(self, payload):
        issuer = _normalized_url(payload.get("iss"))
        expected_issuer = _normalized_url(
            _setting("WORKOS_JWT_ISSUER") or _setting("WORKOS_BASE_URL")
        )
        if expected_issuer and issuer != expected_issuer:
            raise exceptions.AuthenticationFailed("WorkOS token issuer is not trusted.")

        client_id = str(payload.get("client_id") or "").strip()
        expected_client_id = _setting("WORKOS_CLIENT_ID")
        if expected_client_id and client_id != expected_client_id:
            raise exceptions.AuthenticationFailed("WorkOS token client_id is invalid.")

        org_id = str(payload.get("org_id") or "").strip()
        organization_id = str(payload.get("organization_id") or "").strip()
        if not org_id:
            raise exceptions.AuthenticationFailed("WorkOS token is missing org_id.")
        if organization_id and organization_id != org_id:
            raise exceptions.AuthenticationFailed("WorkOS token organization mismatch.")

        expected_org_id = _setting("WORKOS_ORGANIZATION_ID")
        if expected_org_id and org_id != expected_org_id:
            raise exceptions.AuthenticationFailed("WorkOS token org_id is invalid.")

        user_id = str(payload.get("sub") or payload.get("user_id") or "").strip()
        if not user_id:
            raise exceptions.AuthenticationFailed("WorkOS token is missing user id.")


class WorkOSJWTAuthentication(authentication.BaseAuthentication):
    keyword = "Bearer"
    verifier_class = WorkOSJWTVerifier

    def authenticate(self, request):
        auth = authentication.get_authorization_header(request).split()
        if not auth:
            return None

        if len(auth) != 2 or auth[0].lower() != self.keyword.lower().encode():
            raise exceptions.AuthenticationFailed(
                "Invalid Authorization header. Expected Bearer token."
            )

        try:
            token = auth[1].decode("utf-8")
        except UnicodeError as exc:
            raise exceptions.AuthenticationFailed(
                "Invalid Authorization header encoding."
            ) from exc

        payload = self.verifier_class().verify(token)
        user_id = str(payload.get("sub") or payload.get("user_id") or "").strip()
        try:
            user = Users.objects.get(workos_user_id=user_id)
        except Users.DoesNotExist as exc:
            raise exceptions.AuthenticationFailed(
                "No Datamingle user is linked to this WorkOS identity."
            ) from exc

        if not user.is_active:
            raise exceptions.AuthenticationFailed(
                "This Datamingle account is inactive."
            )

        user.organization_id = str(payload.get("org_id") or "")
        user.workos_claims = payload
        return user, payload
