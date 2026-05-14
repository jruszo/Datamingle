from dataclasses import dataclass

from rest_framework import authentication
from rest_framework.exceptions import AuthenticationFailed

from api_agents.services import AgentAPIKeyRejected, authenticate_agent_api_key


@dataclass
class AuthenticatedAgent:
    agent: object
    is_authenticated: bool = True
    is_anonymous: bool = False

    @property
    def id(self):
        return self.agent.id

    @property
    def pk(self):
        return self.agent.pk

    def __str__(self):
        return str(self.agent)


class AgentAPIKeyAuthentication(authentication.BaseAuthentication):
    keyword = "Bearer"

    def authenticate(self, request):
        try:
            header = authentication.get_authorization_header(request).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise AuthenticationFailed("Invalid agent authorization header.") from exc
        if not header:
            return None

        parts = header.split()
        if not parts or parts[0].lower() != self.keyword.lower():
            return None
        if len(parts) != 2:
            raise AuthenticationFailed("Invalid agent authorization header.")

        try:
            agent = authenticate_agent_api_key(parts[1])
        except AgentAPIKeyRejected as exc:
            raise AuthenticationFailed(str(exc)) from exc
        if agent is None:
            raise AuthenticationFailed("Invalid agent API key.")
        return AuthenticatedAgent(agent), agent
