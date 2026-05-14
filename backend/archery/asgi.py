"""
ASGI config for untitled project.

It exposes the ASGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/3.1/howto/deployment/asgi/
"""

import os

from channels.auth import AuthMiddlewareStack
from channels.routing import ProtocolTypeRouter, URLRouter
from django.core.asgi import get_asgi_application
from django.urls import path

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "archery.settings")

django_asgi_application = get_asgi_application()

from api_agents.consumers import AgentConsumer  # noqa: E402

application = ProtocolTypeRouter(
    {
        "http": django_asgi_application,
        "websocket": AuthMiddlewareStack(
            URLRouter(
                [
                    path("api/ws/agent/", AgentConsumer.as_asgi()),
                ]
            )
        ),
    }
)
