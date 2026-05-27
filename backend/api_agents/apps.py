from django.apps import AppConfig


class ApiAgentsConfig(AppConfig):
    name = "api_agents"
    verbose_name = "Datamingle Agents"

    def ready(self):
        import api_agents.signals  # noqa: F401
