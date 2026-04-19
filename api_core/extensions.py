from importlib import import_module

from django.apps import apps
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured


def get_extension_urlpatterns():
    extension_apps = getattr(settings, "DATAMINGLE_API_EXTENSION_APPS", [])
    urlpatterns = []

    for app_path in extension_apps:
        if not apps.is_installed(app_path):
            raise ImproperlyConfigured(
                f"{app_path} must be present in INSTALLED_APPS before it can be used "
                "in DATAMINGLE_API_EXTENSION_APPS."
            )
        module_name = f"{app_path}.api_urls"
        try:
            module = import_module(module_name)
        except ModuleNotFoundError as exc:
            if exc.name != module_name:
                raise
            raise ImproperlyConfigured(
                f"{module_name} could not be imported. Ensure {app_path} defines "
                "an api_urls module."
            ) from exc
        module_urlpatterns = getattr(module, "urlpatterns", None)
        if module_urlpatterns is None:
            raise ImproperlyConfigured(f"{app_path}.api_urls must define urlpatterns.")
        urlpatterns.extend(module_urlpatterns)

    return urlpatterns
