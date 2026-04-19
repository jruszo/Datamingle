from importlib import import_module

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured


def get_extension_urlpatterns():
    extension_apps = getattr(settings, "DATAMINGLE_API_EXTENSION_APPS", [])
    installed_apps = set(settings.INSTALLED_APPS)
    urlpatterns = []

    for app_path in extension_apps:
        if app_path not in installed_apps:
            raise ImproperlyConfigured(
                f"{app_path} must be present in INSTALLED_APPS before it can be used "
                "in DATAMINGLE_API_EXTENSION_APPS."
            )
        module = import_module(f"{app_path}.api_urls")
        module_urlpatterns = getattr(module, "urlpatterns", None)
        if module_urlpatterns is None:
            raise ImproperlyConfigured(f"{app_path}.api_urls must define urlpatterns.")
        urlpatterns.extend(module_urlpatterns)

    return urlpatterns
