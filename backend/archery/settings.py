# -*- coding: UTF-8 -*-


# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
import os
from datetime import timedelta
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
import environ
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

environ.Env.read_env(os.path.join(BASE_DIR, ".env"))
env = environ.Env(
    DEBUG=(bool, False),
    ALLOWED_HOSTS=(list, ["*"]),
    TRUST_X_FORWARDED_PROTO=(bool, False),
    SECRET_KEY=(
        str,
        "",
    ),  # Reference: https://docs.djangoproject.com/en/4.0/ref/settings/#secret-key
    DATABASE_URL=(str, "mysql://root:@127.0.0.1:3306/datamingle"),
    CACHE_URL=(str, "redis://127.0.0.1:6379/0"),
    WORKOS_API_KEY=(str, ""),
    WORKOS_CLIENT_ID=(str, ""),
    WORKOS_ORGANIZATION_ID=(str, ""),
    WORKOS_BASE_URL=(str, "https://api.workos.com/"),
    WORKOS_JWKS_URL=(str, ""),
    WORKOS_JWT_ISSUER=(str, ""),
    DATAMINGLE_SINGLE_TENANT_ORGANIZATION_ID=(str, "datamingle"),
    DATAMINGLE_METRICS_BACKEND_URL=(str, "http://victoriametrics-local-dev:8428"),
    DATAMINGLE_METRICS_TENANT_URLS=(str, ""),
    DATAMINGLE_METRICS_PROXY_TIMEOUT_SECONDS=(int, 20),
    DATAMINGLE_METRICS_MAX_QUERY_LENGTH=(int, 8192),
    DATAMINGLE_METRICS_MAX_RANGE_SECONDS=(int, 2592000),
    DATAMINGLE_METRICS_MIN_STEP_SECONDS=(int, 15),
    DATAMINGLE_METRICS_MAX_RANGE_POINTS=(int, 11000),
    DATAMINGLE_METRICS_MAX_MATCHERS=(int, 32),
    CHANNEL_LAYER_URL=(str, ""),
    # CSRF_TRUSTED_ORIGINS=subdomain.example.com,subdomain.example2.com subdomain.example.com
    CSRF_TRUSTED_ORIGINS=(list, []),
    ENABLED_ENGINES=(
        list,
        [
            "mysql",
            "clickhouse",
            "goinception",
            "mssql",
            "redis",
            "pgsql",
            "oracle",
            "mongo",
            "phoenix",
            "odps",
            "cassandra",
            "doris",
            "elasticsearch",
            "opensearch",
            "memcached",
        ],
    ),
    ENABLED_NOTIFIERS=(
        list,
        [
            "sql.notify:FeishuWebhookNotifier",
            "sql.notify:FeishuPersonNotifier",
            "sql.notify:QywxWebhookNotifier",
            "sql.notify:QywxToUserNotifier",
            "sql.notify:MailNotifier",
            "sql.notify:GenericWebhookNotifier",
        ],
    ),
    CURRENT_AUDITOR=(str, "sql.utils.workflow_audit:AuditV2"),
    PASSWORD_MIXIN_PATH=(str, "sql.plugins.password:DummyMixin"),
    FIELD_ENCRYPTION_KEYS=(str, ""),
    CELERY_BROKER_URL=(str, ""),
    CELERY_RESULT_BACKEND=(str, ""),
    CELERY_TASK_DEFAULT_QUEUE=(str, "default"),
    CELERY_TASK_SOFT_TIME_LIMIT=(int, 0),
    CELERY_TASK_TIME_LIMIT=(int, 0),
)

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = env("SECRET_KEY")

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = env("DEBUG")

ALLOWED_HOSTS = env("ALLOWED_HOSTS")

AUTH_MODE = "allauth"

# https://docs.djangoproject.com/en/4.0/ref/settings/#csrf-trusted-origins
CSRF_TRUSTED_ORIGINS = env("CSRF_TRUSTED_ORIGINS")

# Fix 404 redirects behind nginx deployment
USE_X_FORWARDED_HOST = True
TRUST_X_FORWARDED_PROTO = env("TRUST_X_FORWARDED_PROTO")
if TRUST_X_FORWARDED_PROTO:
    SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")

# Request limits
DATA_UPLOAD_MAX_MEMORY_SIZE = 15728640

AVAILABLE_ENGINES = {
    "mysql": {"path": "sql.engines.mysql:MysqlEngine"},
    "cassandra": {"path": "sql.engines.cassandra:CassandraEngine"},
    "clickhouse": {"path": "sql.engines.clickhouse:ClickHouseEngine"},
    "goinception": {"path": "sql.engines.goinception:GoInceptionEngine"},
    "mssql": {"path": "sql.engines.mssql:MssqlEngine"},
    "redis": {"path": "sql.engines.redis:RedisEngine"},
    "pgsql": {"path": "sql.engines.pgsql:PgSQLEngine"},
    "oracle": {"path": "sql.engines.oracle:OracleEngine"},
    "mongo": {"path": "sql.engines.mongo:MongoEngine"},
    "phoenix": {"path": "sql.engines.phoenix:PhoenixEngine"},
    "odps": {"path": "sql.engines.odps:ODPSEngine"},
    "doris": {"path": "sql.engines.doris:DorisEngine"},
    "elasticsearch": {"path": "sql.engines.elasticsearch:ElasticsearchEngine"},
    "opensearch": {"path": "sql.engines.elasticsearch:OpenSearchEngine"},
    "memcached": {"path": "sql.engines.memcached:MemcachedEngine"},
}

ENABLED_NOTIFIERS = env("ENABLED_NOTIFIERS")

ENABLED_ENGINES = env("ENABLED_ENGINES")

CURRENT_AUDITOR = env("CURRENT_AUDITOR")

PASSWORD_MIXIN_PATH = env("PASSWORD_MIXIN_PATH")

FIELD_ENCRYPTION_KEYS = env("FIELD_ENCRYPTION_KEYS")
# Application definition
INSTALLED_APPS = (
    "daphne",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.sites",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "channels",
    "sql",
    "sql_api",
    "api_core",
    "api_auth",
    "api_users",
    "api_instances",
    "api_workflows",
    "api_archives",
    "api_queries",
    "api_access",
    "api_mailbox",
    "api_agents",
    "api_infrastructure",
    "api_metrics",
    "api_admin",
    "common",
    "rest_framework",
    "django_filters",
    "drf_spectacular",
    "allauth",
    "allauth.account",
    "allauth.headless",
    "allauth.usersessions",
)

DATAMINGLE_API_EXTENSION_APPS = []

MIDDLEWARE = (
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "allauth.account.middleware.AccountMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
    "django.middleware.security.SecurityMiddleware",
    "django.middleware.gzip.GZipMiddleware",
    "common.middleware.exception_logging_middleware.ExceptionLoggingMiddleware",
)

ROOT_URLCONF = "archery.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [os.path.join(BASE_DIR, "common/templates")],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
                "common.utils.global_info.global_info",
            ],
        },
    },
]

WSGI_APPLICATION = "archery.wsgi.application"
ASGI_APPLICATION = "archery.asgi.application"

# Internationalization
LANGUAGE_CODE = "en-us"

TIME_ZONE = "Asia/Shanghai"

USE_I18N = True

USE_TZ = False

# Time formatting
DATETIME_FORMAT = "Y-m-d H:i:s"
DATE_FORMAT = "Y-m-d"

# Static files (CSS, JavaScript, Images)
STATIC_URL = "/static/"
STATIC_ROOT = os.path.join(BASE_DIR, "static")
STATICFILES_DIRS = [
    os.path.join(BASE_DIR, "common/static"),
]
STORAGES = {
    "default": {"BACKEND": "django.core.files.storage.FileSystemStorage"},
    "staticfiles": {"BACKEND": "common.storage.ForgivingManifestStaticFilesStorage"},
}

# Used to extend users field in Django admin, pointing to sql/models.py Users class
AUTH_USER_MODEL = "sql.Users"
AUTHENTICATION_BACKENDS = (
    "common.auth_backends.TeamPermissionBackend",
    "allauth.account.auth_backends.AuthenticationBackend",
)
LOGIN_REDIRECT_URL = "/"
SITE_ID = 1

ACCOUNT_ADAPTER = "api_auth.adapters.DatamingleAccountAdapter"
ACCOUNT_LOGIN_METHODS = {"email"}
ACCOUNT_SIGNUP_FIELDS = ["email*", "password1*", "password2*"]
ACCOUNT_EMAIL_VERIFICATION = "optional"
ACCOUNT_UNIQUE_EMAIL = True
ACCOUNT_USER_MODEL_EMAIL_FIELD = "email"
ACCOUNT_USER_MODEL_USERNAME_FIELD = "username"
HEADLESS_ONLY = True
HEADLESS_CLIENTS = ("app",)
HEADLESS_TOKEN_STRATEGY = "allauth.headless.tokens.strategies.jwt.JWTTokenStrategy"
HEADLESS_JWT_ALGORITHM = "HS256"
HEADLESS_JWT_ACCESS_TOKEN_EXPIRES_IN = 60 * 60 * 4
HEADLESS_JWT_REFRESH_TOKEN_EXPIRES_IN = 60 * 60 * 24 * 3
HEADLESS_JWT_ROTATE_REFRESH_TOKEN = True
HEADLESS_JWT_STATEFUL_VALIDATION_ENABLED = True
HEADLESS_JWT_AUTHORIZATION_HEADER_SCHEME = "Bearer"

# Password validation
AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
        "OPTIONS": {
            "min_length": 9,
        },
    },
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
]

############### The following section should be adjusted based on your environment ###################

# SESSION settings
SESSION_COOKIE_AGE = 60 * 300  # 300 minutes
SESSION_SAVE_EVERY_REQUEST = True
SESSION_EXPIRE_AT_BROWSER_CLOSE = True  # Cookie expires when browser is closed

# MySQL database address for this project
DATABASES = {
    "default": {
        **env.db(),
        **{
            "DEFAULT_CHARSET": "utf8mb4",
            "CONN_MAX_AGE": 50,
            "OPTIONS": {
                "init_command": "SET sql_mode='STRICT_TRANS_TABLES'",
                "charset": "utf8mb4",
            },
            "TEST": {
                "NAME": "test_datamingle",
                "CHARSET": "utf8mb4",
            },
        },
    }
}

# Celery
CELERY_BROKER_URL = env("CELERY_BROKER_URL", default="")
CELERY_RESULT_BACKEND = env("CELERY_RESULT_BACKEND", default="")
CELERY_TASK_DEFAULT_QUEUE = env("CELERY_TASK_DEFAULT_QUEUE", default="default")
CELERY_TASK_SOFT_TIME_LIMIT = env("CELERY_TASK_SOFT_TIME_LIMIT", default=0) or None
CELERY_TASK_TIME_LIMIT = env("CELERY_TASK_TIME_LIMIT", default=0) or None
CELERY_ACCEPT_CONTENT = ["json"]
CELERY_TASK_SERIALIZER = "json"
CELERY_RESULT_SERIALIZER = "json"

# Cache settings
CACHES = {
    "default": env.cache(),
}


def _redis_url_for_channels(raw_url):
    parsed = urlparse(raw_url)
    query = dict(parse_qsl(parsed.query))
    password = query.pop("PASSWORD", "") or query.pop("password", "")
    if password and not parsed.password:
        if not parsed.hostname:
            raise ValueError(f"Malformed Redis URL for channels: {raw_url!r}")
        host = parsed.hostname
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        netloc = host
        if parsed.port:
            netloc = f"{netloc}:{parsed.port}"
        if parsed.username:
            netloc = f"{parsed.username}:{password}@{netloc}"
        else:
            netloc = f":{password}@{netloc}"
        parsed = parsed._replace(netloc=netloc)
    parsed = parsed._replace(query=urlencode(query))
    return urlunparse(parsed)


CHANNEL_LAYER_URL = env("CHANNEL_LAYER_URL", default="") or env("CACHE_URL")
CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels_redis.core.RedisChannelLayer",
        "CONFIG": {"hosts": [_redis_url_for_channels(CHANNEL_LAYER_URL)]},
    }
}

# https://docs.djangoproject.com/en/3.2/ref/settings/#std-setting-DEFAULT_AUTO_FIELD
DEFAULT_AUTO_FIELD = "django.db.models.AutoField"

# API Framework
REST_FRAMEWORK = {
    "DEFAULT_SCHEMA_CLASS": "drf_spectacular.openapi.AutoSchema",
    "DEFAULT_RENDERER_CLASSES": ("rest_framework.renderers.JSONRenderer",),
    # Authentication
    "DEFAULT_AUTHENTICATION_CLASSES": (
        "allauth.headless.contrib.rest_framework.authentication.JWTTokenAuthentication",
        "rest_framework.authentication.SessionAuthentication",
    ),
    # Permissions
    "DEFAULT_PERMISSION_CLASSES": ("rest_framework.permissions.IsAuthenticated",),
    # Throttling (anon: unauthenticated users, user: authenticated users)
    "DEFAULT_THROTTLE_CLASSES": (
        "rest_framework.throttling.AnonRateThrottle",
        "rest_framework.throttling.UserRateThrottle",
    ),
    "DEFAULT_THROTTLE_RATES": {
        "anon": "120/min",
        "user": "600/min",
        "metrics_metadata": "300/min",
        "metrics_query": "120/min",
        "metrics_query_range": "60/min",
        "metrics_ai": "20/hour",
    },
    # Filtering
    "DEFAULT_FILTER_BACKENDS": ("django_filters.rest_framework.DjangoFilterBackend",),
    # Pagination
    "DEFAULT_PAGINATION_CLASS": "rest_framework.pagination.PageNumberPagination",
    "PAGE_SIZE": 5,
}

# Swagger UI
SPECTACULAR_SETTINGS = {
    "TITLE": "Datamingle API",
    "DESCRIPTION": "OpenAPI 3.0",
    "VERSION": "1.0.0",
}

# API Authentication
SIMPLE_JWT = {
    "ACCESS_TOKEN_LIFETIME": timedelta(hours=4),
    "REFRESH_TOKEN_LIFETIME": timedelta(days=3),
    "ALGORITHM": "HS256",
    "SIGNING_KEY": SECRET_KEY,
    "AUTH_HEADER_TYPES": ("Bearer",),
}

WORKOS_API_KEY = env("WORKOS_API_KEY", default="")
WORKOS_CLIENT_ID = env("WORKOS_CLIENT_ID", default="")
WORKOS_ORGANIZATION_ID = env("WORKOS_ORGANIZATION_ID", default="")
WORKOS_BASE_URL = env("WORKOS_BASE_URL", default="https://api.workos.com/")
WORKOS_JWKS_URL = env("WORKOS_JWKS_URL", default="")
WORKOS_JWT_ISSUER = env("WORKOS_JWT_ISSUER", default="") or WORKOS_BASE_URL.rstrip("/")
DATAMINGLE_SINGLE_TENANT_ORGANIZATION_ID = env(
    "DATAMINGLE_SINGLE_TENANT_ORGANIZATION_ID", default="datamingle"
)
DATAMINGLE_METRICS_BACKEND_URL = env(
    "DATAMINGLE_METRICS_BACKEND_URL",
    default="http://victoriametrics-local-dev:8428",
).rstrip("/")
DATAMINGLE_METRICS_TENANT_URLS = env("DATAMINGLE_METRICS_TENANT_URLS", default="")
DATAMINGLE_METRICS_PROXY_TIMEOUT_SECONDS = env(
    "DATAMINGLE_METRICS_PROXY_TIMEOUT_SECONDS", default=20
)
DATAMINGLE_METRICS_MAX_QUERY_LENGTH = env(
    "DATAMINGLE_METRICS_MAX_QUERY_LENGTH", default=8192
)
DATAMINGLE_METRICS_MAX_RANGE_SECONDS = env(
    "DATAMINGLE_METRICS_MAX_RANGE_SECONDS", default=2592000
)
DATAMINGLE_METRICS_MIN_STEP_SECONDS = env(
    "DATAMINGLE_METRICS_MIN_STEP_SECONDS", default=15
)
DATAMINGLE_METRICS_MAX_RANGE_POINTS = env(
    "DATAMINGLE_METRICS_MAX_RANGE_POINTS", default=11000
)
DATAMINGLE_METRICS_MAX_MATCHERS = env("DATAMINGLE_METRICS_MAX_MATCHERS", default=32)
# Logging configuration
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "verbose": {
            "format": "[%(asctime)s][%(threadName)s:%(thread)d][task_id:%(name)s][%(filename)s:%(lineno)d][%(levelname)s]- %(message)s"
        },
    },
    "handlers": {
        "default": {
            "level": "DEBUG",
            "class": "logging.handlers.RotatingFileHandler",
            "filename": "logs/datamingle.log",
            "maxBytes": 1024 * 1024 * 100,  # 5 MB
            "backupCount": 5,
            "formatter": "verbose",
        },
        "console": {
            "level": "DEBUG",
            "class": "logging.StreamHandler",
            "formatter": "verbose",
        },
    },
    "loggers": {
        "default": {  # default logs
            "handlers": ["console", "default"],
            "level": "WARNING",
        },
        # 'django.db': {  # print SQL statements for development
        #     'handlers': ['console', 'default'],
        #     'level': 'DEBUG',
        #     'propagate': False
        # },
        # 'django.request': {  # print request error stack traces for development
        #     'handlers': ['console', 'default'],
        #     'level': 'DEBUG',
        #     'propagate': False
        # },
    },
}

# Append this content to website title and login page to distinguish multiple Datamingle instances.
# The same option exists in Datamingle admin; if both are set, admin configuration takes precedence.
CUSTOM_TITLE_SUFFIX = env("CUSTOM_TITLE_SUFFIX", default="")

MEDIA_ROOT = os.path.join(BASE_DIR, "media")
if not os.path.exists(MEDIA_ROOT):
    os.mkdir(MEDIA_ROOT)

PKEY_ROOT = os.path.join(MEDIA_ROOT, "keys")
if not os.path.exists(PKEY_ROOT):
    os.mkdir(PKEY_ROOT)

try:
    from local_settings import *
except ImportError:
    print("import local settings failed, ignored")
