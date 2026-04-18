# -*- coding: UTF-8 -*-


# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
import os
from datetime import timedelta
import environ
import logging
from django.core.exceptions import ImproperlyConfigured

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

environ.Env.read_env(os.path.join(BASE_DIR, ".env"))
env = environ.Env(
    DEBUG=(bool, False),
    ALLOWED_HOSTS=(list, ["*"]),
    SECRET_KEY=(
        str,
        "",
    ),  # Reference: https://docs.djangoproject.com/en/4.0/ref/settings/#secret-key
    DATABASE_URL=(str, "mysql://root:@127.0.0.1:3306/archery"),
    CACHE_URL=(str, "redis://127.0.0.1:6379/0"),
    AUTH_MODE=(str, "builtin"),
    WORKOS_API_KEY=(str, ""),
    WORKOS_CLIENT_ID=(str, ""),
    WORKOS_ORGANIZATION_ID=(str, ""),
    WORKOS_REDIRECT_URI=(str, ""),
    WORKOS_LOGOUT_REDIRECT_URI=(str, ""),
    WORKOS_STAFF_EMAILS=(list, []),
    WORKOS_SUPERUSER_EMAILS=(list, []),
    Q_CLUISTER_SYNC=(
        bool,
        False,
    ),  # qcluster sync mode; set to True for local debugging if needed
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
    TASK_BACKEND=(str, "django_q"),
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

AUTH_MODE = env("AUTH_MODE", default="builtin").strip().lower()
if AUTH_MODE not in {"builtin", "workos"}:
    raise ImproperlyConfigured("AUTH_MODE must be either 'builtin' or 'workos'.")
ENABLE_WORKOS_AUTH = AUTH_MODE == "workos"

# https://docs.djangoproject.com/en/4.0/ref/settings/#csrf-trusted-origins
CSRF_TRUSTED_ORIGINS = env("CSRF_TRUSTED_ORIGINS")

# Fix 404 redirects behind nginx deployment
USE_X_FORWARDED_HOST = True

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
DEFAULT_TASK_BACKEND = env("TASK_BACKEND", default="django_q").strip().lower()
if DEFAULT_TASK_BACKEND not in {"django_q", "celery"}:
    raise ImproperlyConfigured("TASK_BACKEND must be either 'django_q' or 'celery'.")

# Application definition
INSTALLED_APPS = (
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "django_q",
    "sql",
    "sql_api",
    "common",
    "rest_framework",
    "django_filters",
    "drf_spectacular",
)

MIDDLEWARE = (
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
    "django.middleware.security.SecurityMiddleware",
    "django.middleware.gzip.GZipMiddleware",
    "common.middleware.check_login_middleware.CheckLoginMiddleware",
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
AUTHENTICATION_BACKENDS = ("django.contrib.auth.backends.ModelBackend",)
LOGIN_REDIRECT_URL = "/"

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
                "NAME": "test_archery",
                "CHARSET": "utf8mb4",
            },
        },
    }
}

# Django-Q
Q_CLUSTER = {
    "name": "archery",
    "workers": env("Q_CLUISTER_WORKERS", default=4),
    "recycle": 500,
    "timeout": env("Q_CLUISTER_TIMEOUT", default=60),
    "compress": True,
    "cpu_affinity": 1,
    "save_limit": 0,
    "queue_limit": 50,
    "label": "Django Q",
    "django_redis": "default",
    "sync": env("Q_CLUISTER_SYNC"),  # Set to True for local synchronous debugging
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

if DEFAULT_TASK_BACKEND == "celery":
    if not CELERY_BROKER_URL:
        raise ImproperlyConfigured(
            "CELERY_BROKER_URL is required when TASK_BACKEND is 'celery'."
        )
    if not CELERY_RESULT_BACKEND:
        raise ImproperlyConfigured(
            "CELERY_RESULT_BACKEND is required when TASK_BACKEND is 'celery'."
        )

# Cache settings
CACHES = {
    "default": env.cache(),
}

# https://docs.djangoproject.com/en/3.2/ref/settings/#std-setting-DEFAULT_AUTO_FIELD
DEFAULT_AUTO_FIELD = "django.db.models.AutoField"

# API Framework
REST_FRAMEWORK = {
    "DEFAULT_SCHEMA_CLASS": "drf_spectacular.openapi.AutoSchema",
    "DEFAULT_RENDERER_CLASSES": ("rest_framework.renderers.JSONRenderer",),
    # Authentication
    "DEFAULT_AUTHENTICATION_CLASSES": (
        "rest_framework_simplejwt.authentication.JWTAuthentication",
        "rest_framework.authentication.SessionAuthentication",
    ),
    # Permissions
    "DEFAULT_PERMISSION_CLASSES": ("rest_framework.permissions.IsAuthenticated",),
    # Throttling (anon: unauthenticated users, user: authenticated users)
    "DEFAULT_THROTTLE_CLASSES": (
        "rest_framework.throttling.AnonRateThrottle",
        "rest_framework.throttling.UserRateThrottle",
    ),
    "DEFAULT_THROTTLE_RATES": {"anon": "120/min", "user": "600/min"},
    # Filtering
    "DEFAULT_FILTER_BACKENDS": ("django_filters.rest_framework.DjangoFilterBackend",),
    # Pagination
    "DEFAULT_PAGINATION_CLASS": "rest_framework.pagination.PageNumberPagination",
    "PAGE_SIZE": 5,
}

# Swagger UI
SPECTACULAR_SETTINGS = {
    "TITLE": "Archery API",
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
WORKOS_REDIRECT_URI = env("WORKOS_REDIRECT_URI", default="")
WORKOS_LOGOUT_REDIRECT_URI = env("WORKOS_LOGOUT_REDIRECT_URI", default="")
WORKOS_STAFF_EMAILS = [
    email.strip().lower()
    for email in env("WORKOS_STAFF_EMAILS", default=[])
    if email.strip()
]
WORKOS_SUPERUSER_EMAILS = [
    email.strip().lower()
    for email in env("WORKOS_SUPERUSER_EMAILS", default=[])
    if email.strip()
]

if ENABLE_WORKOS_AUTH:
    missing_workos = [
        key
        for key, value in (
            ("WORKOS_API_KEY", WORKOS_API_KEY),
            ("WORKOS_CLIENT_ID", WORKOS_CLIENT_ID),
            ("WORKOS_ORGANIZATION_ID", WORKOS_ORGANIZATION_ID),
            ("WORKOS_REDIRECT_URI", WORKOS_REDIRECT_URI),
            ("WORKOS_LOGOUT_REDIRECT_URI", WORKOS_LOGOUT_REDIRECT_URI),
        )
        if not value
    ]
    if missing_workos:
        raise ImproperlyConfigured(
            "Missing required WorkOS settings: " + ", ".join(missing_workos)
        )

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
            "filename": "logs/archery.log",
            "maxBytes": 1024 * 1024 * 100,  # 5 MB
            "backupCount": 5,
            "formatter": "verbose",
        },
        "django-q": {
            "level": "DEBUG",
            "class": "logging.handlers.RotatingFileHandler",
            "filename": "logs/qcluster.log",
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
        "django-q": {  # logs for django_q module
            "handlers": ["console", "django-q"],
            "level": "WARNING",
            "propagate": False,
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

# Append this content to website title and login page to distinguish multiple Archery instances.
# The same option exists in Archery admin; if both are set, admin configuration takes precedence.
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
