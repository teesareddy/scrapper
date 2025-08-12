import os
from pathlib import Path

# Optional imports
try:
    import sentry_sdk
    from sentry_sdk.integrations.django import DjangoIntegration
    from sentry_sdk.integrations.celery import CeleryIntegration
    from sentry_sdk.integrations.redis import RedisIntegration
    SENTRY_AVAILABLE = True
except ImportError:
    SENTRY_AVAILABLE = False

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = os.environ.get('SECRET_KEY', 'django-insecure-your-secret-key-here')

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = int(os.environ.get('DEBUG', 0))

# Parse ALLOWED_HOSTS correctly
if os.environ.get('DJANGO_ALLOWED_HOSTS'):
    ALLOWED_HOSTS = os.environ.get('DJANGO_ALLOWED_HOSTS').split(',')
else:
    ALLOWED_HOSTS = ["localhost", "127.0.0.1", "0.0.0.0"]

# Development setting
if DEBUG:
    ALLOWED_HOSTS = ['*']

# Application definition
INSTALLED_APPS = [
    # Unfold admin theme must be before django.contrib.admin
    'unfold',
    'unfold.contrib.filters',
    'unfold.contrib.forms',
    'unfold.contrib.inlines',
    'unfold.contrib.import_export',
    'unfold.contrib.simple_history',
    
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    
    # Celery monitoring (optional)
    'django_celery_beat',
    'django_celery_results',
    
    # Custom apps
    'consumer',
    'scrapers',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'config.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [BASE_DIR / 'templates'],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'config.wsgi.application'

# Database - Ensure PostgreSQL is used
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': os.environ.get('SQL_DATABASE', 'django_db'),
        'USER': os.environ.get('SQL_USER', 'postgres'),
        'PASSWORD': os.environ.get('SQL_PASSWORD', 'postgres'),
        'HOST': os.environ.get('SQL_HOST', 'django-postgres'),
        'PORT': os.environ.get('SQL_PORT', '5432'),
        'OPTIONS': {
            'connect_timeout': 10,
        },
    }
}

# Verify PostgreSQL is being used
if DATABASES['default']['ENGINE'] != 'django.db.backends.postgresql':
    raise ValueError("This application requires PostgreSQL database backend")

# Password validation
AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]

# Internationalization
LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'UTC'
USE_I18N = True
USE_L10N = True
USE_TZ = True

# Static files (CSS, JavaScript, Images)
STATIC_URL = '/static/'
STATIC_ROOT = BASE_DIR / 'staticfiles'

# Media files
MEDIA_URL = '/media/'
MEDIA_ROOT = BASE_DIR / 'mediafiles'

# Default primary key field type
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

# RabbitMQ Settings
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
RABBITMQ_PORT = int(os.environ.get('RABBITMQ_PORT', 5672))
RABBITMQ_USER = os.environ.get('RABBITMQ_USER', 'admin')
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_PASSWORD', 'admin123')
RABBITMQ_VHOST = os.environ.get('RABBITMQ_VHOST', '/')
RABBITMQ_QUEUE = os.environ.get('RABBITMQ_QUEUE', 'nest_to_django')

# Celery Configuration
CELERY_BROKER_URL = os.environ.get('CELERY_BROKER_URL', 'redis://redis:6379/0')
CELERY_RESULT_BACKEND = os.environ.get('CELERY_RESULT_BACKEND', 'redis://redis:6379/0')
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_TIMEZONE = 'UTC'
CELERY_ENABLE_UTC = True

# Celery Beat Scheduler
CELERY_BEAT_SCHEDULER = 'django_celery_beat.schedulers:DatabaseScheduler'

# Celery Results Backend
CELERY_RESULT_BACKEND = 'django-db'
CELERY_CACHE_BACKEND = 'django-cache'

# Celery monitoring
CELERY_TASK_TRACK_STARTED = True
CELERY_TASK_SEND_SENT_EVENT = True
CELERY_WORKER_SEND_TASK_EVENTS = True
CELERY_RESULT_EXTENDED = True

# Task result expiration
CELERY_RESULT_EXPIRES = 3600  # 1 hour

# Task routing
CELERY_TASK_ROUTES = {
    'scrapers.tasks.*': {'queue': 'scrapers'},
    'consumer.tasks.scrape_performance': {'queue': 'default'},  # Route to default queue where workers listen
    'consumer.tasks.sync_performance_pos': {'queue': 'default'},  # Route to default queue where workers listen
}

# Redis Settings (for direct Redis usage - not Django cache)
REDIS_HOST = os.environ.get('REDIS_HOST', 'redis')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
REDIS_DB = int(os.environ.get('REDIS_DB', 0))
REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD', None)

# StubHub API Configuration
STUBHUB_ACCOUNT_ID = os.environ.get('STUBHUB_ACCOUNT_ID', None)
STUBHUB_AUTHORIZATION_TOKEN = os.environ.get('STUBHUB_AUTHORIZATION_TOKEN', None)

# StubHub POS API Configuration
# Using demo API from environment variables
STUBHUB_POS_BASE_URL = os.environ.get('STUBHUB_API_BASE_URL')
STUBHUB_POS_AUTH_TOKEN = os.environ.get('STUBHUB_API_TOKEN', "")

# Validate POS API Configuration
def validate_pos_api_configuration():
    """Validate that required POS API environment variables are set"""
    missing_vars = []
    
    if not STUBHUB_POS_BASE_URL:
        missing_vars.append('STUBHUB_API_BASE_URL')
    
    if not STUBHUB_POS_AUTH_TOKEN:
        missing_vars.append('STUBHUB_API_TOKEN')
    
    if missing_vars:
        print(f"WARNING: POS API integration disabled. Missing environment variables: {', '.join(missing_vars)}")
        print("To enable POS API integration, set the following environment variables:")
        for var in missing_vars:
            print(f"  export {var}=<your_value>")
        return False
    
    print(f"✓ POS API configuration valid: {STUBHUB_POS_BASE_URL}")
    return True

# Validate POS configuration on startup (only in production or when explicitly requested)
POS_API_ENABLED = validate_pos_api_configuration()

# Custom directories
TEMP_DATA_DIR = os.path.join(BASE_DIR, 'temp_data')
PROCESSED_DATA_DIR = os.path.join(BASE_DIR, 'processed_data')

# Don't create directories here - let containers handle it
# os.makedirs(TEMP_DATA_DIR, exist_ok=True)
# os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

# Logging configuration for development
if DEBUG:
    # Use console-only logging in development
    LOGGING = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'verbose': {
                'format': '%(levelname)s %(asctime)s %(module)s %(message)s',
            },
            'simple': {
                'format': '%(levelname)s %(message)s',
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'verbose',
            },
        },
        'root': {
            'handlers': ['console'],
            'level': 'INFO',
        },
        'loggers': {
            'django': {
                'handlers': ['console'],
                'level': os.getenv('DJANGO_LOG_LEVEL', 'INFO'),
                'propagate': False,
            },
            'consumer': {
                'handlers': ['console'],
                'level': 'DEBUG',
                'propagate': False,
            },
            'scrapers': {
                'handlers': ['console'],
                'level': 'DEBUG',
                'propagate': False,
            },
        },
    }
else:
    # Production logging with console and file (for robustness)
    LOGGING = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'verbose': {
                'format': '%(levelname)s %(asctime)s %(module)s %(message)s',
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'verbose',
            },
            'file': {
                'class': 'logging.FileHandler',
                'filename': os.path.join(BASE_DIR, 'logs', 'django.log'),
                'formatter': 'verbose',
            },
        },
        'root': {
            'handlers': ['console', 'file'], # Ensure console is always included
            'level': 'INFO',
        },
        'loggers': {
            'django': {
                'handlers': ['console', 'file'],
                'level': os.getenv('DJANGO_LOG_LEVEL', 'INFO'),
                'propagate': False,
            },
            'consumer': {
                'handlers': ['console', 'file'],
                'level': 'DEBUG',
                'propagate': False,
            },
            'scrapers': {
                'handlers': ['console', 'file'],
                'level': 'DEBUG',
                'propagate': False,
            },
        },
    }

REDIS_TTL_HOURS = int(os.environ.get('REDIS_TTL_HOURS', 24))
REDIS_TTL_SECONDS = REDIS_TTL_HOURS * 3600

# NestJS integration settings
NESTJS_NOTIFICATION_QUEUE = os.environ.get('NESTJS_NOTIFICATION_QUEUE', 'django_to_nest')
DEFAULT_SCRAPE_INTERVAL = int(os.environ.get('DEFAULT_SCRAPE_INTERVAL', 10))
# Import optimization enums (optional)
try:
    from scrapers.utils.performance_optimizer import OptimizationLevel
    from scrapers.utils.resource_blocker import BlockingLevel
    OPTIMIZATION_AVAILABLE = True
except ImportError:
    OPTIMIZATION_AVAILABLE = False
    # Create mock enums for fallback
    class OptimizationLevel:
        BALANCED = 'balanced'
        value = 'balanced'
    class BlockingLevel:
        MODERATE = 'moderate'
        value = 'moderate'

# Performance Optimization Configuration
PERFORMANCE_OPTIMIZATION_CONFIG = {
    "level": os.environ.get('OPTIMIZATION_LEVEL', 'balanced'),  # Use string for type safety
    "bandwidth_monitoring": True,
    "auto_fallback": True,
    "proxy": {
        "enabled": int(os.environ.get('PROXY_ENABLED', 0)),
        "rotation": True,
        "rotation_strategy": os.environ.get('PROXY_ROTATION_STRATEGY', 'round_robin'),  # round_robin, random, least_used, fastest
        "health_check_interval": int(os.environ.get('PROXY_HEALTH_CHECK_INTERVAL', 300)),  # seconds
        "health_check_timeout": int(os.environ.get('PROXY_HEALTH_CHECK_TIMEOUT', 10)),
        "max_failures_before_disable": int(os.environ.get('PROXY_MAX_FAILURES', 3)),
        "retry_disabled_after": int(os.environ.get('PROXY_RETRY_AFTER', 1800)),  # seconds
        "proxies": []
    },
    "resource_blocking": {
        "level": os.environ.get('RESOURCE_BLOCKING_LEVEL', 'moderate'),  # Use string for type safety
        "custom_rules": [
            # Example custom blocking rules
            # {
            #     "resource_types": ["image", "media"],
            #     "url_patterns": [".*banner.*", ".*advertisement.*"],
            #     "allow_patterns": [".*logo.*", ".*icon.*"],
            #     "description": "Block banners and ads but allow logos"
            # }
        ]
    }
}

# Add WebShare proxy if configured
if os.environ.get('WEBSHARE_ENABLED', '0') == '1':
    webshare_proxy = {
        "host": os.environ.get('WEBSHARE_HOST', ''),
        "port": int(os.environ.get('WEBSHARE_PORT', '0')),
        "username": os.environ.get('WEBSHARE_USERNAME', ''),
        "password": os.environ.get('WEBSHARE_PASSWORD', ''),
        "protocol": "http"
    }
    if webshare_proxy["host"] and webshare_proxy["port"]:
        PERFORMANCE_OPTIMIZATION_CONFIG["proxy"]["proxies"].append(webshare_proxy)

# Proxy Configuration (Environment Variables)
PROXY_CONFIG = {
    "enabled": int(os.environ.get('PROXY_ENABLED', 0)),
    "providers": {
        # Bright Data (formerly Luminati)
        "bright_data": {
            "enabled": int(os.environ.get('BRIGHT_DATA_ENABLED', 0)),
            "endpoint": os.environ.get('BRIGHT_DATA_ENDPOINT', ''),
            "username": os.environ.get('BRIGHT_DATA_USERNAME', ''),
            "password": os.environ.get('BRIGHT_DATA_PASSWORD', ''),
            "port": int(os.environ.get('BRIGHT_DATA_PORT', 22225)),
        },
        # Smartproxy
        "smartproxy": {
            "enabled": int(os.environ.get('SMARTPROXY_ENABLED', 0)),
            "endpoint": os.environ.get('SMARTPROXY_ENDPOINT', ''),
            "username": os.environ.get('SMARTPROXY_USERNAME', ''),
            "password": os.environ.get('SMARTPROXY_PASSWORD', ''),
            "port": int(os.environ.get('SMARTPROXY_PORT', 10000)),
        },
        # ProxyMesh
        "proxymesh": {
            "enabled": int(os.environ.get('PROXYMESH_ENABLED', 0)),
            "endpoints": os.environ.get('PROXYMESH_ENDPOINTS', '').split(',') if os.environ.get('PROXYMESH_ENDPOINTS') else [],
            "username": os.environ.get('PROXYMESH_USERNAME', ''),
            "password": os.environ.get('PROXYMESH_PASSWORD', ''),
            "port": int(os.environ.get('PROXYMESH_PORT', 31280)),
        }
    },
    "rotation_strategy": os.environ.get('PROXY_ROTATION_STRATEGY', 'round_robin'),
    "health_check_interval": int(os.environ.get('PROXY_HEALTH_CHECK_INTERVAL', 300)),
    "failover_enabled": int(os.environ.get('PROXY_FAILOVER_ENABLED', 1)),
}

# Resource Blocking Configuration
RESOURCE_BLOCKING_CONFIG = {
    "level": os.environ.get('RESOURCE_BLOCKING_LEVEL', 'moderate'),
    "site_specific_rules": {
        "ticketmaster.com": {
            "level": "aggressive",
            "custom_blocks": ["banner", "promo", "advertisement", "social"]
        },
        "stubhub.com": {
            "level": "moderate", 
            "custom_blocks": ["chat", "support", "tracking"]
        },
        "seatgeek.com": {
            "level": "moderate",
            "custom_blocks": ["analytics", "tracking"]
        }
    },
    "bandwidth_monitoring": int(os.environ.get('BANDWIDTH_MONITORING', 1)),
    "performance_logging": int(os.environ.get('PERFORMANCE_LOGGING', 1)),
}

# Performance Monitoring Settings
PERFORMANCE_MONITORING = {
    "enabled": int(os.environ.get('PERFORMANCE_MONITORING_ENABLED', 1)),
    "log_level": os.environ.get('PERFORMANCE_LOG_LEVEL', 'INFO'),
    "metrics_retention_days": int(os.environ.get('METRICS_RETENTION_DAYS', 7)),
    "auto_optimization": int(os.environ.get('AUTO_OPTIMIZATION', 0)),  # Automatically adjust settings based on performance
}

# Additional development settings
# CSRF Trusted Origins - can be overridden by environment variable
CSRF_TRUSTED_ORIGINS = []
csrf_trusted_origins_env = os.environ.get('CSRF_TRUSTED_ORIGINS', '').strip()
if csrf_trusted_origins_env:
    CSRF_TRUSTED_ORIGINS = [origin.strip() for origin in csrf_trusted_origins_env.split(',') if origin.strip()]
elif DEBUG:
    # Allow CSRF from localhost for API testing
    CSRF_TRUSTED_ORIGINS = [
        'http://localhost:8000',
        'http://localhost:3000',
        'http://127.0.0.1:8000',
    ]
else:
    # Default production CSRF trusted origins
    CSRF_TRUSTED_ORIGINS = [
        'http://localhost:8009',  # Django nginx proxy
        'http://127.0.0.1:8009',
        'https://staging-admin.itchtixdashboard.com',  # Production domain
    ]

# Trust proxy headers for reverse proxy setup
USE_X_FORWARDED_HOST = True
USE_X_FORWARDED_PORT = True
SECURE_PROXY_SSL_HEADER = ('HTTP_X_FORWARDED_PROTO', 'https')

# Sentry Configuration
sentry_dsn = os.environ.get('SENTRY_DSN', '').strip()
if sentry_dsn and sentry_dsn.lower() != 'none':
    try:
        sentry_sdk.init(
            dsn=sentry_dsn,
            environment=os.environ.get('SENTRY_ENVIRONMENT', 'development'),
            release=os.environ.get('SENTRY_RELEASE', '1.0.0'),
            integrations=[
                DjangoIntegration(
                    transaction_style='url',
                    middleware_spans=True,
                    signals_spans=True,
                cache_spans=True,
            ),
            CeleryIntegration(
                monitor_beat_tasks=True,
                propagate_traces=True,
            ),
            RedisIntegration(),
        ],
        traces_sample_rate=1.0 if DEBUG else 0.1,
        profiles_sample_rate=1.0 if DEBUG else 0.1,
        send_default_pii=False,
        attach_stacktrace=True,
        include_source_context=True,
        before_send=lambda event, hint: event if DEBUG or event.get('level') != 'info' else None,
    )
    except Exception as e:
        print(f"Warning: Failed to initialize Sentry: {e}")
        # Continue without Sentry rather than crashing

# Unfold Admin Configuration
from django.templatetags.static import static
from django.urls import reverse_lazy
from django.utils.translation import gettext_lazy as _

UNFOLD = {
    "SITE_TITLE": "Theatre Ticket Scraper",
    "SITE_HEADER": "🎭 Event Scraper Dashboard",
    "SITE_URL": "/",
    "SITE_ICON": {
        "light": lambda request: static("admin/img/icon-scraper.svg"),
        "dark": lambda request: static("admin/img/icon-scraper-dark.svg"),
    },
    "SITE_LOGO": {
        "light": lambda request: static("admin/img/logo-scraper.svg"),
        "dark": lambda request: static("admin/img/logo-scraper-dark.svg"),
    },
    "SITE_SYMBOL": "🎭",
    "SITE_FAVICONS": [
        {
            "rel": "icon",
            "sizes": "32x32",
            "type": "image/svg+xml",
            "href": lambda request: static("admin/img/favicon.svg"),
        },
    ],
    "COLORS": {
        "primary": {
            "50": "250 245 255",
            "100": "243 232 255", 
            "200": "233 213 255",
            "300": "216 180 254",
            "400": "196 181 253",
            "500": "139 92 246",
            "600": "124 58 237",
            "700": "109 40 217",
            "800": "91 33 182",
            "900": "76 29 149",
        },
    },
    "EXTENSIONS": {
        "modeltranslation": {
            "flags": {
                "en": "🇺🇸",
                "fr": "🇫🇷",
                "nl": "🇳🇱",
            },
        },
    },
    "SIDEBAR": {
        "show_search": True,
        "show_all_applications": False,
        "navigation": [
            {
                "title": _("🏠 Overview"),
                "separator": True,
                "items": [
                    {
                        "title": _("Dashboard"),
                        "icon": "dashboard",
                        "link": reverse_lazy("admin:index"),
                        "permission": lambda request: request.user.is_authenticated,
                    },
                ],
            },
            {
                "title": _("🤖 Scrapers"),
                "separator": True,
                "collapsible": True,
                "items": [
                    {
                        "title": _("Scraper Definitions"),
                        "icon": "smart_toy",
                        "link": reverse_lazy("admin:scrapers_scraperdefinition_changelist"),
                    },
                    {
                        "title": _("Scraper Status"),
                        "icon": "monitor_heart",
                        "link": reverse_lazy("admin:scrapers_scraperstatus_changelist"),
                    },
                    {
                        "title": _("Scraper Executions"),
                        "icon": "play_circle",
                        "link": reverse_lazy("admin:scrapers_scraperexecution_changelist"),
                    },
                    {
                        "title": _("Scraper Schedules"),
                        "icon": "schedule",
                        "link": reverse_lazy("admin:scrapers_scraperschedule_changelist"),
                    },
                ],
            },
            {
                "title": _("🌐 Proxies"),
                "separator": True,
                "collapsible": True,
                "items": [
                    {
                        "title": _("Proxy Providers"),
                        "icon": "vpn_key",
                        "link": reverse_lazy("admin:scrapers_proxyprovider_changelist"),
                    },
                    {
                        "title": _("Proxy Configurations"),
                        "icon": "settings_ethernet",
                        "link": reverse_lazy("admin:scrapers_proxyconfiguration_changelist"),
                    },
                    {
                        "title": _("Proxy Assignments"),
                        "icon": "link",
                        "link": reverse_lazy("admin:scrapers_scraperproxyassignment_changelist"),
                    },
                    {
                        "title": _("Proxy Usage Logs"),
                        "icon": "history",
                        "link": reverse_lazy("admin:scrapers_proxyusagelog_changelist"),
                    },
                ],
            },
            {
                "title": _("📊 Results & Data"),
                "separator": True,
                "collapsible": True,
                "items": [
                    {
                        "title": _("Scrape Jobs"),
                        "icon": "assignment",
                        "link": reverse_lazy("admin:scrapers_scrapejob_changelist"),
                    },
                    {
                        "title": _("Venues"),
                        "icon": "location_city",
                        "link": reverse_lazy("admin:scrapers_venue_changelist"),
                    },
                    {
                        "title": _("Events"),
                        "icon": "event",
                        "link": reverse_lazy("admin:scrapers_event_changelist"),
                    },
                    {
                        "title": _("Performances"),
                        "icon": "theaters",
                        "link": reverse_lazy("admin:scrapers_performance_changelist"),
                    },
                ],
            },
            {
                "title": _("🪑 Seating Structure"),
                "separator": True,
                "collapsible": True,
                "items": [
                    {
                        "title": _("Levels"),
                        "icon": "layers",
                        "link": reverse_lazy("admin:scrapers_level_changelist"),
                    },
                    {
                        "title": _("Zones"),
                        "icon": "place",
                        "link": reverse_lazy("admin:scrapers_zone_changelist"),
                    },
                    {
                        "title": _("Sections"),
                        "icon": "view_module",
                        "link": reverse_lazy("admin:scrapers_section_changelist"),
                    },
                    {
                        "title": _("Seats"),
                        "icon": "airline_seat_recline_normal",
                        "link": reverse_lazy("admin:scrapers_seat_changelist"),
                    },
                    {
                        "title": _("Seat Packs"),
                        "icon": "inventory_2",
                        "link": reverse_lazy("admin:scrapers_seatpack_changelist"),
                    },
                ],
            },
            {
                "title": _("📈 Price Analytics"),
                "separator": True,
                "collapsible": True,
                "items": [
                    {
                        "title": _("Seat Snapshots"),
                        "icon": "timeline",
                        "link": reverse_lazy("admin:scrapers_seatsnapshot_changelist"),
                    },
                    {
                        "title": _("Level Price History"),
                        "icon": "trending_up",
                        "link": reverse_lazy("admin:scrapers_levelpricesnapshot_changelist"),
                    },
                    {
                        "title": _("Zone Price History"),
                        "icon": "show_chart",
                        "link": reverse_lazy("admin:scrapers_zonepricesnapshot_changelist"),
                    },
                    {
                        "title": _("Section Price History"),
                        "icon": "analytics",
                        "link": reverse_lazy("admin:scrapers_sectionpricesnapshot_changelist"),
                    },
                ],
            },
            {
                "title": _("🔧 System"),
                "separator": True,
                "collapsible": True,
                "items": [
                    {
                        "title": _("Resource Monitor"),
                        "icon": "memory",
                        "link": reverse_lazy("admin:scrapers_resourcemonitor_changelist"),
                    },
                    {
                        "title": _("Users"),
                        "icon": "group",
                        "link": reverse_lazy("admin:auth_user_changelist"),
                    },
                    {
                        "title": _("Groups"),
                        "icon": "security",
                        "link": reverse_lazy("admin:auth_group_changelist"),
                    },
                ],
            },
        ],
    },
    "TABS": [
        {
            "models": [
                "scrapers.venue",
                "scrapers.event", 
                "scrapers.performance",
                "scrapers.level",
                "scrapers.zone",
                "scrapers.section",
                "scrapers.seat",
                "scrapers.seatpack",
            ],
            "items": [
                {
                    "title": _("Event Structure"),
                    "icon": "account_tree",
                    "link": reverse_lazy("admin:scrapers_venue_changelist"),
                },
                {
                    "title": _("Seating"),
                    "icon": "airline_seat_recline_normal",
                    "link": reverse_lazy("admin:scrapers_seat_changelist"),
                },
            ],
        },
        {
            "models": [
                "scrapers.scraperdefinition",
                "scrapers.scraperstatus",
                "scrapers.scraperexecution",
                "scrapers.scraperschedule",
            ],
            "items": [
                {
                    "title": _("Scrapers"),
                    "icon": "smart_toy",
                    "link": reverse_lazy("admin:scrapers_scraperdefinition_changelist"),
                },
                {
                    "title": _("Status"),
                    "icon": "monitor_heart",
                    "link": reverse_lazy("admin:scrapers_scraperstatus_changelist"),
                },
            ],
        },
        {
            "models": [
                "scrapers.proxyprovider",
                "scrapers.proxyconfiguration",
                "scrapers.scraperproxyassignment",
                "scrapers.proxyusagelog",
            ],
            "items": [
                {
                    "title": _("Proxies"),
                    "icon": "vpn_key",
                    "link": reverse_lazy("admin:scrapers_proxyprovider_changelist"),
                },
                {
                    "title": _("Usage"),
                    "icon": "history",
                    "link": reverse_lazy("admin:scrapers_proxyusagelog_changelist"),
                },
            ],
        },
        {
            "models": [
                "scrapers.seatsnapshot",
                "scrapers.levelpricesnapshot",
                "scrapers.zonepricesnapshot", 
                "scrapers.sectionpricesnapshot",
            ],
            "items": [
                {
                    "title": _("Price Analytics"),
                    "icon": "analytics",
                    "link": reverse_lazy("admin:scrapers_seatsnapshot_changelist"),
                },
            ],
        },
    ],
}


# Seat Pack Sync Safety Configuration
# Prevent mass delisting when scraping returns empty results
SEAT_PACK_PREVENT_MASS_DELISTING = int(os.environ.get('SEAT_PACK_PREVENT_MASS_DELISTING', 1))  # 1 = enabled, 0 = disabled
SEAT_PACK_MIN_NEW_PACKS_THRESHOLD = int(os.environ.get('SEAT_PACK_MIN_NEW_PACKS_THRESHOLD', 0))  # Minimum new packs before allowing delisting
