from .base import *  # NOQA

DEBUG = False
TEMPLATE_DEBUG = False

DATABASES = {
    "default": {
        # os.environ.get('KEY', 'DEFAULT_FALLBACK')
        "ENGINE": os.environ.get("DB_ENGINE", "django.db.backends.mysql"),
        "NAME": os.environ.get("DB_NAME", "cm_prices"),
        "USER": os.environ.get("DB_USER"),
        "PASSWORD": os.environ.get("DB_PASSWORD"),
        "HOST": os.environ.get("DB_HOST", "127.0.0.1"),
        "PORT": os.environ.get("DB_PORT", "3306"),
        "OPTIONS": {
            "init_command": "SET sql_mode='STRICT_TRANS_TABLES'",
        },
    }
}