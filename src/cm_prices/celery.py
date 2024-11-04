import os

from celery import Celery
from celery.schedules import crontab

# Set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'cm_prices.settings')

app = Celery('cm_prices')

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object('django.conf:settings', namespace='CELERY')

# Load task modules from all registered Django apps.
app.autodiscover_tasks()
# app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)


app.conf.beat_schedule = {
    'sync_scryfall': {'task': 'sync_scryfall_task', 'schedule': crontab(minute='38', hour='*/2')},
    'update_mtg': {'task': 'update_mtg_task', 'schedule': crontab(minute='43', hour='*/2')},
}

app.conf.timezone = 'Europe/London'
