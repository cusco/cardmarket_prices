import os

from celery import Celery
from celery.schedules import crontab
from celery.signals import task_postrun, task_prerun
from django.db import close_old_connections

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
    'sync_scryfall': {'task': 'sync_scryfall_task', 'schedule': crontab(minute='28', hour='*/4')},
    'update_mtg': {'task': 'update_mtg_task', 'schedule': crontab(minute='58', hour='*')},
}

app.conf.timezone = 'Europe/London'


@task_prerun.connect
def db_health_check_before_task(*args, **kwargs):
    """Flush out dead database connections before a task executes."""
    close_old_connections()


@task_postrun.connect
def db_cleanup_after_task(*args, **kwargs):
    """Close old database connections after a task completes to prevent leaks."""
    close_old_connections()
