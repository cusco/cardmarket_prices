from celery.utils.log import get_task_logger

from cm_prices.celery import app
from mtg.services import update_scryfall_data

logger = get_task_logger('tasks.common')


@app.task(name='sync_scryfall_task')
def sync_scryfall(*args, **kwargs):
    """Run Scryfall update bulk task."""
    logger.info('BEGINNING SCRYFALL SYNC TASK')
    update = update_scryfall_data(disable_progress=True)
    logger.info(update)
