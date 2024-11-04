from celery.utils.log import get_task_logger

from cm_prices.celery import app
from prices.services import update_mtg

logger = get_task_logger('tasks.common')


@app.task(name='update_mtg_task')
def update_mtg_task():
    """Fetch new cards, new prices and save them in the local models."""
    update_mtg()
