from celery import group
from celery.utils.log import get_task_logger

from mtg.models import ScryfallCard
from mtg.services import scryfall_download_bulk_data, scryfall_transform_card_data, scryfall_save_card
from cm_prices.celery import app
from tqdm.auto import tqdm

logger = get_task_logger('tasks.common')


@app.task(name='sync_scryfall_task')
def sync_scryfall(*args, **kwargs):
    """Run scryfall update bulk task."""

    logger.info('BEGINNING SCRYFALL SYNC TASK')
    scryfall_data = scryfall_download_bulk_data()
    if kwargs.get('test'):
        scryfall_data = scryfall_data[:2]
    load_tasks = []
    for raw_card_data in tqdm(scryfall_data, unit='card'):
        card = scryfall_transform_card_data(raw_card_data)
        if card:
            if not ScryfallCard.objects.filter(cardmarket_id=card.get('cardmarket_id')).exists():
                load_tasks.append(get_or_create_scryfall_card.s(card))
    task_group = group(load_tasks)
    task_group.apply()
    logger.info('SCRYFALL SYNC TASK COMPLETE!')


@app.task(name='get_or_create_scryfall_card')
def get_or_create_scryfall_card(card_data):
    """Create card in local scryfall model."""

    created, card = scryfall_save_card(card_data)
    if created:
        logger.info('Created new Scryfall card: %s', card.name)