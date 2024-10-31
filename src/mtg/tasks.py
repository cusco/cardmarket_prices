from celery.utils.log import get_task_logger
from django.utils import timezone
from tqdm.auto import tqdm

from cm_prices.celery import app
from mtg.models import ScryfallCard
from mtg.services import scryfall_download_bulk_data, scryfall_transform_card_data

logger = get_task_logger('tasks.common')


@app.task(name='sync_scryfall_task')
def sync_scryfall(*args, **kwargs):
    """Run Scryfall update bulk task."""
    logger.info('BEGINNING SCRYFALL SYNC TASK')

    scryfall_data = scryfall_download_bulk_data()
    new_cards = []
    existing_cards = []
    existing_card_ids = set(str(card_id) for card_id in ScryfallCard.objects.values_list('id', flat=True))

    for raw_card_data in tqdm(scryfall_data, unit='card'):
        card_data = scryfall_transform_card_data(raw_card_data)
        if card_data:
            # Check if the card already exists by cardmarket_id
            if card_data['id'] in existing_card_ids:
                existing_cards.append(ScryfallCard(**card_data))
            else:
                timestamp = timezone.now()
                card_data['date_updated'] = timestamp
                card_data['date_created'] = timestamp
                new_cards.append(ScryfallCard(**card_data))

    # Field list for bulk_update
    fields_to_update = [
        'oracle_id',
        'name',
        'mana_cost',
        'cmc',
        'types',
        'subtypes',
        'colors',
        'color_identity',
        'oracle_text',
        'image_small',
        'image_normal',
        'legalities',
        'cardmarket_id',
    ]

    # Bulk create and update
    if new_cards:
        ScryfallCard.objects.bulk_create(new_cards)
        logger.info('%d new cards inserted.', len(new_cards))
    if existing_cards:
        bulk_update_if_changed(existing_cards, fields_to_update)

    logger.info('SCRYFALL SYNC TASK COMPLETE!')


def bulk_update_if_changed(update_cards, fields):
    """Bulk update only cards that are different."""
    # Create a mapping of cardmarket_id to existing card data
    scryfall_ids = [card.id for card in update_cards]
    existing_cards = {str(card.id): card for card in ScryfallCard.objects.filter(id__in=scryfall_ids)}

    cards_to_update = []

    for update_card in update_cards:
        existing_card = existing_cards.get(update_card.id)
        # Compare fields to see if there are changes
        has_changes = any(getattr(existing_card, field) != getattr(update_card, field) for field in fields)

        if has_changes:
            update_card.date_updated = timezone.now()
            cards_to_update.append(update_card)

    # Perform the bulk update only if there are changes
    if cards_to_update:
        update_fields = fields + ['date_updated']
        ScryfallCard.objects.bulk_update(cards_to_update, update_fields)
        logger.info('Updated %d cards.', len(cards_to_update))
