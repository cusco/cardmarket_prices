import json
import logging
import unicodedata

import requests
from django.utils import timezone
from tqdm.auto import tqdm

from .constants import BASIC_TYPES, SCRYFALL_BULK_DATA_URL
from .models import ScryfallCard

logger = logging.getLogger(__name__)

# Inspired on https://github.com/baronvonvaderham/django-mtg-card-catalog


def process_card_types(card_data):
    """Split and clean up types and subtypes for a card."""

    types = card_data.get('type_line', '')

    # If type_line is empty, gather types from card_faces if available
    if not types and 'card_faces' in card_data:
        types = ' // '.join(card_face.get('type_line', '') for card_face in card_data['card_faces'])

    types = types.replace('—', '-').split(' // ')
    card_types, card_subtypes = [], []

    for type_line in types:
        if not type_line:
            continue

        # If there is a ' - ', that means we have subtypes to the right, supertypes to the left
        if ' - ' in type_line:
            main_types, subtypes = type_line.split(' - ')
        else:
            main_types, subtypes = type_line, None

        if subtypes:
            card_subtypes.extend(subtypes.split())

        card_types.extend(main_types.split())
        if len(set(types)) == 1:
            break

    return card_types, card_subtypes


def scryfall_download_bulk_data(disable_progress=False):
    """Download the bulk data file from Scryfall."""
    response = requests.get(SCRYFALL_BULK_DATA_URL, timeout=10)
    response.raise_for_status()  # Raise an error for bad responses
    url = response.json()

    # Find bulk data url
    url = next(item for item in url['data'] if item['type'] == 'default_cards')
    url = url['download_uri']

    # Download in chunks
    response = requests.get(url, timeout=10, stream=True)
    response.raise_for_status()

    total_size = int(response.headers.get('Content-Length', 0)) if 'Content-Length' in response.headers else None
    with tqdm(total=total_size, unit='B', unit_scale=True, desc='Downloading', disable=disable_progress) as pg_bar:
        json_data = []
        for chunk in response.iter_content(chunk_size=8192):
            json_data.append(chunk)
            pg_bar.update(len(chunk))

    return json.loads(b''.join(json_data))


def scryfall_transform_card_data(raw_card_data):
    """Convert raw Scryfall data to model-compatible format, applying constants-based filters and transformations."""

    # Skipping unwanted stuff
    skipping_ids = {'90f17b85-a866-48e8-aae0-55330109550e'}
    if not raw_card_data.get('cardmarket_id'):
        return None
    if raw_card_data.get('name').split(' ')[0] in BASIC_TYPES:
        return None
    if '(' in raw_card_data.get('name'):
        return None
    if raw_card_data.get('id') in skipping_ids:
        return None

    scryfall_id = raw_card_data.get('id')
    oracle_id = raw_card_data.get('oracle_id')
    cardmarket_id = raw_card_data.get('cardmarket_id')
    card_name = raw_card_data.get('name', '')
    card_name = ''.join(c for c in unicodedata.normalize('NFD', card_name) if unicodedata.category(c) != 'Mn')
    card_types, card_subtypes = process_card_types(raw_card_data)
    mana_cost = []
    colors = set()  # avoid duplicates
    oracle_text = []
    legalities = raw_card_data.get('legalities', None)
    image_small = None
    image_normal = None
    color_identity = raw_card_data.get('color_identity')
    cmc = raw_card_data.get('cmc')

    # Split cards
    if ' // ' in card_name:
        for card_face in raw_card_data.get('card_faces', []):
            mana_cost.append(card_face.get('mana_cost'))
            colors.update(card_face.get('colors', []))
            oracle_text.append(card_face.get('oracle_text'))

            names = card_name.split(' // ')
            if len(set(names)) == 1:  # avoid reversible cards with same name
                card_name = names[0]
                break
        colors = list(colors)
    else:
        mana_cost = [raw_card_data.get('mana_cost')]
        colors = raw_card_data.get('colors', [])
        oracle_text = [raw_card_data.get('oracle_text')]

    if legalities:
        legal_card_types = [card_type for card_type, status in legalities.items() if status == 'legal']
        legalities = ','.join(legal_card_types)

    # Check for image URIs in raw_card_data
    image_uris = raw_card_data.get('image_uris') or (
        raw_card_data.get('card_faces', [{}])[0].get('image_uris') if 'card_faces' in raw_card_data else None
    )

    if image_uris:
        image_small = image_uris.get('small' if 'image_uris' in raw_card_data else 'image_small', None)
        image_normal = image_uris.get('image_normal' if 'image_uris' in raw_card_data else 'normal', None)

    transformed_data = {
        'id': scryfall_id,
        'oracle_id': oracle_id,
        'name': card_name,
        'mana_cost': json.dumps(mana_cost),
        'cmc': cmc,
        'types': json.dumps(card_types),
        'subtypes': json.dumps(card_subtypes),
        'colors': json.dumps(list(colors)),
        'color_identity': json.dumps(color_identity),
        'oracle_text': json.dumps(oracle_text),
        'cardmarket_id': cardmarket_id,
        'image_small': image_small,
        'image_normal': image_normal,
        'legalities': legalities,
    }
    return transformed_data


def bulk_update_if_changed(update_cards):
    """Bulk update only cards that are different."""
    # Create a mapping of cardmarket_id to existing card data
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
    scryfall_ids = [card.id for card in update_cards]
    existing_cards = {str(card.id): card for card in ScryfallCard.objects.filter(id__in=scryfall_ids)}

    cards_to_update = []

    for update_card in update_cards:
        existing_card = existing_cards.get(update_card.id)
        # Compare fields to see if there are changes
        has_changes = any(getattr(existing_card, field) != getattr(update_card, field) for field in fields_to_update)

        if has_changes:
            update_card.date_updated = timezone.now()
            cards_to_update.append(update_card)

    # Perform the bulk update only if there are changes
    if cards_to_update:
        update_fields = fields_to_update + ['date_updated']
        ScryfallCard.objects.bulk_update(cards_to_update, update_fields)
        logger.info('Updated %d cards.', len(cards_to_update))

    return len(cards_to_update)


def update_scryfall_data(disable_progress=False):
    """Update Scryfall data in the local database."""

    scryfall_data = scryfall_download_bulk_data(disable_progress)
    updated_cards = 0
    new_cards = []
    existing_cards = []
    existing_card_ids = set(str(card_id) for card_id in ScryfallCard.objects.values_list('id', flat=True))

    for raw_card_data in tqdm(scryfall_data, unit='card', disable=disable_progress):
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

    # Bulk create and update
    if new_cards:
        ScryfallCard.objects.bulk_create(new_cards)
        logger.info('%d new cards inserted.', len(new_cards))
    if existing_cards:
        updated_cards = bulk_update_if_changed(existing_cards)

    return {'new_cards': len(new_cards), 'updated_cards': updated_cards}
