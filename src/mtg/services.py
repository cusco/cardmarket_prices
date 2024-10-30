import unicodedata

import requests

from .constants import BASIC_TYPES, SCRYFALL_BULK_DATA_URL
from .models import ScryfallCard

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
        if ' — ' in type_line:
            main_types, subtypes = type_line.split(' - ')
        else:
            main_types, subtypes = type_line, None

        if subtypes:
            card_subtypes.extend(subtypes.split())

        card_types.extend(main_types.split())
        if len(set(types)) == 1:
            break

    return card_types, card_subtypes


def scryfall_download_bulk_data():
    """Download the bulk data file from Scryfall."""
    response = requests.get(SCRYFALL_BULK_DATA_URL, timeout=10)
    response.raise_for_status()  # Raise an error for bad responses
    url = response.json()
    url = next(item for item in url['data'] if item['type'] == 'default_cards')
    url = url['download_uri']

    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


def scryfall_process_data(data):
    """Parse each process each card entry from data."""
    for raw_card_data in data:
        transformed_data = scryfall_transform_card_data(raw_card_data)
        if transformed_data:
            scryfall_save_card(transformed_data)


def scryfall_transform_card_data(raw_card_data):
    """Convert raw Scryfall data to model-compatible format, applying constants-based filters and transformations."""

    # Skipping unwanted stuff
    if not raw_card_data.get('cardmarket_id'):
        return None
    if raw_card_data.get('name').split(' ')[0] in BASIC_TYPES:
        return None
    if '(' in raw_card_data.get('name'):
        return None

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
    cardmarket_id = raw_card_data.get('cardmarket_id')

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
        'oracle_id': raw_card_data.get('oracle_id'),
        'name': card_name,
        'mana_cost': mana_cost,
        'cmc': raw_card_data.get('cmc'),
        'types': card_types,
        'subtypes': card_subtypes,
        'colors': list(colors),
        'color_identity': color_identity,
        'oracle_text': oracle_text,
        'cardmarket_id': cardmarket_id,
        'image_small': image_small,
        'image_normal': image_normal,
        'legalities': legalities,
    }
    return transformed_data


def scryfall_save_card(card_data):
    """Save a new card or update an existing card in the database."""
    _, created = ScryfallCard.objects.update_or_create(cardmarket_id=card_data['cardmarket_id'], defaults=card_data)
    return created


def update_scryfall_data():
    """Update Scryfall data in the local database."""
    data = scryfall_download_bulk_data()

    # Process and save data
    scryfall_process_data(data)
