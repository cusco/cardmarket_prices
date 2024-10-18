import hashlib
import json
import logging
from datetime import datetime

import pytz
import requests
from bs4 import BeautifulSoup
from curl_cffi import requests as curl

from prices.models import Catalog, MTGCard, MTGCardPrice, MTGSet

logging.basicConfig(level=logging.INFO)  # temporary
logger = logging.getLogger(__name__)
germany_tz = pytz.timezone('Europe/Berlin')


def update_mtg():
    """Fetch new cards, new prices and save them in the local models."""

    new_sets = update_cm_sets()
    updated_sets = update_cm_sets_extra()
    new_cards = update_cm_products()
    updated_prices = update_cm_prices()

    return new_sets, updated_sets, new_cards, updated_prices


def update_cm_products():
    """Fetch and store product data for MTG cards."""

    # Lists for bulk create/update
    insert_cards = []

    url = 'https://downloads.s3.cardmarket.com/productCatalog/productList/products_singles_1.json'

    response = requests.get(url, timeout=10)
    if response.ok:
        # skip catalog if already downloaded
        md5sum = hashlib.md5(response.text.encode('utf-8'), usedforsecurity=False).hexdigest()  # nosemgrep
        existing_catalog = Catalog.objects.filter(md5sum=md5sum, catalog_type=Catalog.PRODUCTS)
        if existing_catalog.exists():
            catalog_date = existing_catalog.first().catalog_date
            logger.info('Products (MTG Singles) already up to date since %s (%s)', catalog_date, md5sum)
            return 0

        data = response.json()

        catalog_date = data['createdAt']
        catalog_date = datetime.strptime(catalog_date, '%Y-%m-%dT%H:%M:%S%z')
        Catalog.objects.create(catalog_date=catalog_date, md5sum=md5sum, catalog_type=Catalog.PRODUCTS)

        # List all existing cards within the current JSON
        all_cm_ids = [item['idProduct'] for item in data['products']]
        existing_cards = MTGCard.objects.filter(cm_id__in=all_cm_ids).in_bulk(field_name='cm_id')

        for product_item in data['products']:
            cm_id = product_item['idProduct']
            name = product_item.get('name', None)
            # slug = product_item.get('website', None).replace("/en/", "") if product_item.get('website') else None

            # Check if the card already exists
            card = existing_cards.get(cm_id)
            if not card:

                date_added = product_item.get('dateAdded')
                if date_added:
                    date_added = datetime.strptime(date_added, '%Y-%m-%d %H:%M:%S')  # convert str to date
                    date_added = date_added.replace(tzinfo=germany_tz)

                card = MTGCard(
                    cm_id=cm_id,
                    name=name,
                    category_id=product_item.get('idCategory', None),
                    expansion_id=product_item.get('idExpansion', None),
                    metacard_id=product_item.get('idMetacard', None),
                    cm_date_added=date_added,
                )
                insert_cards.append(card)

        # Bulk create new cards
        if insert_cards:
            MTGCard.objects.bulk_create(insert_cards)
            logger.info('%d new cards inserted.', len(insert_cards))

    return len(insert_cards)


def update_cm_prices(local_content=None):
    """Fetch and store catalog prices for MTG cards."""

    # Lists to be used in bulk_create
    insert_prices = []

    # ############ previously downloaded json files
    if local_content:
        md5sum = hashlib.md5(local_content.encode('utf-8'), usedforsecurity=False).hexdigest()  # nosemgrep
        try:
            content = json.loads(local_content)
        except json.JSONDecodeError as e:
            logger.error("Failed to decode JSON: %s", e)
            return 0

    # ############ Typical behaviour
    else:
        url = 'https://downloads.s3.cardmarket.com/productCatalog/priceGuide/price_guide_1.json'
        response = requests.get(url, timeout=10)
        if response.ok:
            content = response.json()
            md5sum = hashlib.md5(response.text.encode('utf-8'), usedforsecurity=False).hexdigest()  # nosemgrep
        else:
            logger.error('Unable to download JSON: %s', response.text)
            return 0

    existing_catalog = Catalog.objects.filter(md5sum=md5sum, catalog_type=Catalog.PRICES)
    if existing_catalog.exists():
        catalog_date = existing_catalog.first().catalog_date
        logger.info('Prices already up to date since %s (%s)', catalog_date, md5sum)
        return 0

    data = content
    if data['version'] == 1:

        catalog_date = data['createdAt']
        catalog_date = datetime.strptime(catalog_date, '%Y-%m-%dT%H:%M:%S%z')
        Catalog.objects.create(catalog_date=catalog_date, md5sum=md5sum, catalog_type=Catalog.PRICES)

        # List all existing cards within the current JSON
        all_cm_ids = [item['idProduct'] for item in data['priceGuides']]
        existing_cards = MTGCard.objects.filter(cm_id__in=all_cm_ids).in_bulk(field_name='cm_id')

        for price_item in data['priceGuides']:
            cm_id = price_item['idProduct']

            # Check if the card already exists
            # products function should handle this
            card = existing_cards.get(cm_id)
            if not card:
                # card = MTGCard(cm_id=cm_id)
                # insert_cards.append(card)
                logger.warning('Card with idProduct %s not found in MTGCard.', cm_id)
                continue

            mtg_card_price = MTGCardPrice(
                catalog_date=catalog_date,
                card=card,
                cm_id=cm_id,
                avg=price_item.get('avg', None),
                low=price_item.get('low', None),
                trend=price_item.get('trend', None),
                avg1=price_item.get('avg1', None),
                avg7=price_item.get('avg7', None),
                avg30=price_item.get('avg30', None),
                avg_foil=price_item.get('avg-foil', None),
                low_foil=price_item.get('low-foil', None),
                trend_foil=price_item.get('trend-foil', None),
                avg1_foil=price_item.get('avg1-foil', None),
                avg7_foil=price_item.get('avg7-foil', None),
                avg30_foil=price_item.get('avg30-foil', None),
            )

            insert_prices.append(mtg_card_price)

        # Bulk create all prices
        if insert_prices:
            MTGCardPrice.objects.bulk_create(insert_prices)
            logger.info('%d new prices inserted.', len(insert_prices))

    return len(insert_prices)


def update_cm_sets():
    """Update cardmarket set names and ids."""

    url = "https://www.cardmarket.com/en/Magic/Products/Singles"
    created_sets = 0

    response = curl.get(url, impersonate='chrome')
    if not response.ok:
        logger.error('Could not read cardmmarket.com url: %s', response.status_code)
        return 0

    soup = BeautifulSoup(response.text, 'html.parser')
    cm_sets = soup.find('select', attrs={'name': 'idExpansion'})
    existing_sets = MTGSet.objects.values_list('expansion_id', flat=True)

    for opt in cm_sets.find_all('option'):
        set_id = int(opt.get('value').strip())
        if set_id == 0:  # skip "All" option
            continue

        if set_id not in existing_sets:
            set_name = opt.text.strip()
            new_set = MTGSet(name=set_name, expansion_id=set_id)
            new_set.save()
            created_sets += 1
            # logger.info('Created new set %s.', set_name)
    logger.info('Created %d sets', created_sets)
    return created_sets


def update_cm_sets_extra():
    """Update cardmarket set extra info based on mtgjson.com."""

    url = "https://mtgjson.com/api/v5/SetList.json"
    updated_sets = 0

    response = requests.get(url, timeout=10)
    if not response.ok:
        logger.error('Could not read cardmmarket.com url: %s', response.status_code)
        return 0

    data = response.json()['data']

    for set_obj in data:
        cm_id = set_obj.get('mcmId')
        cm_id_extra = set_obj.get('mcmIdExtras')

        if cm_id:
            existing_set = MTGSet.objects.get(expansion_id=cm_id)
            if not existing_set.code:
                existing_set.code = set_obj.get('code')
                existing_set.release_date = set_obj.get('releaseDate')
                existing_set.type = set_obj.get('type')
                existing_set.is_foil_only = set_obj.get('isFoilOnly')
                existing_set.save()
                updated_sets += 1

        if cm_id_extra:
            existing_set = MTGSet.objects.get(expansion_id=cm_id_extra)
            if not existing_set.code:
                existing_set.code = set_obj.get('code')
                existing_set.release_date = set_obj.get('releaseDate')
                existing_set.type = set_obj.get('type')
                existing_set.is_foil_only = set_obj.get('isFoilOnly')
                existing_set.save()
                updated_sets += 1

    logger.info('Updated %d sets', updated_sets)
    return updated_sets
