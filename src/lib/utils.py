import hashlib
import json
import logging
from datetime import datetime

import pytz
import requests

from prices.models import Catalog, MTGCard, MTGCardPrice

logging.basicConfig(level=logging.INFO)  # temporary
logger = logging.getLogger(__name__)
germany_tz = pytz.timezone('Europe/Berlin')


def update_mtg():
    """Fetch new cards, new prices and save them in the local models."""

    new_cards = update_cm_products()
    updated_prices = update_cm_prices()
    logger.info('Added new %d cards and updated %d prices', new_cards, updated_prices)


def update_cm_products():
    """Fetch and store product data for MTG cards."""

    # Lists for bulk create/update
    insert_cards = []

    url = 'https://downloads.s3.cardmarket.com/productCatalog/productList/products_singles_1.json'

    response = requests.get(url, timeout=10)
    if response.ok:
        # skip catalog if already downloaded
        md5sum = hashlib.md5(response.text.encode('utf-8'), usedforsecurity=False).hexdigest()  # nosemgrep
        if Catalog.objects.filter(md5sum=md5sum, catalog_type=Catalog.PRODUCTS).exists():
            catalog_date = Catalog.objects.get(md5sum=md5sum, catalog_type=Catalog.PRODUCTS).catalog_date
            logger.info('Products already up to date since %s (%s)', catalog_date, md5sum)
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


def update_cm_prices():
    """Fetch and store catalog prices for MTG cards."""

    # Lists to be used in bulk_create
    insert_prices = []

    url = 'https://downloads.s3.cardmarket.com/productCatalog/priceGuide/price_guide_1.json'

    response = requests.get(url, timeout=10)
    if response.ok:
        # skip catalog if already downloaded
        md5sum = hashlib.md5(response.text.encode('utf-8'), usedforsecurity=False).hexdigest()  # nosemgrep
        if Catalog.objects.filter(md5sum=md5sum, catalog_type=Catalog.PRICES).exists():
            catalog_date = Catalog.objects.get(md5sum=md5sum, catalog_type=Catalog.PRICES).catalog_date
            logger.info('Prices already up to date since %s (%s)', catalog_date, md5sum)
            return 0

        data = response.json()
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


def save_local_prices(content):
    """User previously saved jsons to save its prices info in the MTGCardPrice model."""

    # from pathlib import Path
    # directory = Path('../local/catalogs')
    # for file in directory.glob("202*json"):
    #     with open(file, 'r') as f:
    #         content = f.read()
    #         try:
    #             save_local_prices(content)
    #         except Exception as err:
    #             print(err)

    # Lists to be used in bulk_create
    insert_prices = []

    # skip catalog if already downloaded
    md5sum = hashlib.md5(content.encode('utf-8'), usedforsecurity=False).hexdigest()  # nosemgrep
    if Catalog.objects.filter(md5sum=md5sum, catalog_type=Catalog.PRICES).exists():
        catalog_date = Catalog.objects.get(md5sum=md5sum, catalog_type=Catalog.PRICES).catalog_date
        logger.info('Prices already up to date since %s (%s)', catalog_date, md5sum)
        return 0

    data = json.loads(content)
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
                logger.warning('Card with idProduct %d not found in MTGCard.', cm_id)
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
