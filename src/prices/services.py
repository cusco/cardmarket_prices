import gzip
import hashlib
import io
import json
import logging
from datetime import datetime
from pathlib import Path

import pytz
import requests
from bs4 import BeautifulSoup
from curl_cffi import requests as curl
from dateutil import parser
from django.utils import timezone

from lib.utils import update_card_slopes
from prices.models import Catalog, MTGCard, MTGCardPrice, MTGSet

logging.basicConfig(level=logging.INFO)  # temporary
logger = logging.getLogger(__name__)
germany_tz = pytz.timezone('Europe/Berlin')


def update_mtg():
    """Fetch new cards, new prices and save them in the local models."""

    # pricing needs card, card needs set/expansion
    new_sets = create_cm_sets()
    updated_sets = update_sets_extra_info()
    new_cards, updated_cards = update_cm_products()
    updated_prices = update_cm_prices()

    result = {
        'new_sets': new_sets,
        'updated_sets': updated_sets,
        'new_cards': new_cards,
        'updated_cards': updated_cards,
        'updated_prices': updated_prices,
    }

    if updated_prices:
        catalog_date = MTGCardPrice.objects.order_by('catalog_date').last().catalog_date
        card_ids = MTGCardPrice.objects.filter(catalog_date=catalog_date).values_list('cm_id', flat=True)
        card_qs = MTGCard.objects.filter(cm_id__in=card_ids)
        new_slopes, updated_slopes = update_card_slopes(card_qs=card_qs)
        result['new_slopes'] = new_slopes
        result['updated_slopes'] = updated_slopes

    return result


def update_cm_products():
    """Fetch and store product data for MTG cards."""

    # Lists for bulk create/update
    insert_cards = []
    update_cards = []

    url = 'https://downloads.s3.cardmarket.com/productCatalog/productList/products_singles_1.json'

    response = requests.get(url, timeout=10)
    if response.ok:
        # skip catalog if already downloaded
        md5sum = hashlib.md5(response.text.encode('utf-8'), usedforsecurity=False).hexdigest()  # nosemgrep
        existing_catalog = Catalog.objects.filter(md5sum=md5sum, catalog_type=Catalog.PRODUCTS)
        if existing_catalog.exists():
            # catalog_date = existing_catalog.first().catalog_date
            # logger.info('Products (MTG Singles) already up to date since %s (%s)', catalog_date, md5sum)
            return 0, 0

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
            expansion_id = product_item.get('idExpansion', None)
            category_id = product_item.get('idCategory', None)
            metacard_id = product_item.get('idMetacard', None)
            date_added = product_item.get('dateAdded')
            if date_added:
                date_added = datetime.strptime(date_added, '%Y-%m-%d %H:%M:%S')
                date_added = date_added.replace(tzinfo=germany_tz)
            # slug = product_item.get('website', None).replace("/en/", "") if product_item.get('website') else None

            card = MTGCard(
                cm_id=cm_id,
                name=name,
                category_id=category_id,
                expansion_id=expansion_id,
                metacard_id=metacard_id,
                cm_date_added=date_added,
                date_updated=timezone.now(),
            )

            existing_card = existing_cards.get(cm_id)  # Exists?
            # INSERT
            if not existing_card:
                insert_cards.append(card)

            else:
                # for some reason, expansion_id of newly added cards changed the next day... (happened in SL extra life)
                # UPDATE
                fields = ['name', 'expansion_id', 'category_id', 'metacard_id', 'cm_date_added']
                has_changes = any(getattr(existing_card, field) != getattr(card, field) for field in fields)
                if has_changes:
                    update_cards.append(card)

        # Bulk create / update
        if insert_cards:
            MTGCard.objects.bulk_create(insert_cards)
            logger.info('%d new cards inserted.', len(insert_cards))

        if update_cards:
            MTGCard.objects.bulk_update(
                update_cards, fields=['name', 'expansion_id', 'category_id', 'metacard_id', 'date_updated']
            )
            logger.info('%d existing cards updated.', len(update_cards))

    return len(insert_cards), len(update_cards)


def update_cm_prices(local_content=None):
    """Fetch and store catalog prices for MTG cards."""

    # Lists to be used in bulk_create
    insert_prices = []

    # ############ previously downloaded JSON files
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
        # catalog_date = existing_catalog.first().catalog_date
        # logger.info('Prices already up to date since %s (%s)', catalog_date, md5sum)
        return 0

    data = content
    if data['version'] == 1:

        catalog_date = data['createdAt']
        catalog_date = datetime.strptime(catalog_date, '%Y-%m-%dT%H:%M:%S%z')
        Catalog.objects.create(catalog_date=catalog_date, md5sum=md5sum, catalog_type=Catalog.PRICES)

        # List all existing cards within the current JSON
        all_cm_ids = [item['idProduct'] for item in data['priceGuides']]
        existing_cards = MTGCard.objects.filter(cm_id__in=all_cm_ids).in_bulk(field_name='cm_id')
        existing_prices = MTGCardPrice.objects.filter(catalog_date=catalog_date).values_list('cm_id', flat=True)
        existing_price_ids = set(existing_prices)
        unknown_cards = set()

        for price_item in data['priceGuides']:
            cm_id = price_item['idProduct']

            # Check if the card already exists
            # products function should handle this
            card = existing_cards.get(cm_id)
            if not card:
                unknown_cards.add(cm_id)
                # card = MTGCard(cm_id=cm_id)
                # insert_cards.append(card)
                # logger.warning('Card with idProduct %s not found in MTGCard.', cm_id)
                continue

            if cm_id in existing_price_ids:
                logger.warning('Pricing for card %s on date %s already exists.', cm_id, catalog_date)
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

        if unknown_cards:
            logger.warning('Prices with unknown cards: %s', len(unknown_cards))

    return len(insert_prices)


def create_cm_sets():
    """Read cardmarket set names and ids from its select/option HTML element."""

    # url = "https://www.cardmarket.com/en/Magic/Products/Singles"
    url = "https://www.cardmarket.com/en/Magic/Products/Search?idExpansion=0&idRarity=0&perSite=20"
    created_sets = 0

    response = curl.get(url, impersonate='safari')
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
    if created_sets:
        logger.info('Created %d sets', created_sets)
    return created_sets


def update_sets_extra_info():
    """Scrape SETS release_date and set_url from Cardmarket."""

    missing_info = MTGSet.objects.filter(url__isnull=True) | MTGSet.objects.filter(release_date__isnull=True)
    if not missing_info.exists():
        return 0

    url = "https://www.cardmarket.com/en/Magic/Expansions"
    response = curl.get(url=url, impersonate='safari')
    if not response.ok:
        logger.error('Could not read cardmmarket.com url: %s', response.status_code)
        return 0
    update_sets = []

    if response.ok:
        soup = BeautifulSoup(response.text, 'html.parser')

        for exp_row in soup.find_all('div', attrs={'class': 'expansion-row'}):
            # exp_code = None
            # exp_card_qty = int(exp_row.find_all('div')[4].text.split(' ')[0])
            exp_url = exp_row.attrs.get('data-url', None)
            exp_name = exp_row.attrs.get('data-local-name', None)
            exp_release_date = exp_row.find_all('div')[5].text

            local_set = MTGSet.objects.filter(name=exp_name).first()
            if not local_set:
                continue

            to_update = False

            if not local_set.url:
                local_set.url = exp_url
                to_update = True
            if not local_set.release_date:
                local_set.release_date = parser.parse(exp_release_date)
                to_update = True

            if to_update:
                update_sets.append(local_set)
                # local_set.save(update_fields=['code', 'release_date'])

        if update_sets:
            MTGSet.objects.bulk_update(update_sets, ['url', 'release_date'])
            logger.info('Updated %d sets', len(update_sets))

    return len(update_sets)


def update_from_local_files():
    """Update prices from local JSON files compressed in .gz."""
    directory = Path("../local/catalogs")
    catalog_files = sorted(directory.glob("202*json.gz"), key=lambda f: f.name)
    for catalog_file in catalog_files:
        try:
            with gzip.open(catalog_file, "rb") as gz_file:
                # noinspection PyTypeChecker
                content = io.TextIOWrapper(gz_file, encoding='utf-8').read()
            update_cm_prices(local_content=content)
        except (OSError, IOError, ValueError) as err:
            logger.error("Failed to update from %s: %s", catalog_file, err)


def not_used_old_update_cm_sets_extra():
    """
    Update cardmarket set extra info based on mtgjson.com.

    After some verification, data in mtgjson is not completely linked with Cardmarket.
    Skipping this for now, but maybe it gets fixed in the future.
    See: https://github.com/mtgjson/mtgjson/issues/1236#issuecomment-2430108124
    """

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


def get_set_code(url, proxies=None):
    """Given a cardmarket card url, find the SET it belongs to and return its code."""

    # url = f'https://www.cardmarket.com/en/Magic/Products/Singles/{set_name}'
    # url = url.replace("Magic/Expansions", "Magic/Products/Singles")
    response = curl.get(url=url, impersonate='safari', proxies=proxies)
    if not response.ok:
        return -1

    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('div', attrs={'class': 'table-body'})
    if not table:
        return None
    span = table.find('span', attrs={'class': 'is-magic'})
    if 'title' in span.attrs:
        title = span['title']
    elif 'data-bs-title' in span.attrs:
        title = span['data-bs-title']
    else:
        return None
    code = title.split('/')[4]

    return code
