import gzip
import hashlib
import io
import json
import logging
import time
from datetime import datetime
from pathlib import Path

import pytz
import requests
from django.utils import timezone

from prices.models import Catalog, MTGCard, MTGCardPrice

logger = logging.getLogger(__name__)
germany_tz = pytz.timezone('Europe/Berlin')


def update_from_local_files_with_retry(from_date=None, max_retries=3, delay_between_retries=2, force_reprocess=False):
    """
    Update prices from local JSON files compressed in .gz with retry logic and date filtering.

    Args:
        from_date: datetime or date object to filter files from that date onwards
        max_retries: Maximum number of retry attempts per file
        delay_between_retries: Seconds to wait between retries
        force_reprocess: If True, reprocess files even if they were already processed

    Returns
    -------
        dict: Summary of processing results

    """
    directory = Path('../local/catalogs')

    if not directory.exists():
        logger.error('Catalog directory does not exist: %s', directory)
        return {'error': 'Directory not found', 'processed': 0, 'failed': 0, 'skipped': 0}

    # Get all catalog files
    catalog_files = sorted(directory.glob('202*json.gz'), key=lambda f: f.name)

    if not catalog_files:
        logger.warning('No catalog files found in %s', directory)
        return {'processed': 0, 'failed': 0, 'skipped': 0}

    # Filter files by date if specified
    if from_date:
        if hasattr(from_date, 'date'):
            filter_date = from_date.date()
        else:
            filter_date = from_date

        filtered_files = []
        for catalog_file in catalog_files:
            try:
                # Extract date from filename (assuming format: YYYY-MM-DD_hash_price_guide_1.json.gz)
                file_date_str = catalog_file.name[:10]  # The first 10 chars should be YYYY-MM-DD
                file_date = datetime.strptime(file_date_str, '%Y-%m-%d').date()

                if file_date >= filter_date:
                    filtered_files.append(catalog_file)
                else:
                    logger.debug('Skipping file %s (before filter date %s)', catalog_file.name, filter_date)
            except ValueError:
                logger.warning('Could not parse date from filename: %s', catalog_file.name)
                # Include files with unparseable dates to be safe
                filtered_files.append(catalog_file)

        catalog_files = filtered_files
        logger.info('Filtered to %d files from date %s onwards', len(catalog_files), from_date)

    results = {
        'processed': 0,
        'failed': 0,
        'skipped': 0,
        'total_new_prices': 0,
        'file_details': [],
    }

    for catalog_file in catalog_files:
        file_result = process_single_catalog_file(
            catalog_file,
            max_retries=max_retries,
            delay_between_retries=delay_between_retries,
            force_reprocess=force_reprocess,
        )

        results['file_details'].append(
            {
                'file': catalog_file.name,
                'status': file_result['status'],
                'new_prices': file_result['new_prices'],
                'attempts': file_result['attempts'],
                'error': file_result.get('error'),
            }
        )

        if file_result['status'] == 'processed':
            results['processed'] += 1
            results['total_new_prices'] += file_result['new_prices']
        elif file_result['status'] == 'failed':
            results['failed'] += 1
        else:  # skipped
            results['skipped'] += 1

    logger.info(
        'Processing complete: %d processed, %d failed, %d skipped, %d total new prices',
        results['processed'],
        results['failed'],
        results['skipped'],
        results['total_new_prices'],
    )

    return results


def process_single_catalog_file(catalog_file, max_retries=3, delay_between_retries=2, force_reprocess=False):
    """
    Process a single catalog file with retry logic.

    Returns
    -------
        dict: Processing result with status, new_prices, attempts, and optional error

    """
    result = {'status': 'failed', 'new_prices': 0, 'attempts': 0, 'error': None}

    for attempt in range(max_retries):
        result['attempts'] = attempt + 1

        try:
            logger.info('Processing %s (attempt %d/%d)', catalog_file.name, attempt + 1, max_retries)

            # Read and decompress a file
            with gzip.open(catalog_file, 'rb') as gz_file:
                content = io.TextIOWrapper(gz_file, encoding='utf-8').read()

            # Check if already processed (unless force_reprocess is True)
            if not force_reprocess:
                md5sum = hashlib.md5(content.encode('utf-8'), usedforsecurity=False).hexdigest()  # nosemgrep
                existing_catalog = Catalog.objects.filter(md5sum=md5sum, catalog_type=Catalog.PRICES)
                if existing_catalog.exists():
                    catalog_date = existing_catalog.first().catalog_date
                    logger.info(
                        'File %s already processed on %s (md5: %s)', catalog_file.name, catalog_date, md5sum[:8]
                    )
                    result['status'] = 'skipped'
                    return result

            # Process the content
            new_prices = update_cm_prices(local_content=content)

            if new_prices is not None:
                result['status'] = 'processed'
                result['new_prices'] = new_prices
                logger.info('Successfully processed %s: %d new prices', catalog_file.name, new_prices)
                return result

            raise ValueError('update_cm_prices returned None')

        except (OSError, IOError, ValueError, json.JSONDecodeError) as err:
            error_msg = f'Attempt {attempt + 1} failed for {catalog_file.name}: {err}'
            logger.warning(error_msg)
            result['error'] = str(err)

            # If not the last attempt, wait before retrying
            if attempt < max_retries - 1:
                logger.info('Waiting %d seconds before retry...', delay_between_retries)
                time.sleep(delay_between_retries)
            else:
                logger.error('All %d attempts failed for %s', max_retries, catalog_file.name)

    return result


def update_cm_prices(local_content=None):
    """
    Enhanced version of the existing update_cm_prices function with better error handling.

    This is an improved version with better error handling and duplicate prevention.
    """
    # Lists to be used in bulk_create
    insert_prices = []

    if local_content:
        md5sum = hashlib.md5(local_content.encode('utf-8'), usedforsecurity=False).hexdigest()  # nosemgrep
        try:
            content = json.loads(local_content)
        except json.JSONDecodeError as exc:
            logger.error('Failed to decode JSON: %s', exc)
            return None
    else:
        # This is the original HTTP fetch logic - keeping for compatibility
        url = 'https://downloads.s3.cardmarket.com/productCatalog/priceGuide/price_guide_1.json'
        try:
            response = requests.get(url, timeout=10)
            if response.ok:
                content = response.json()
                md5sum = hashlib.md5(response.text.encode('utf-8'), usedforsecurity=False).hexdigest()  # nosemgrep
            else:
                logger.error('Unable to download JSON: %s', response.text)
                return None
        except requests.RequestException as exc:
            logger.error('Error fetching from URL: %s', exc)
            return None

    # Check if already processed
    existing_catalog = Catalog.objects.filter(md5sum=md5sum, catalog_type=Catalog.PRICES)
    if existing_catalog.exists():
        return 0

    data = content
    if data.get('version') != 1:
        logger.error('Unexpected JSON version: %s', data.get('version'))
        return None

    catalog_date = data['createdAt']
    catalog_date = datetime.strptime(catalog_date, '%Y-%m-%dT%H:%M:%S%z')

    # Create catalog entry
    Catalog.objects.create(catalog_date=catalog_date, md5sum=md5sum, catalog_type=Catalog.PRICES)

    # Get existing data for bulk operations
    all_cm_ids = [item['idProduct'] for item in data['priceGuides']]
    existing_cards = MTGCard.objects.filter(cm_id__in=all_cm_ids).in_bulk(field_name='cm_id')
    existing_prices = MTGCardPrice.objects.filter(catalog_date=catalog_date).values_list('cm_id', flat=True)
    existing_price_ids = set(existing_prices)
    unknown_cards = set()

    for price_item in data['priceGuides']:
        cm_id = price_item['idProduct']

        # Check if the card exists
        card = existing_cards.get(cm_id)
        if not card:
            unknown_cards.add(cm_id)
            continue

        # Skip if the price already exists for this date
        if cm_id in existing_price_ids:
            continue

        mtg_card_price = MTGCardPrice(
            catalog_date=catalog_date,
            card=card,
            cm_id=cm_id,
            avg=price_item.get('avg'),
            low=price_item.get('low'),
            trend=price_item.get('trend'),
            avg1=price_item.get('avg1'),
            avg7=price_item.get('avg7'),
            avg30=price_item.get('avg30'),
            avg_foil=price_item.get('avg-foil'),
            low_foil=price_item.get('low-foil'),
            trend_foil=price_item.get('trend-foil'),
            avg1_foil=price_item.get('avg1-foil'),
            avg7_foil=price_item.get('avg7-foil'),
            avg30_foil=price_item.get('avg30-foil'),
        )

        insert_prices.append(mtg_card_price)

    # Bulk create all prices
    if insert_prices:
        MTGCardPrice.objects.bulk_create(insert_prices, ignore_conflicts=True)
        logger.info('%d new prices inserted for %s', len(insert_prices), catalog_date.date())

    if unknown_cards:
        logger.warning('Prices with unknown cards: %d (examples: %s)', len(unknown_cards), list(unknown_cards)[:5])

    return len(insert_prices)


# Convenience functions for common use cases
def retry_recent_files(days_back=7, **kwargs):
    """Retry processing files from the last N days."""
    from_date = timezone.now() - timezone.timedelta(days=days_back)
    return update_from_local_files_with_retry(from_date=from_date, **kwargs)


def retry_all_files(**kwargs):
    """Retry processing all available files."""
    return update_from_local_files_with_retry(from_date=None, **kwargs)


def force_reprocess_from_date(from_date, **kwargs):
    """Force reprocessing of files from a specific date, ignoring previous processing."""
    kwargs['force_reprocess'] = True
    return update_from_local_files_with_retry(from_date=from_date, **kwargs)
