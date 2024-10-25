import gzip
import io
import logging
import statistics
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import timedelta
from pathlib import Path

import pytz
from django.utils import timezone

from prices.constants import LEGAL_STANDARD_SETS
from prices.models import MTGCard
from prices.services import update_cm_prices

logger = logging.getLogger(__name__)
germany_tz = pytz.timezone('Europe/Berlin')
SLOPE_THRESHOLD = 0.4  # Move magic values to named constants
MIN_PRICE_VALUE = 1
MIN_PERCENTAGE = 1


def show_stats(days=7, cards_qs=None):
    """Show statistics for MTG cards regarding latest price changes over a specified period."""
    if not cards_qs:
        cards_qs = MTGCard.objects.filter(expansion_id__in=LEGAL_STANDARD_SETS)

    always_rising = {}
    trending_cards = {}
    logger.info('Processing stats for %d cards', cards_qs.count())

    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(rank_card_by_price, card, days): ('rising', card.pk) for card in cards_qs}
        futures.update({executor.submit(price_slope, card, days): ('trending', card.pk) for card in cards_qs})

        for future in as_completed(futures):
            task_type, card_id = futures[future]
            result = future.result()
            if task_type == 'rising' and result:
                always_rising[card_id] = result
            elif task_type == 'trending' and result >= SLOPE_THRESHOLD:
                trending_cards[card_id] = result

    logger.info('Always Rising:')
    log_sorted_cards(always_rising, "price increase")

    logger.info('Trending Cards:')
    log_sorted_cards(trending_cards, "slope")


def log_sorted_cards(card_dict, label):
    """Reusable function to log sorted cards by specified label."""
    for card_id, value in sorted(card_dict.items(), key=lambda x: x[1], reverse=True):
        card = MTGCard.objects.only("name").get(pk=card_id)
        logger.info('%.2f | %s (%s)', value, card, label)


def simple_trend(price_dates, price_values):
    """Calculate the rate of price change (slope) over time using basic linear regression."""
    num_values = len(price_dates)
    time_values = [(date - price_dates[0]).days for date in price_dates]
    sum_time = sum(time_values)
    sum_price = sum(price_values)
    sum_time_price = sum(t * p for t, p in zip(time_values, price_values))
    sum_time_squared = sum(t**2 for t in time_values)
    numerator = num_values * sum_time_price - sum_time * sum_price
    denominator = num_values * sum_time_squared - sum_time**2
    return numerator / denominator if denominator != 0 else 0


def price_slope(card, days=None):
    """Calculate trend slope for card prices over a period."""
    prices = fetch_prices(card, "trend", days)
    if len(prices) <= 1:
        return 0

    price_dates, price_values = zip(*prices)
    return simple_trend(price_dates, price_values)


def price_increase_ranking(card, price_field, days=None):
    """Calculate percentage increase for a specified price field over a period."""
    prices = fetch_prices(card, price_field, days)
    if len(prices) < 2 or prices[0][1] >= prices[-1][1]:
        return 0

    for i in range(1, len(prices)):
        if prices[i][1] < prices[i - 1][1]:
            return 0

    return ((prices[-1][1] - prices[0][1]) / prices[0][1]) * 100


def fetch_prices(card, field, days):
    """Fetch filtered prices for a specific field and days."""
    if days:
        days_ago = timezone.now() - timedelta(days=days)
        return list(
            card.prices.filter(catalog_date__gte=days_ago, **{f"{field}__isnull": False})
            .order_by('catalog_date')
            .values_list('catalog_date', field)
        )
    return list(
        card.prices.filter(**{f"{field}__isnull": False}).order_by('catalog_date').values_list('catalog_date', field)
    )


def update_from_local_files():
    """Update prices from local JSON files compressed in .gz."""
    directory = Path("../local/catalogs")
    catalog_files = sorted(directory.glob("202*json.gz"), key=lambda f: f.name)
    for catalog_file in catalog_files:
        try:
            with gzip.open(catalog_file, "rb") as gz_file:
                content = io.TextIOWrapper(gz_file, encoding='utf-8').read()
            update_cm_prices(local_content=content)
        except (OSError, IOError, ValueError) as err:
            logger.error("Failed to update from %s: %s", catalog_file, err)


def rank_card_by_price(card, days=None):
    """Calculate the mean percentage increase across multiple price metrics for a card over a period."""

    price_fields = ['avg', 'avg1', 'low', 'trend']
    min_value = 1  # Minimum threshold for last price to be considered significant
    min_percentage = 1  # Minimum threshold for percentage increase

    # Get the latest price to check if it meets the minimum threshold
    last_prices = (
        card.prices.filter(catalog_date__gte=timezone.now() - timedelta(days=days)).order_by('-catalog_date').first()
    )

    # Skip card if the last trend price is below min_value or missing
    if not last_prices or (last_prices.trend and last_prices.trend < min_value):
        return 0

    # List to hold percentage increases for each price field
    increase_list = []
    for price_field in price_fields:
        increase = price_increase_ranking(card, price_field, days)
        if increase < min_percentage:
            return 0  # Discard if any increase is below threshold
        increase_list.append(increase)

    # Return the mean of the increases if all price fields meet the threshold
    return statistics.mean(increase_list) if increase_list else 0
