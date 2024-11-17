import logging
import statistics
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import timedelta

import pytz
from django.conf import settings
from django.db.models import F, OuterRef, Subquery
from django.utils import timezone

from prices.constants import LEGAL_PIONEER_SETS, LEGAL_STANDARD_SETS
from prices.models import MTGCard, MTGCardPrice, MTGCardPriceSlope

logger = logging.getLogger(__name__)
germany_tz = pytz.timezone('Europe/Berlin')
MIN_PRICE_VALUE = 1
MIN_PERCENTAGE = 1


def show_stats(days=7, cards_qs=None):
    """Show statistics for MTG cards regarding latest price changes over a specified period."""
    if not cards_qs:
        cards_qs = MTGCard.objects.filter(expansion_id__in=LEGAL_STANDARD_SETS)

    always_rising = {}
    trending_cards = {}
    logger.info('Processing stats for %d cards', cards_qs.count())

    # parallelism
    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(rank_card_by_price, card, days): ('rising', card.pk) for card in cards_qs}
        futures.update({executor.submit(price_slope, card, days): ('trending', card.pk) for card in cards_qs})

        for future in as_completed(futures):
            task_type, card_id = futures[future]
            result = future.result()
            if task_type == 'rising' and result:
                always_rising[card_id] = result
            elif task_type == 'trending' and result >= settings.SLOPE_THRESHOLD:
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

    # Convert dates to time intervals in days
    base_date = price_dates[0]
    time_values = [(date - base_date).total_seconds() / 86400 for date in price_dates]

    # Precompute sums to avoid multiple passes
    sum_time = sum(time_values)
    sum_price = sum(price_values)
    sum_time_price = sum(t * p for t, p in zip(time_values, price_values))
    sum_time_squared = sum(t * t for t in time_values)

    # Calculate slope
    numerator = num_values * sum_time_price - sum_time * sum_price
    denominator = num_values * sum_time_squared - sum_time * sum_time
    return numerator / denominator if denominator != 0 else 0


def price_slope(card, days=None):
    """Calculate trending slope for card prices over a period."""
    prices = fetch_prices(card, settings.PRICE_FIELD, days)
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

    # ############# quick hack, 1 entry per day, lets count entries
    if days:
        return list(
            card.prices.filter(**{f"{field}__isnull": False})
            .order_by('-catalog_date')
            .values_list('catalog_date', field)[:days:-1]
        )

    return list(
        card.prices.filter(**{f"{field}__isnull": False}).order_by('catalog_date').values_list('catalog_date', field)
    )

    # if days:
    #     days_ago = (timezone.now() - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)
    #     return list(
    #         card.prices.filter(catalog_date__gte=days_ago, **{f"{field}__isnull": False})
    #         .order_by('catalog_date')
    #         .values_list('catalog_date', field)
    #     )
    # return list(
    #     card.prices.filter(**{f"{field}__isnull": False}).order_by('catalog_date').values_list('catalog_date', field)
    # )


def rank_card_by_price(card, days=None):
    """Calculate the mean percentage increase across multiple price metrics for a card over a period."""

    price_field = settings.PRICE_FIELD
    price_fields = ['avg', 'avg1', 'low', 'trend']
    min_value = 1  # Minimum threshold for last price to be considered significant
    min_percentage = 1  # Minimum threshold for percentage increase

    # Get the latest price to check if it meets the minimum threshold
    last_prices = (
        card.prices.filter(catalog_date__gte=timezone.now() - timedelta(days=days)).order_by('-catalog_date').first()
    )

    # Skip card if the last low price is below min_value or missing
    if not last_prices or (getattr(last_prices, price_field) and getattr(last_prices, price_field) < min_value):
        return 0

    # List to hold percentage increases for each price field
    increase_list = []
    for p_field in price_fields:
        increase = price_increase_ranking(card, p_field, days)
        if increase < min_percentage:
            return 0  # Discard if any increase is below threshold
        increase_list.append(increase)

    # Return the mean of the increases if all price fields meet the threshold
    return statistics.mean(increase_list) if increase_list else 0


def build_price_data(card_qs, field):
    """Efficiently build price data using a bulk query."""
    price_data = defaultdict(list)

    # Fetch all relevant prices in a single query
    all_prices = (
        MTGCardPrice.objects.filter(card__in=card_qs, **{f"{field}__isnull": False})
        .annotate(card_cm_id=F('card__cm_id'))
        .order_by('card_cm_id', '-catalog_date')
    )

    # Process results in a single pass
    for price in all_prices:
        card_id = price.card_cm_id
        price_data[card_id].append((price.catalog_date, getattr(price, field)))

    return price_data


def update_card_slopes(card_qs=None):
    """Update slopes and percentage changes for all cards based on recent prices."""
    intervals = [2, 7, 30]
    p_field = settings.PRICE_FIELD  # The price field to use for slope calculations
    slopes_to_update = []
    slopes_to_create = []
    current_time = timezone.now()

    if not card_qs:
        card_qs = MTGCard.objects.filter(expansion_id__in=LEGAL_STANDARD_SETS)

    price_data = build_price_data(card_qs, p_field)

    # Retrieve all slopes at once to minimize queries
    existing_slopes = MTGCardPriceSlope.objects.filter(card__in=card_qs).only(
        'card', 'interval_days', 'slope', 'percent_change', 'date_updated'
    )
    slope_dict = {(slope.card.cm_id, slope.interval_days): slope for slope in existing_slopes}

    for card in card_qs:
        card_prices = price_data.get(card.cm_id, [])

        for days in intervals:
            # Filter the recent prices to match the required interval
            recent_prices = card_prices[:days][::-1]
            if len(recent_prices) < 2:
                continue

            # Extract dates and values
            price_dates, price_values = zip(*recent_prices)

            # Calculate the slope and percentage change
            slope = simple_trend(price_dates, price_values)
            initial_price = price_values[0]
            final_price = price_values[-1]
            percent_change = (final_price - initial_price) / initial_price * 100 if initial_price != 0 else 0

            slope_key = (card.cm_id, days)
            if slope_key in slope_dict:  # Update
                slope_instance = slope_dict[slope_key]
                slope_instance.slope = slope
                slope_instance.percent_change = percent_change
                slope_instance.date_updated = current_time
                slopes_to_update.append(slope_instance)
            else:  # Create
                slopes_to_create.append(
                    MTGCardPriceSlope(card=card, interval_days=days, slope=slope, percent_change=percent_change)
                )

    # Perform bulk operations to save database trips
    if slopes_to_create:
        MTGCardPriceSlope.objects.bulk_create(slopes_to_create)
    if slopes_to_update:
        MTGCardPriceSlope.objects.bulk_update(slopes_to_update, ['slope', 'percent_change', 'date_updated'])

    return len(slopes_to_create), len(slopes_to_update)


def get_top_20_cards_by_slope(card_qs, min_price=3, interval_days=7, only_positive=True):
    """Return up to the top 20 cards with the highest slopes, filtering only positive changes if specified."""

    p_field = settings.PRICE_FIELD
    # Fetch latest price for filtering cards based on min_price
    latest_price = MTGCardPrice.objects.filter(card=OuterRef('pk')).order_by('-catalog_date').values(p_field)[:1]
    annotated_qs = card_qs.annotate(latest_price=Subquery(latest_price))
    filtered_qs = annotated_qs.filter(latest_price__gte=min_price)

    # Retrieve pre-calculated slopes and get a larger initial set
    slopes = MTGCardPriceSlope.objects.filter(card__in=filtered_qs, interval_days=interval_days).order_by(
        '-percent_change'
    )[:50]

    top_cards = []
    for slope in slopes:
        interval_prices = list(slope.card.prices.order_by('-catalog_date')[:interval_days])
        if len(interval_prices) < 2:
            continue

        first_price = getattr(interval_prices[-1], p_field)
        last_price = getattr(interval_prices[0], p_field)

        if first_price is None or last_price is None:
            continue  # Skip if prices are missing

        price_change = last_price - first_price
        if first_price == last_price:
            percent_change = 0
            slope_value = 0
        else:
            percent_change = ((last_price - first_price) / first_price) * 100 if first_price != 0 else 0
            slope_value = (last_price - first_price) / interval_days

        # Apply positive filtering if specified
        if only_positive and percent_change <= 0:
            continue

        top_cards.append(
            (
                slope.card.name,
                round(percent_change, 1),
                slope_value,
                first_price,
                last_price,
                price_change,
                slope.card.expansion.code,
            )
        )

        if len(top_cards) == 20:  # Stop once we have the top 20
            break

    return top_cards


def show_changes(card_qs=None, days=7, min_price=3):
    """Display the top 20 cards based on slope and percentage change."""
    if not card_qs:
        card_qs = MTGCard.objects.filter(expansion_id__in=LEGAL_PIONEER_SETS)
        card_qs = card_qs.exclude(expansion__code__startswith='X')

    top_20_cards = get_top_20_cards_by_slope(card_qs, min_price, days)

    # Ensure the results are sorted by percent_change (index 1 in the tuple)
    top_20_cards = sorted(top_20_cards, key=lambda x: x[1], reverse=True)

    print(f"{'Name':<40} | {'Expansion Code':<14} | {'% Change':<8} | {'Slope':<6} | Price Change")
    print("-" * 90)

    for name, percent_change, slope, first_price, last_price, price_change, code in top_20_cards:
        truncated_name = name[:37] + '...' if len(name) > 37 else name
        print(
            f"{truncated_name:<40} | {code:<14} | {percent_change:+6.1f}% | {slope:+6.2f} | "
            f"{first_price:.2f} -> {last_price:.2f} ({price_change:+.2f}€)"
        )
