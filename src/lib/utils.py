import logging
import statistics
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import timedelta

import pytz
from django.conf import settings
from django.db.models import OuterRef, Subquery
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


def update_card_slopes(card_qs=None, chunk_size=990):
    """Calculate and store slopes for a queryset of MTGCards in chunks, and returns created/updated counts."""

    if not card_qs:
        card_qs = MTGCard.objects.filter(expansion_id__in=LEGAL_STANDARD_SETS)

    cards = list(card_qs)
    created_count = 0
    deleted_count = 0
    for i in range(0, len(cards), chunk_size):
        end_index = min(i + chunk_size, len(cards))
        chunk = cards[i:end_index]
        slopes_to_create = []
        cards_ids_to_delete = []

        for card in chunk:
            cards_ids_to_delete.append(card.cm_id)
            slopes_to_create.extend(calculate_card_slopes(card))

        deleted_count += MTGCardPriceSlope.objects.filter(card__cm_id__in=cards_ids_to_delete).delete()[0]
        created_count += len(slopes_to_create)
        MTGCardPriceSlope.objects.bulk_create(slopes_to_create)

    return created_count, deleted_count


def calculate_card_slopes(card):
    """Calculate and return a list of MTGCardPriceSlope instances for a single MTGCard."""

    intervals = [2, 7, 30]
    slopes = []

    latest_price = card.prices.filter(**{f"{settings.PRICE_FIELD}__isnull": False}).order_by("-catalog_date").first()
    if not latest_price:
        return []

    earliest_date = latest_price.catalog_date + timedelta(-(max(intervals) + 2))
    earliest_date = earliest_date.replace(hour=0, minute=0, second=0, microsecond=0)

    end_date = latest_price.catalog_date.replace(hour=0, minute=0, second=0, microsecond=0)

    prices = list(
        card.prices.filter(catalog_date__gte=earliest_date, **{f"{settings.PRICE_FIELD}__isnull": False})
        .order_by("catalog_date")
        .values_list("catalog_date", settings.PRICE_FIELD)
    )

    for days in intervals:
        start_date = end_date - timedelta(days=days)

        interval_prices = [(date, price) for date, price in prices if date >= start_date]

        if len(interval_prices) < 2:
            continue

        price_dates = [item[0] for item in interval_prices]
        price_values = [item[1] for item in interval_prices]

        final_price = price_values[-1]
        initial_price = price_values[0]

        if initial_price == 0:
            percent_change = 0
        else:
            percent_change = ((final_price - initial_price) / initial_price) * 100

        slope = simple_trend(price_dates, price_values)

        slopes.append(
            MTGCardPriceSlope(
                card=card,
                interval_days=days,
                slope=slope,
                percent_change=percent_change,
                initial_price=initial_price,
                final_price=final_price,
            )
        )

    return slopes


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


# ####### GEMINI
def find_spiking_cards(
    card_qs=None,
    min_price=3,
    last_entries=3,
    min_percentage_change=5,
    accelerating_increase_factor=1.0,
    min_price_difference=0.50,
):
    """Find cards with potential price spikes based on increasing trend."""
    if not 2 <= last_entries <= 30:
        raise ValueError("last_entries must be between 2 and 30.")

    if not card_qs:
        card_qs = MTGCard.objects.filter(expansion_id__in=LEGAL_PIONEER_SETS)

    # filter cards with trend != 0
    valid_card_ids = {
        card.pk
        for card in card_qs
        if len(card.prices.order_by('-catalog_date')[:last_entries]) == last_entries
        and all(p.trend is not None for p in card.prices.order_by('-catalog_date')[:last_entries])
    }
    valid_cards_qs = card_qs.filter(pk__in=valid_card_ids)

    spiking_cards = []

    for card in valid_cards_qs:
        prices = list(card.prices.order_by('-catalog_date')[:last_entries])
        current_price = prices[0].trend
        previous_price = prices[1].trend
        earliest_price = prices[2].trend

        percentage_change = ((current_price - previous_price) / previous_price) * 100 if previous_price != 0 else 0

        if current_price > min_price:
            if percentage_change >= min_percentage_change:
                trend_increase_day1 = previous_price - earliest_price
                trend_increase_day2 = current_price - previous_price

                if trend_increase_day2 >= accelerating_increase_factor * trend_increase_day1:
                    price_difference = current_price - earliest_price

                    if price_difference > min_price_difference:
                        spiking_cards.append((card, current_price, previous_price, earliest_price, price_difference))

    spiking_cards.sort(key=lambda x: x[4], reverse=True)
    return spiking_cards


def display_spiking_cards(spiking_cards):
    """Display spiking cards with detailed information, sorted by price difference."""

    if not spiking_cards:
        print("No spiking cards found.")
        return

    print(
        f"{'Name':<40} || {'Expansion Code':<14} | {'Price_0':<8} | {'Price_1':<8} | {'Price_2':<8} | {'Price Difference':<8}"  # NOQA
    )
    print("-" * 100)

    for card, price_0, price_1, price_2, price_difference in spiking_cards:
        truncated_name = card.name[:37] + '...' if len(card.name) > 37 else card.name

        print(
            f"{truncated_name:<40} || {card.expansion.code:<14} | {price_0:+6.2f}€ | {price_1:+6.2f}€ | {price_2:+6.2f}€ | {price_difference:+6.2f}€"  # NOQA
        )


# Example usage:
# card_qs = MTGCard.objects.filter(expansion_id__in=LEGAL_STANDARD_SETS)
# spiking_cards = find_spiking_cards(card_qs, min_price=2)
# display_spiking_cards(spiking_cards)
