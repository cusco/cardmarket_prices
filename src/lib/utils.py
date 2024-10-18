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

logging.basicConfig(level=logging.INFO)  # temporary
logger = logging.getLogger(__name__)
germany_tz = pytz.timezone('Europe/Berlin')


def simple_trend(price_dates, price_values):
    """Calculate the rate of price change (slope) over time using basic linear regression."""

    # Number of data points (dates and prices)
    num_values = len(price_dates)

    # Convert dates into numeric time values (days since the first date)
    time_values = [(date - price_dates[0]).days for date in price_dates]

    # Calculate necessary sums for the linear regression formula
    sum_time = sum(time_values)  # Sum of time values (e.g., total days)
    sum_price = sum(price_values)  # Sum of price values
    sum_time_price = sum(t * p for t, p in zip(time_values, price_values))  # Sum of (time * price)
    sum_time_squared = sum(t**2 for t in time_values)  # Sum of squared time values

    # Calculate the numerator and denominator for the slope (linear regression formula)
    numerator = num_values * sum_time_price - sum_time * sum_price
    denominator = num_values * sum_time_squared - sum_time**2

    # Calculate the slope of the trend (rate of price change)
    trend_slope = numerator / denominator if denominator != 0 else 0

    return trend_slope


def price_slope(card, days=None):
    """Find the trend of x days of a card pricing."""

    if days:
        days_ago = timezone.now() - timedelta(days=days)
        if card.prices.filter(catalog_date__gte=days_ago).count() <= 1:
            logger.warning('error getting trend from %d days', days)
            return 0
        prices = card.prices.filter(catalog_date__gte=days_ago)
    else:
        prices = card.prices.all()
    prices = prices.exclude(trend__isnull=True)
    if prices.count() <= 1:
        return 0
    price_labels = list(prices.values_list('catalog_date', flat=True))
    price_values = list(prices.values_list('trend', flat=True))
    price_values = [0 if val is None else val for val in price_values]  # replace None with 0

    # price_labels = list(self.charted_prices.values_list('price_date', flat=True))[-days:]
    # price_values = list(self.charted_prices.values_list('price_value', flat=True))[-days:]

    # x_values = np.linspace(0, 1, len(price_labels))
    # y_values = [float(x) for x in price_values]
    # price_trend = np.polyfit(x_values, y_values, 1)[-2]
    slope = simple_trend(price_labels, price_values)

    return slope


def price_increase_ranking(card, price='trend'):
    """Calculate how much the card's price has risen, returns the percentage increase."""

    # Get the prices for the card, ordered by date and exclude None values for trend.
    prices = list(
        card.prices.filter(**{f"{price}__isnull": False}).order_by('catalog_date').values_list('catalog_date', price)
    )

    # If there are less than 2 price points, return 0 (no ranking possible).
    if len(prices) < 2:
        return 0

    first_price = prices[0][1]
    last_price = prices[len(prices) - 1][1]

    # Ensure the price has risen at least once, return 0 otherwise.
    if first_price >= last_price:
        return 0

    # Check if the price is rising or staying the same every day
    for i in range(1, len(prices)):
        if prices[i][1] < prices[i - 1][1]:  # Price dropped at some point
            return 0

    # Calculate the percentage increase.
    percentage_increase = ((last_price - first_price) / first_price) * 100

    return percentage_increase


def update_from_local_files():
    """Update prices from local json files."""

    directory = Path("../local/catalogs")
    files = sorted(directory.glob("202*json"), key=lambda f: f.name)
    for json in files:
        # noset
        with open(json, "r", encoding='utf-8') as file:
            # content = json.load(f)
            content = file.read()
            try:
                update_cm_prices(local_content=content)
            except Exception as err:  # NOQA
                print(err)


def rank_card_by_price(card):
    """Process a card to determine its mean price increase and add it to the results."""

    prices = ['avg', 'avg1', 'low', 'trend']
    min_value = 1
    min_percentage = 1

    last_price = card.prices.order_by('-catalog_date').first()
    if not last_price:
        return None, None

    if last_price.trend and last_price.trend >= min_value:
        increase_list = []
        for price in prices:
            increase = price_increase_ranking(card, price)
            if not increase or increase < min_percentage:
                return None, None
            increase_list.append(increase)

        increase_mean = statistics.mean(increase_list)

        return card.pk, increase_mean
    return None, None


def show_stats():
    """Show statistics for mtg cards regarding latest price changes."""

    t2_cards = MTGCard.objects.filter(expansion_id__in=LEGAL_STANDARD_SETS)

    print('=============== Always Rising')
    always_rising = {}
    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(rank_card_by_price, card) for card in t2_cards]
        for future in as_completed(futures):
            card_id, increase = future.result()
            if card_id and increase:
                always_rising[card_id] = increase

    for card_id, increase in sorted(always_rising.items(), key=lambda x: x[1]):
        card = MTGCard.objects.get(pk=card_id)
        print(f'{increase} | {card}')
