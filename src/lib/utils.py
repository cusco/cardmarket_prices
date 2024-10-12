import logging
from datetime import datetime
from pathlib import Path

import pytz

from prices.services import update_cm_prices

logging.basicConfig(level=logging.INFO)  # temporary
logger = logging.getLogger(__name__)
germany_tz = pytz.timezone('Europe/Berlin')


def simple_trend(price_dates, price_values):
    """Calculate the trend (slope) of a card's price history using basic linear regression."""

    num_values = len(price_dates)

    # Convert datetime objects to numeric values (e.g., days since the first date)
    time_values = [(date - price_dates[0]).days for date in price_dates]

    # Calculate the sums
    sum_time = sum(time_values)
    sum_price = sum(price_values)
    sum_time_price = sum(t * p for t, p in zip(time_values, price_values))
    sum_time_squared = sum(t**2 for t in time_values)

    # Calculate slope (trend)
    numerator = num_values * sum_time_price - sum_time * sum_price
    denominator = num_values * sum_time_squared - sum_time**2
    trend_slope = numerator / denominator if denominator != 0 else 0

    return trend_slope


def trend(card, days=None):
    """Find the trend of x days of a card pricing."""

    if days:
        days_ago = datetime.datetime.now() - datetime.timedelta(days=days)
        if card.prices.filter(catalog_date__gte=days_ago).count() <= 1:
            logger.warning('error getting trend from %d days', days_ago.days)
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
    price_trend = simple_trend(price_labels, price_values)

    return price_trend


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
