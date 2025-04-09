
# Cardmarket Prices

A Django project to fetch, store, and analyze Magic: The Gathering card data from Cardmarket. Can be expanded to other Cardmarket products.

Cardmarket publishes its "Product Catalogue" and "Price Guide" daily: [Cardmarket News](https://news.cardmarket.com/en/Magic/were-making-the-price-guide-and-product-catalogue-available-for-download).

## Features

- Fetches card sets, cards, and prices from Cardmarket.
- Stores historical data in a local database.
- Analyzes historical price changes.

## Setup

This project uses Django with Celery for task scheduling.

### Local Setup

Follow these steps to set up the project locally:

```bash
# Clone the repository and install dependencies
git clone git@github.com:cusco/cardmarket_prices.git
cd cardmarket_prices
pip install -r requirements.txt

# Apply migrations and load initial data
cd src
./manage.py migrate
./manage.py loaddata fixture_01_mtgset.json
./manage.py loaddata fixture_02_mtgcard.json
```

### Load Historical Data

Some data has been added to the `local/catalogs` directory. To load this data into the database:

1. Open the Django Python shell:
   ```bash
   ./manage.py shell_plus
   ```

2. Run the following command:
   ```python
   from prices.services import update_mtg, update_from_local_files
   update_mtg()
   update_from_local_files()
   ```

### Celery Automation

To automate data fetching, you can set up Celery with the following commands or as systemd services:

```bash
# Start the Celery worker
celery -A cm_prices worker -l info

# Start the Celery scheduler
celery -A cm_prices beat -l info
```

## Usage

Ensure the database contains historical data. Use `update_mtg()` to fetch the latest data daily or rely on Celery automation.

### Main Functions

1. **`show_changes(card_qs=None, days=7, min_price=3)`**  
   Displays price changes over the specified days for a card queryset.

2. **`show_stats(days=7, card_qs=None)`**  
   Provides statistical insights for a specified period.

Both functions are in `src/lib/utils.py`. By default, `card_qs` filters cards from Pioneer-legal sets, and `days` specifies the time period for analysis.

### Example Usage

```python
qs = MTGCard.objects.filter(expansion_id__in=LEGAL_STANDARD_SETS)
settings.PRICE_FIELD = 'trend'
show_changes(card_qs=qs, days=2)
```

Sample output:
```
Name                       | Expansion Code | % Change | Slope  | Price Change
----------------------------------------------------------------------------
Kiora, the Rising Tide     | FDN            |   +5.3%  |  +0.15 | 5.65 -> 5.95 (+0.30€)
Glissa Sunslayer           | ONE            |   +3.3%  |  +0.09 | 5.74 -> 5.93 (+0.19€)
...
```

Set `settings.PRICE_FIELD` to adjust the price metric (`trend`, `low`, etc.).

---

## Contributing

Pull requests are welcome, but they must pass validation. Run the following script to ensure compliance before submitting:

```bash
./scripts/static_validate_backend.sh
```
