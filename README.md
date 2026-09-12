# cardmarket_prices

Fetches, stores, and analyzes Magic: The Gathering card price history from Cardmarket, looking for cards worth
buying before they rise or selling while they're still up. Flat Python scripts backed by a single DuckDB file - no
framework, no external database server.

## What it does

- **`ingest.py`** - fetches the daily Cardmarket price-guide snapshot, archives it locally, and loads whichever
  snapshots aren't in the database yet. Also refreshes the current product catalog (card name/set mapping).
- **`spikes.py`** - two price-movement signals: week-over-week trend change, and same-day divergence between a
  card's cheapest listing and its smoothed price.
- **`export_premodern_bulk.py`** - tracks Premodern's priciest "cheapest available printing" cards and their recent
  price history.
- **`export_signal_comparison.py`** - compares several trend-detection methods (rate of change, linear regression,
  MACD) across multiple time windows for the same cards, and flags each one as an early riser, a card that's likely
  peaked, or neither.
- **`parse_sets.py`** / **`merge_sets.py`** - build the set metadata (name/code/type) used everywhere else.
- **`update_formats.py`** - refreshes which sets are currently legal in Standard and Pioneer.
- **`daily.py`** - the daily entry point: ingest, then push results to Google Sheets if anything new came in.

Results are pushed to a shared Google Sheet, one tab per export.

## Setup

```bash
pip install -r requirements.txt
```

A `google_secrets.json` Google service-account credentials file at the repo root is needed to push to Google
Sheets (see `constants.py`).

## First run

```bash
python ingest.py
```

A fresh checkout doesn't need a separate init step - `ingest.py` detects an empty database and, if
`constants.REMOTE_CATALOGS_URL` is set to a directory listing of historical snapshots, backfills from there first.
Set metadata (`sets.json`, `data/formats.json`) ships committed, so a fresh checkout works immediately.

## Usage

```bash
python daily.py                      # ingest + push everything that has new data
python ingest.py                     # just fetch/load new price snapshots
python spikes.py                     # print current spikes to stdout
python export_premodern_bulk.py      # push the Premodern bulk export
python export_signal_comparison.py   # push the trend-signal comparison
```

## Formats

`constants.FORMATS` maps a format name (`"premodern"`, `"standard"`, `"pioneer"`) to its legal `expansion_id`s.
Premodern is a fixed list; Standard/Pioneer live in `data/formats.json`, refreshed by `update_formats.py`.

## Schema

Two tables in `price_pulse.duckdb`:

- `products(cm_id PK, name, expansion_id, metacard_id, category_id)` - current snapshot, replaced each run.
- `prices(cm_id, catalog_date, avg, low, trend, avg1, avg7, avg30, *_foil...)` - append-only, one row per card per
  day.

## Testing

```bash
pytest                              # test suite
./scripts/static_validate_backend.sh  # ruff format + lint
```
