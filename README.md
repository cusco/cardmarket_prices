# cardmarket_prices

Fetch, store, and analyze Magic: The Gathering card prices from Cardmarket.
No Django, no Celery, no MariaDB - just a handful of small scripts and a
DuckDB file.

This project replaced an earlier Django-based version (moved to `old/`, kept
around in case anyone still needs it) once its price history table grew large
enough (tens of millions of rows) that SQLite/MariaDB - row-oriented engines
built for transactional reads/writes - started struggling with what's actually
an analytical, append-only time-series workload. DuckDB is a columnar engine
built for exactly that: scan a pile of daily snapshots, aggregate a couple of
numeric columns, done.

## What it does

- **`ingest.py`** - finds the local `.gz` price-guide snapshots already sitting
  in `local/catalogs` (or fetches today's from Cardmarket and archives it
  there in the same naming scheme), and loads whichever ones aren't in the
  database yet. Also refreshes the current product catalog (card name/set
  mapping) - that's a snapshot, not history, so it's just overwritten each run.
- **`spikes.py`** - finds cards rising in price while still affordable. Uses
  DuckDB's `ASOF JOIN` to compare today's price against the closest available
  snapshot at least `window_days` ago, and reports the *actual* elapsed days
  alongside the % change - so a comparison spanning a data gap is visible as
  such, rather than silently wrong (see "Why not the old slope logic?" below).
- **`export_gdrive.py`** - pushes the current spikes table to a Google Sheet,
  in its own `price_pulse_spikes` tab.
- **`parse_sets.py`** / **`merge_sets.py`** - build `sets.json`, the
  `{expansion_id: {name, code, type, is_foil_only, release_date}}` metadata for
  every set Cardmarket sells (~750+, not just Premodern). See "Set metadata"
  below - this isn't scope you need to touch day to day, only when refreshing
  the set list.
- **`update_formats.py`** - refreshes Standard (and, additively, Pioneer)
  legal-set ids from whatsinstandard.com. See "Formats" below.
- **`init.py`** - first-time setup, run automatically by `ingest.py`. See
  "First-time setup" below.

## Setup

```bash
pip install -r requirements.txt
```

Needs a `google_secrets.json` Google service-account credentials file at the
repo root (see `constants.py`) to run `export_gdrive.py`.

## First-time setup

A fresh checkout doesn't need a separate init command - just run:

```bash
python ingest.py
```

`ingest.py` detects an empty database (`prices` table has zero rows) and, if
`constants.REMOTE_CATALOGS_URL` is set to an Apache-style directory listing of
historical `.gz` snapshots (e.g. an S3/OVH bucket with autoindex on),
backfills `local/catalogs` from it before doing the normal ingest - so you get
real history on day one instead of starting from a single day's snapshot.
Leave `REMOTE_CATALOGS_URL` empty to skip that step and just start
accumulating history from today forward.

`sets.json`/`data/formats.json` ship committed in the repo (they're small,
derived reference data, not secrets) - a fresh checkout already has current
set names/codes/format legality without needing to run `parse_sets.py`/
`merge_sets.py`/`update_formats.py` first.

## Usage

```bash
python ingest.py           # fetch + load new price snapshots, refresh products
python spikes.py           # print the current top spikes to stdout
python export_gdrive.py    # push spikes to Google Sheets
```

Or from a Python shell:

```python
from db import get_connection
from spikes import find_spikes, print_spikes

con = get_connection()
df = find_spikes(con, window_days=7, min_price=1, max_price=20)
print_spikes(df)
```

## Why not the old slope logic?

The old Django project's `MTGCardPriceSlope` table pre-computes 2/7/30-day slopes,
but only produces a row when a card has >=2 price points *inside* that fixed
calendar window - if there's a gap in history bigger than the window, it
silently produces nothing. Its fallback path (`fetch_prices` in `lib/utils.py`)
counts the last N price *rows*, not the last N *days*, so a gap quietly mixes a
recent price with one from months earlier without saying so.

`find_spikes()` here sidesteps both problems with an as-of join: for each card,
it finds whatever price point is closest to (but not after) `window_days` ago,
and always reports the real `elapsed_days` for that comparison. A stale
comparison is still visible in the output instead of being either silently
dropped or silently wrong.

## Set metadata

Cardmarket's set list isn't in any of its official downloadable JSON (the
product catalog only has numeric `idExpansion`, no name) and the HTML pages
that used to have it are now behind a Cloudflare challenge that blocks scripted
access - so this isn't fetched automatically. Instead:

1. Log into cardmarket.com yourself, go to any Magic Singles search page, and
   copy the `<select id="idExpansion...">` element's HTML (view-source or
   devtools). Save it over `data/cardmarket_sets.html`.
2. `python parse_sets.py` - parses that HTML into `data/set_names.json`
   (id -> name, every set Cardmarket currently sells).
3. `python merge_sets.py` - folds in `code`/`type`/`is_foil_only`/`release_date`
   from `data/cardmarket_prices_mtgset_dump.json`, a one-time export of the old
   Django project's `MTGSet` table (it already had ~93% of sets' codes and about
   half their `type` backfilled from past manual runs of `get_set_code()` and an
   mtgjson.com matcher) - and writes the final `sets.json` that `constants.py`
   loads.

`type` uses Scryfall/MTGJSON's vocabulary (`expansion`, `promo`, `core`,
`masters`, `commander`, `memorabilia`, ...) - useful as an "is this a real set"
signal, though coverage is partial and no set in the current data is actually
tagged `token`, so it can't yet cleanly filter out token-only sets.

Re-run steps 1-3 whenever you want to pick up new sets Cardmarket has added.

## Formats

`constants.FORMATS` maps a format name to the set of expansion_ids legal in
it: `"premodern"`, `"standard"`, `"pioneer"`. `find_spikes(con,
format_name="pioneer")` (or `format_name=None` for every set) is already
wired to use it.

- `premodern` is a plain Python literal in `constants.py` - closed, decades-old
  set list, never needs updating.
- `standard`/`pioneer` live in `data/formats.json`, refreshed by
  `python update_formats.py` (pulls from whatsinstandard.com's public API,
  resolves names to Cardmarket ids via `sets.json`). Standard is rewritten
  wholesale each run; Pioneer only ever grows (union with its previous
  contents), since it's non-rotating. Anything it can't resolve to a
  Cardmarket id gets reported, not guessed - see that script's docstring.

## Schema

Two DuckDB tables in `price_pulse.duckdb`:

- `products(cm_id PK, name, expansion_id, metacard_id, category_id)` - current
  snapshot, replaced wholesale on each `refresh_products()` run.
- `prices(cm_id, catalog_date, avg, low, trend, avg1, avg7, avg30, *_foil...)`
  - append-only, `PRIMARY KEY (cm_id, catalog_date)`, one row per card per day.

## Known gaps / things to revisit

- No scheduling built in yet - run `ingest.py` (and `update_formats.py`
  periodically) via cron to keep history gap-free and Standard current.
- `ingest.py` re-checks every local `.gz` file's date against the database on
  every run (even ones it's already ingested), which gets slow as
  `local/catalogs` grows into the hundreds of files. Fine for now; worth a
  faster "have we seen this filename before" check if it becomes annoying.
