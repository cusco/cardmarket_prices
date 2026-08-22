# cardmarket_prices

Fetches, stores, and analyzes Magic: The Gathering card prices from
Cardmarket, to catch cards rising in price while they're still affordable to
buy. Flat Python scripts at the repo root (no package, no framework) backed by
a single DuckDB file - no Django, no Celery, no MariaDB.

## Why DuckDB, not a normal SQL database

Price history is an append-only time series (one row per card per day) queried
analytically (scan, aggregate, trend). A prior Django/SQLite version of this
project hit real scaling pain here (an 11GB+ sqlite file, slow slope
calculations) because row-oriented OLTP engines aren't built for that access
pattern. DuckDB is columnar and built for exactly this.

## Layout

- **`constants.py`** - all config: paths, URLs, price-field choice, format
  legality (`FORMATS`), spike-detection thresholds. Read this first.
- **`db.py`** - `get_connection()`, the DuckDB schema (`products`, `prices`).
- **`ingest.py`** - the entry point. Finds/fetches daily `.gz` price-guide
  snapshots, loads them, refreshes `products`. Schema is declared explicitly
  (`PRICE_GUIDE_COLUMNS`/`PRODUCT_CATALOG_COLUMNS`) rather than inferred, so a
  field absent from a given day's sampled rows can't crash the insert.
  `ingest_file()` checks a file's date from its filename against the DB before
  opening it at all - measured 16x faster (168ms -> 10ms/file) against the real
  650+-file archive, since almost every run's files are already ingested and
  used to require a full parse just to find that out. The real `createdAt`
  inside the file is still authoritative for anything actually opened, so a
  misleading filename can't corrupt data, only lose the fast path.
- **`init.py`** - first-run detection (`prices` table empty), called
  automatically from `ingest.py`. If `constants.REMOTE_CATALOGS_URL` is set
  (an Apache-style directory listing), backfills `local/catalogs` from it
  before the normal ingest - a fresh checkout doesn't need a manual step.
- **`spikes.py`** - the actual analysis, two signals so far (see below).
- **`export_gdrive.py`** - pushes `find_spikes()` results to a Google Sheet.
- **`parse_sets.py`** / **`merge_sets.py`** - build `sets.json` (id -> name/
  code/type/etc. for every set Cardmarket sells). Names come from HTML you
  paste yourself (Cardmarket's set-list page is Cloudflare-blocked from
  scripted access); code/type are a one-time export from the old Django DB.
- **`update_formats.py`** - refreshes Standard (wholesale) and Pioneer
  (append-only union) legal-set ids from whatsinstandard.com's public API,
  resolving names to Cardmarket ids via `sets.json`. Unmatched names are
  reported, never guessed.

## Data

- `local/catalogs/*.json.gz` - raw daily snapshots. Gitignored (bulk, ~GB
  scale). `REMOTE_CATALOGS_URL` + `init.py` backfill this on a fresh checkout.
- `sets.json`, `data/*.json` - small derived fixtures, **tracked in git** (no
  secrets, cheap, and it's what makes a fresh checkout usable without needing
  a Cardmarket login first).
- `price_pulse.duckdb` - the database. Gitignored, fully regenerable from
  `local/catalogs` via `ingest.py`.
- `google_secrets.json` - Google service-account credentials for
  `export_gdrive.py`. Gitignored, not present on a fresh checkout.

## Spike detection (`spikes.py`)

Two independent signals, meant to be building blocks, not a finished design:

1. **`find_spikes()`** - week-over-week trend movement. Uses DuckDB's `ASOF
   JOIN` to compare today's price against the closest available snapshot at
   least `window_days` ago, and always reports the *real* elapsed days
   alongside the % change - so a comparison spanning a data gap in history is
   visible as such instead of silently wrong (this was a real, hit-twice bug
   class in the predecessor Django project's slope table).
2. **`find_price_gaps()`** - same-day signal: how far today's cheapest listing
   (`low`) has pulled above the smoothed `trend` price, i.e. "the cheap copies
   are gone, remaining sellers are asking more." Idea ported from an even
   older project (`~/cardmarket_scraper`, pre-dates this repo, scraped
   Cardmarket's live per-card page directly). On Premodern it essentially
   never fires (`low > trend` in 0 of 502 cards checked, 1-20 EUR range) - that
   market's thin/stable enough that `low` rarely outruns `trend` at all. It
   *does* fire on Pioneer's larger, more actively-traded pool. One real bug
   found this way and fixed: `trend` was only checked `> 0`, not floored at
   `min_price` like `low` was - a card with trend=0.02 and one oddly-priced
   low=16.00 listing reported as a nonsensical "+79900%" gap. Both sides are
   floored now.
   
   Known, accepted limitation (not a bug): `low` is just the cheapest listing
   that day, with no visibility into *why* - Cardmarket's bulk price-guide
   JSON exposes no per-listing condition/finish data, so there's no way to
   tell a real market move from a one-off foil/signed listing skewing that
   day's minimum. `find_spikes()` doesn't have this problem because it
   requires a move to persist a week later; `find_price_gaps()` is same-day by
   design, which is exactly what makes it useful (catches things a day
   earlier than a trend-based signal could) and exactly what makes it
   occasionally wrong for a day. Decided not to build a persistence check for
   this: it would cancel the same-day advantage that's the whole point of the
   signal, and rare false positives here are an accepted tradeoff, not
   something to engineer around.

**Where this goes next** (not yet built): a real statistical view across
`trend`/`low`/`avg` (and their medians) together, instead of several ad hoc
single-field signals each needing its own noise-guards discovered one at a
time (see the `trend > 0` bug above - `find_spikes()` already had the
equivalent guard, `find_price_gaps()` didn't, until it broke on real data).
Two ideas worth trying before adding a fourth independent signal: (a) track
the *trajectory* of the low/trend gap over time rather than its size on one
day - a gap that's widening day over day might be meaningful even if it never
crosses an absolute threshold; (b) look at how far each field sits from the
*median* across all of them (or across recent days) rather than picking one
field as "the" baseline for another - the median is more robust to any single
field's own noise/lag characteristics than treating `trend` as ground truth.

## Formats

`constants.FORMATS["premodern"|"standard"|"pioneer"]` - sets of Cardmarket
`expansion_id`s legal in each format. Premodern is a fixed literal (closed set
list, pre-2004, never changes). Standard/Pioneer live in `data/formats.json`,
refreshed by `update_formats.py`.

## Testing

`pytest` from repo root just works (`pytest.ini` sets `pythonpath = .`).
Network calls are mocked; DB tests run against in-memory DuckDB
(`tests/conftest.py`'s `con` fixture). `scripts/test_backend.sh` runs pytest
as a CI gate; `scripts/static_validate_backend.sh` runs black/isort/
prospector/bandit/semgrep as a second gate (`old/` excluded from all of
them - it's retired, not maintained). `.github/workflows/ci.yml` runs both
and uploads `htmlcov/` plus the JUnit/coverage XML as build artifacts.

## Known gaps

- Deployment (OVH server, replacing the old systemd Celery service with cron)
  hasn't happened yet.

## `old/`

The previous Django/Celery/sqlite version of this project, retired and moved
here wholesale (`git mv`, history preserved) rather than deleted, in case
anyone still needs it. Not part of the active codebase - nothing here imports
from it, and there's no expectation it still runs.
