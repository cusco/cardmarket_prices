"""Static configuration for price_pulse.

Set metadata comes from sets.json - {expansion_id: {name, code, type,
is_foil_only, release_date}} for every set Cardmarket sells (~750+), not just
Premodern. Built by two scripts, in order:

  1. parse_sets.py - names for every current set, parsed from data/cardmarket_sets.html,
     HTML you copy yourself out of your own logged-in browser session (Cardmarket's
     dropdown is behind a Cloudflare challenge, so this can't be fetched by script).
  2. merge_sets.py - folds in code/type/is_foil_only/release_date from a one-time
     export of the old Django project's MTGSet table (data/cardmarket_prices_mtgset_dump.json),
     which already had most of this backfilled historically.

`type` is Scryfall/MTGJSON's set-type vocabulary ("expansion", "promo", "token",
"memorabilia", "masters", ...) - that's the "is this a real set or tokens/junk"
flag. Coverage isn't 100% (roughly 93% have a code, ~half have a type) since it
depends on what the old project happened to backfill; missing fields are None.

Format legality (which sets count as "Premodern", "Pioneer", etc.) isn't in any
of this - Cardmarket sells everything from every set regardless of tournament
legality. Premodern is a closed, permanently-fixed set list, so it's a plain
Python literal below. Standard and Pioneer rotate/grow over time, so those live
in data/formats.json instead, kept current by update_formats.py (pulls from
whatsinstandard.com's public API) rather than hand-edited here - see that
script's docstring for how id resolution and its limits work.
"""

import json
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent

SETS_JSON_PATH = PROJECT_DIR / "sets.json"

# {expansion_id: {name, code, type, is_foil_only, release_date}} - see module docstring.
SETS: dict[int, dict] = (
    {int(k): v for k, v in json.loads(SETS_JSON_PATH.read_text(encoding="utf-8")).items()}
    if SETS_JSON_PATH.exists()
    else {}
)

# {expansion_id: name}, convenience view for display code that only needs the name.
SET_NAMES: dict[int, str] = {eid: meta["name"] for eid, meta in SETS.items() if meta.get("name")}

# Where daily price-guide snapshots (.json.gz) are read from / saved to.
CATALOGS_DIR = PROJECT_DIR / "local" / "catalogs"

# Optional Apache-style directory listing of historical .gz snapshots (e.g. an
# S3/OVH bucket with autoindex on). When set, a fresh checkout with an empty
# database backfills from here on its first ingest.py run instead of starting
# from today with no history - see init.py. Leave empty to skip that step.
REMOTE_CATALOGS_URL = "https://sirius.tretas.eu/~cusco/catalogs/"

DB_PATH = PROJECT_DIR / "price_pulse.duckdb"

PRICE_GUIDE_URL = "https://downloads.s3.cardmarket.com/productCatalog/priceGuide/price_guide_1.json"
PRODUCT_CATALOG_URL = "https://downloads.s3.cardmarket.com/productCatalog/productList/products_singles_1.json"

# DuckDB's JSON reader caps object size at 16MB by default; the price guide and
# product catalog are both well over that once decompressed.
MAX_JSON_OBJECT_SIZE = 200_000_000

PRICE_FIELD = "trend"  # avg | avg1 | avg7 | avg30 | low | trend

# Closed, permanently-fixed set list (all pre-2004) - never needs updating.
PREMODERN_SET_IDS = {
    10,
    11,
    12,
    14,
    15,
    16,
    17,
    18,
    19,
    20,
    21,
    23,
    26,
    27,
    28,
    29,
    31,
    32,
    33,
    34,
    35,
    36,
    37,
    38,
    39,
    40,
    41,
    42,
    43,
}

FORMATS_JSON_PATH = PROJECT_DIR / "data" / "formats.json"
_formats_data = json.loads(FORMATS_JSON_PATH.read_text(encoding="utf-8")) if FORMATS_JSON_PATH.exists() else {}

# Rotating/growing formats, refreshed by update_formats.py - see data/formats.json.
STANDARD_SET_IDS: set[int] = set(_formats_data.get("standard", []))
PIONEER_SET_IDS: set[int] = set(_formats_data.get("pioneer", []))

# format name -> its legal expansion_ids. Add more here as their own curated id
# sets - everything else (naming, spike detection, export) is already
# format-agnostic.
FORMATS: dict[str, set[int]] = {
    "premodern": PREMODERN_SET_IDS,
    "standard": STANDARD_SET_IDS,
    "pioneer": PIONEER_SET_IDS,
}

# ---- Google Sheets export ----
GOOGLE_SECRET_CREDENTIALS = PROJECT_DIR / "google_secrets.json"

# Reuses the same spreadsheet as the original project but writes to its own
# worksheet tabs, so it never clobbers the existing "premodern_bulk"/"status" tabs.
SPREADSHEET_ID = "1vQs3vlXHu7BELFoVuK4ysfzMeYHDxMdOWgPAGmPVZjk"
SPIKES_WORKSHEET = "price_pulse_spikes"
STATUS_WORKSHEET = "price_pulse_status"

# ---- spike detection defaults ----
DEFAULT_MIN_PRICE = 1.0  # floor, avoids bulk-card % noise
DEFAULT_MAX_PRICE = 20.0  # ceiling, "still affordable"
DEFAULT_WINDOW_DAYS = 7
DEFAULT_MIN_PCT = 5.0

# ---- price-gap (low vs trend divergence) defaults - see find_price_gaps() ----
DEFAULT_MIN_GAP_PCT = 10.0  # low must sit at least this far above trend, in %
DEFAULT_MIN_GAP_ABS = 0.20  # ...and by at least this many euros (filters cent-level noise)
