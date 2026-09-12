"""Ingestion: fetch/find Cardmarket price-guide snapshots and load them into DuckDB.

Two ways data gets in:
  - fetch_latest_catalog(): downloads today's price guide and archives it as a new
    dated .gz file in CATALOGS_DIR, mirroring the naming scheme the original
    project's fetch_catalog.sh already used.
  - find_local_catalog_files(): scans CATALOGS_DIR for whatever .gz snapshots
    already exist (fetched live or dropped in manually).

Either way, every snapshot flows through the same ingest_file() parser, so there's only one code path that
understands the JSON shape.
"""

import gzip
import hashlib
import logging
import re
import tempfile
from datetime import date
from pathlib import Path

import duckdb
import requests

from constants import (
    CATALOGS_DIR,
    MAX_JSON_OBJECT_SIZE,
    PRICE_GUIDE_URL,
    PRODUCT_CATALOG_URL,
)
from db import get_connection
from init import run_if_first_time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Explicit schemas for read_json(), rather than read_json_auto()'s sample-based inference: a field absent from every
# row DuckDB happens to sample (e.g. a day where no product has foil pricing data) would otherwise make the
# inferred struct type lack that key entirely, and querying it would raise a BinderException instead of just
# returning NULL. Declaring the schema up front means a missing field is always NULL, never a schema mismatch.
PRICE_GUIDE_COLUMNS = {
    "version": "BIGINT",
    "createdAt": "VARCHAR",
    "priceGuides": (
        "STRUCT(idProduct BIGINT, avg DOUBLE, low DOUBLE, trend DOUBLE, "
        "avg1 DOUBLE, avg7 DOUBLE, avg30 DOUBLE, "
        '"avg-foil" DOUBLE, "low-foil" DOUBLE, "trend-foil" DOUBLE, '
        '"avg1-foil" DOUBLE, "avg7-foil" DOUBLE, "avg30-foil" DOUBLE)[]'
    ),
}

PRODUCT_CATALOG_COLUMNS = {
    "createdAt": "VARCHAR",
    "products": (
        "STRUCT(idProduct BIGINT, name VARCHAR, idCategory BIGINT, "
        "idExpansion BIGINT, idMetacard BIGINT, dateAdded VARCHAR)[]"
    ),
}

# Every snapshot filename starts with its date (fetch_latest_catalog() writes them that way, and every existing
# local/remote file follows the same convention), e.g. "2026-08-22_<hash>_price_guide_1.json.gz".
FILENAME_DATE_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})_")


def _date_from_filename(path: Path) -> date | None:
    """Best-effort catalog_date guess from the filename alone, no file I/O."""

    match = FILENAME_DATE_RE.match(path.name)
    return date.fromisoformat(match.group(1)) if match else None


def fetch_latest_catalog(catalogs_dir: Path = CATALOGS_DIR) -> Path | None:
    """Download today's price guide and save it as a new local .gz snapshot.

    Returns the new file's path, or None if this exact snapshot (by content hash) is already archived.
    """

    response = requests.get(PRICE_GUIDE_URL, timeout=30)
    response.raise_for_status()
    content = response.text
    md5sum = hashlib.md5(content.encode("utf-8"), usedforsecurity=False).hexdigest()

    catalogs_dir.mkdir(parents=True, exist_ok=True)
    if any(md5sum in f.name for f in catalogs_dir.glob("*price_guide*.json.gz")):
        logger.info("Latest price guide (%s) already archived locally.", md5sum)
        return None

    out_path = catalogs_dir / f"{date.today().isoformat()}_{md5sum}_price_guide_1.json.gz"
    with gzip.open(out_path, "wt", encoding="utf-8") as f:
        f.write(content)
    logger.info("Saved new snapshot: %s", out_path.name)
    return out_path


def find_local_catalog_files(catalogs_dir: Path = CATALOGS_DIR) -> list[Path]:
    """Return all local price-guide .gz snapshots, oldest first."""

    return sorted(catalogs_dir.glob("*price_guide*.json.gz"))


def ingest_file(con: duckdb.DuckDBPyConnection, path: Path) -> int:
    """Parse one local price-guide snapshot and insert it if not already ingested.

    Returns the number of rows inserted (0 if this catalog_date was already loaded).

    Checking whether a file's already ingested used to require opening and fully parsing it just to read
    `createdAt` (~170ms/file measured against real data - minutes, across hundreds of files, almost all of which
    turn out to already be loaded). The filename already encodes the date, so that's checked against the DB first,
    with zero file I/O; only a file whose filename-date isn't already present gets actually opened, and its real
    `createdAt` (not the filename guess) is still what gets stored, so a file with a misleading name can't corrupt
    the data - it just loses the fast path.
    """

    guessed_date = _date_from_filename(path)
    if guessed_date is not None:
        already = con.execute("SELECT 1 FROM prices WHERE catalog_date = ? LIMIT 1", [guessed_date]).fetchone()
        if already:
            logger.info("Skipping %s: catalog_date %s (from filename) already ingested.", path.name, guessed_date)
            return 0

    # {read_json} only ever interpolates MAX_JSON_OBJECT_SIZE, a hardcoded int constant - the actual path/columns
    # values are bound parameters (the `?`s below), not interpolated.
    read_json = f"read_json(?, columns=?, maximum_object_size={MAX_JSON_OBJECT_SIZE})"

    # DuckDB's .execute() is not SQLAlchemy's; the rule below matches on method name alone.
    (catalog_date,) = con.execute(
        f"SELECT CAST(createdAt AS DATE) FROM {read_json}", [str(path), PRICE_GUIDE_COLUMNS]
    ).fetchone()

    already = con.execute("SELECT 1 FROM prices WHERE catalog_date = ? LIMIT 1", [catalog_date]).fetchone()
    if already:
        logger.info("Skipping %s: catalog_date %s already ingested.", path.name, catalog_date)
        return 0

    con.execute(
        f"""
        INSERT INTO prices
        SELECT
            pg.idProduct AS cm_id,
            CAST(? AS DATE) AS catalog_date,
            pg.avg, pg.low, pg.trend, pg.avg1, pg.avg7, pg.avg30,
            pg['avg-foil'], pg['low-foil'], pg['trend-foil'],
            pg['avg1-foil'], pg['avg7-foil'], pg['avg30-foil']
        FROM (SELECT UNNEST(priceGuides) AS pg FROM {read_json})
        WHERE pg.idProduct IS NOT NULL
        """,
        [str(catalog_date), str(path), PRICE_GUIDE_COLUMNS],
    )
    (row_count,) = con.execute("SELECT count(*) FROM prices WHERE catalog_date = ?", [catalog_date]).fetchone()
    logger.info("Ingested %s: %d rows for %s.", path.name, row_count, catalog_date)
    return row_count


def ingest_all(con: duckdb.DuckDBPyConnection, catalogs_dir: Path = CATALOGS_DIR) -> int:
    """Ingest every local snapshot not yet loaded. Returns total rows inserted."""

    total = 0
    for path in find_local_catalog_files(catalogs_dir):
        total += ingest_file(con, path)
    return total


def refresh_products(con: duckdb.DuckDBPyConnection) -> int:
    """Fetch the current product catalog live and replace the `products` snapshot.

    Unlike prices, products aren't versioned history - each run just overwrites the table with Cardmarket's
    current view of card -> set/name/metacard.
    """

    response = requests.get(PRODUCT_CATALOG_URL, timeout=30)
    response.raise_for_status()

    with tempfile.NamedTemporaryFile(suffix=".json", mode="w", encoding="utf-8", delete=False) as tmp:
        tmp.write(response.text)
        tmp.flush()
        tmp_path = tmp.name

    try:
        read_json = f"read_json(?, columns=?, maximum_object_size={MAX_JSON_OBJECT_SIZE})"
        con.execute(
            f"""
            CREATE OR REPLACE TABLE products AS
            SELECT
                p.idProduct AS cm_id,
                p.name,
                p.idExpansion AS expansion_id,
                p.idMetacard AS metacard_id,
                p.idCategory AS category_id
            FROM (SELECT UNNEST(products) AS p FROM {read_json})
            WHERE p.idProduct IS NOT NULL
            """,
            [tmp_path, PRODUCT_CATALOG_COLUMNS],
        )
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    (row_count,) = con.execute("SELECT count(*) FROM products").fetchone()
    logger.info("Refreshed products table: %d cards.", row_count)
    return row_count


def run() -> int:
    """Fetch the latest snapshot, ingest all local snapshots, refresh product metadata.

    On a fresh database (no price rows yet), backfills local/catalogs from constants.REMOTE_CATALOGS_URL first -
    see init.py.

    Returns the number of new price rows ingested (0 if nothing was new) - see daily.py, which uses this to decide
    whether there's anything worth pushing to Google Sheets.
    """

    con = get_connection()
    run_if_first_time(con)
    fetch_latest_catalog()
    rows = ingest_all(con)
    products = refresh_products(con)
    logger.info("Done: %d new price rows, %d products in catalog.", rows, products)
    con.close()
    return rows


if __name__ == "__main__":
    run()
