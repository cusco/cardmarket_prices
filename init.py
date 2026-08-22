"""First-time setup: detect an empty database and backfill it before ingesting.

Called automatically from ingest.py's run() - a fresh checkout doesn't need a separate setup command, just
`python ingest.py`. Detection is simple: the `prices` table has zero rows.

If constants.REMOTE_CATALOGS_URL is set (an Apache-style directory listing of historical .gz price-guide snapshots
- e.g. an S3/OVH bucket with autoindex on), first-run also backfills local/catalogs from it before the normal
ingest runs, so a fresh checkout starts with real history instead of just today's snapshot. Same idea as the old
project's README instructing a manual `wget -r -A "*.gz"` step, just built into the tool instead of a doc a person
has to remember to follow.

If REMOTE_CATALOGS_URL isn't set, first-run just logs that no backfill is configured and lets the normal ingest.py
flow continue as usual.
"""

import logging
import re
from urllib.parse import urljoin

import duckdb
import requests

from constants import CATALOGS_DIR, REMOTE_CATALOGS_URL

logger = logging.getLogger(__name__)

HREF_GZ_RE = re.compile(r'href="([^"]+\.gz)"')


def is_first_run(con: duckdb.DuckDBPyConnection) -> bool:
    """Return True if the prices table has no data yet."""

    (count,) = con.execute("SELECT count(*) FROM prices").fetchone()
    return count == 0


def fetch_remote_catalog_listing(url: str) -> list[str]:
    """Parse an Apache-style directory listing page and return absolute .gz URLs."""

    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return [urljoin(url, href) for href in HREF_GZ_RE.findall(response.text)]


def download_catalog(url: str, catalogs_dir=CATALOGS_DIR) -> bool:
    """Download one remote catalog file if not already present locally.

    Returns True if it was downloaded, False if it already existed.
    """

    filename = url.rsplit("/", 1)[-1]
    dest = catalogs_dir / filename
    if dest.exists():
        return False

    response = requests.get(url, timeout=60)
    response.raise_for_status()
    catalogs_dir.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(response.content)
    return True


def bootstrap_from_remote(catalogs_dir=CATALOGS_DIR) -> int:
    """Backfill catalogs_dir from REMOTE_CATALOGS_URL. Returns count downloaded."""

    if not REMOTE_CATALOGS_URL:
        logger.info("No REMOTE_CATALOGS_URL configured - skipping remote backfill.")
        return 0

    urls = fetch_remote_catalog_listing(REMOTE_CATALOGS_URL)
    logger.info("Found %d catalog(s) at %s.", len(urls), REMOTE_CATALOGS_URL)

    downloaded = 0
    for url in urls:
        if download_catalog(url, catalogs_dir):
            downloaded += 1
    return downloaded


def run_if_first_time(con: duckdb.DuckDBPyConnection) -> bool:
    """If this looks like a fresh database, backfill from the remote mirror.

    Returns whether first-time setup ran (so callers can log accordingly).
    """

    if not is_first_run(con):
        return False

    logger.info("Empty database detected - running first-time setup.")
    downloaded = bootstrap_from_remote()
    logger.info("First-time setup: backfilled %d catalog(s) from remote mirror.", downloaded)
    return True
