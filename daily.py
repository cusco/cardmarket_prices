"""Daily entry point: ingest the latest snapshot, push to Google Sheets only if it changed anything.

Meant to be invoked by scripts/cron_ingest.sh. Kept separate from ingest.py's run() so this "only export when
something's new" decision is its own testable unit, rather than conditional logic buried in a shell script.
"""

import logging

from export_gdrive import export_spikes_to_gdrive
from export_premodern_bulk import export_premodern_bulk_to_gdrive
from ingest import run as run_ingest

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run() -> None:
    """Ingest the latest snapshot; push to Google Sheets only if new rows arrived."""

    new_rows = run_ingest()
    if new_rows:
        logger.info("New data ingested (%d rows) - exporting to Google Sheets.", new_rows)
        logger.info(export_spikes_to_gdrive())
        logger.info(export_premodern_bulk_to_gdrive())
    else:
        logger.info("No new data this run - skipping Google Sheets export.")


if __name__ == "__main__":
    run()
