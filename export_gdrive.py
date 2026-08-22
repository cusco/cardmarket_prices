"""Push the current spikes table to Google Sheets."""

import logging
import time

import gspread
from gspread.exceptions import APIError, GSpreadException

from constants import GOOGLE_SECRET_CREDENTIALS, SPIKES_WORKSHEET, SPREADSHEET_ID
from db import get_connection
from spikes import find_spikes

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def export_spikes_to_gdrive(spikes_df=None) -> str:
    """Upload the current spikes table to the SPIKES_WORKSHEET tab. Returns a status message."""

    start = time.perf_counter()

    if spikes_df is None:
        con = get_connection()
        spikes_df = find_spikes(con)
        con.close()

    if spikes_df.empty:
        return "Nothing to export: no cards matched the spike thresholds."

    df = spikes_df.copy()
    for date_col in ("latest_date", "baseline_date"):
        df[date_col] = df[date_col].astype(str)

    try:
        client = gspread.service_account(filename=str(GOOGLE_SECRET_CREDENTIALS))
        sheet = client.open_by_key(SPREADSHEET_ID)

        try:
            ws = sheet.worksheet(SPIKES_WORKSHEET)
        except gspread.exceptions.WorksheetNotFound:
            ws = sheet.add_worksheet(title=SPIKES_WORKSHEET, rows=1000, cols=len(df.columns))

        ws.clear()
        ws.update([df.columns.values.tolist()] + df.values.tolist())

        elapsed = time.perf_counter() - start
        return f"Success: {len(df)} spiking cards uploaded to '{SPIKES_WORKSHEET}' in {elapsed:.2f}s."
    except (GSpreadException, APIError) as err:
        return f"Google Sheets error: {err}"
    except OSError as err:
        return f"File system error: {err}"


if __name__ == "__main__":
    print(export_spikes_to_gdrive())
