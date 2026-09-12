"""Rebuild the retired old/ Django project's Premodern "floor price" bulk export.

For every Premodern-legal card, find its cheapest available printing today, keep the 800 priciest of those (the
most valuable cards, ranked by the cheapest way to own one), then pull each one's last 60 days of price history and
push it to the shared spreadsheet's ORIGINAL "premodern_bulk"/"status" tabs - the ones the pre-DuckDB-rewrite
project (old/src/prices/export.py) used to write, which export_gdrive.py's own price_pulse_* tabs deliberately
don't touch.

Deliberately written as small, sequential steps instead of one dense query - each function below does one thing;
see export_premodern_bulk_to_gdrive() at the bottom for how they chain together.
"""

import time
from datetime import datetime

import gspread
import pandas as pd
from gspread.exceptions import APIError, GSpreadException

from constants import (
    FORMATS,
    GOOGLE_SECRET_CREDENTIALS,
    PREMODERN_BULK_EXCLUDED_EXPANSION_IDS,
    PREMODERN_BULK_HISTORY_BUFFER,
    PREMODERN_BULK_MAX_HISTORY_DAYS,
    PREMODERN_BULK_STATUS_WORKSHEET,
    PREMODERN_BULK_TOP_N,
    PREMODERN_BULK_WORKSHEET,
    PRICE_FIELD,
    SETS,
    SPREADSHEET_ID,
)
from db import get_connection


def _premodern_products(con) -> pd.DataFrame:
    """All products legal in Premodern, minus the two oversized-promo expansions that aren't real prints."""

    premodern_ids = FORMATS["premodern"] - PREMODERN_BULK_EXCLUDED_EXPANSION_IDS
    placeholders = ",".join(str(i) for i in premodern_ids)  # developer-controlled config, not external input
    query = f"SELECT cm_id, name, expansion_id, metacard_id FROM products WHERE expansion_id IN ({placeholders})"
    return con.execute(query).df()


def _latest_prices(con, cur_date, price_field) -> pd.DataFrame:
    """Today's price for every card, for one price field, excluding untracked (<=1 cent) entries."""

    query = f"SELECT cm_id, {price_field} AS price FROM prices WHERE catalog_date = ? AND {price_field} > 0.01"
    return con.execute(query, [cur_date]).df()


def _cheapest_print_per_card(products_df, prices_df) -> pd.DataFrame:
    """For each Premodern card (by metacard_id), keep only its cheapest-priced printing today."""

    merged = products_df.merge(prices_df, on="cm_id")
    if merged.empty:
        return merged.rename(columns={"price": "floor_price"})

    merged = merged.sort_values("cm_id")  # deterministic tie-break: lowest cm_id wins a tied floor price
    cheapest_rows = merged.loc[merged.groupby("metacard_id")["price"].idxmin()]
    return cheapest_rows.rename(columns={"price": "floor_price"})


def _most_expensive_cards(cheapest_df, top_n) -> pd.DataFrame:
    """Of those floor prices, keep the N priciest cards - "cheapest way to own an expensive card"."""

    return cheapest_df.sort_values("floor_price", ascending=False).head(top_n)


def _recent_catalog_dates(con, days, buffer_days) -> list:
    """The most recent `days + buffer_days` distinct catalog dates that exist, newest first."""

    rows = con.execute(
        "SELECT DISTINCT catalog_date FROM prices ORDER BY catalog_date DESC LIMIT ?",
        [days + buffer_days],
    ).fetchall()
    return [row[0] for row in rows]


def _price_history(con, cm_ids, since_date, price_field) -> pd.DataFrame:
    """Per-card, per-day price history for the given cards from `since_date` onward."""

    query = f"""
        SELECT cm_id, catalog_date, {price_field} AS price
        FROM prices
        WHERE cm_id = ANY(?) AND catalog_date >= ? AND {price_field} > 0.01
        ORDER BY cm_id, catalog_date
    """
    return con.execute(query, [cm_ids, since_date]).df()


def _add_card_labels(history_df, cards_df) -> pd.DataFrame:
    """Attach each history row's card name and set name, for use as the pivot table's row labels."""

    labels = cards_df[["cm_id", "name", "expansion_id"]].copy()
    labels["set_name"] = labels["expansion_id"].map(lambda eid: SETS.get(eid, {}).get("name", "Unknown"))
    labels = labels.rename(columns={"name": "card_name"})[["cm_id", "card_name", "set_name"]]

    return history_df.merge(labels, on="cm_id")


def _pivot_history(labeled_df, max_days) -> pd.DataFrame:
    """Reshape into (set_name, card_name) rows x date columns, newest date first, capped at max_days columns."""

    labeled_df = labeled_df.copy()
    labeled_df["date_only"] = pd.to_datetime(labeled_df["catalog_date"]).dt.date

    pivoted = labeled_df.pivot_table(
        index=["set_name", "card_name"], columns="date_only", values="price", aggfunc="mean"
    )
    pivoted = pivoted.reindex(sorted(pivoted.columns, reverse=True), axis=1)
    pivoted = pivoted.iloc[:, :max_days]

    if not pivoted.empty:
        newest_column = pivoted.columns[0]
        pivoted = pivoted.sort_values(by=newest_column, ascending=False)

    pivoted.columns = [column.strftime("%Y-%m-%d") for column in pivoted.columns]
    return pivoted.reset_index().fillna("")


def _upload_to_sheets(final_df, cur_date, price_field, recent_dates_count, start) -> str:
    """Clear + rewrite the 'premodern_bulk' data tab, then the 'status' metadata tab. Returns a status message."""

    try:
        client = gspread.service_account(filename=str(GOOGLE_SECRET_CREDENTIALS))
        sheet = client.open_by_key(SPREADSHEET_ID)

        # 1. Main data tab
        bulk_sheet = sheet.worksheet(PREMODERN_BULK_WORKSHEET)
        bulk_sheet.clear()
        bulk_sheet.update([final_df.columns.values.tolist()] + final_df.values.tolist())

        # 2. Stop the timer only after the upload finishes, for the true end-to-end runtime
        elapsed_seconds = time.perf_counter() - start
        minutes, seconds = divmod(elapsed_seconds, 60)
        runtime = f"{int(minutes)}m {int(seconds)}s" if minutes > 0 else f"{seconds:.2f} seconds"

        # 3. Status/metadata tab
        status_sheet = sheet.worksheet(PREMODERN_BULK_STATUS_WORKSHEET)
        status_sheet.clear()
        status_sheet.update(
            [
                ["Metric", "Value"],
                ["Last script run", datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
                ["Total Export Runtime", runtime],
                ["Latest pricing date", str(cur_date)],
                ["Price Metric Tracked", price_field.upper()],
                ["Data Window", f"{recent_dates_count} entries"],
                ["Total Cards Tracked", len(final_df)],
            ]
        )
        return (
            f"Success: {len(final_df)} cards uploaded to '{PREMODERN_BULK_WORKSHEET}' using {price_field} in {runtime}."
        )
    except (GSpreadException, APIError) as err:
        return f"Google Sheets error: {err}"
    except OSError as err:
        return f"File system error: {err}"


def export_premodern_bulk_to_gdrive(con=None) -> str:
    """Rebuild the Premodern floor-price bulk export and push it to the original premodern_bulk/status tabs.

    Steps: find today's cheapest printing for every Premodern card -> keep the 800 priciest of those -> pull each
    one's last 60 days of price history -> pivot into a sheet-shaped table -> upload.
    """

    start = time.perf_counter()
    owns_connection = con is None
    if owns_connection:
        con = get_connection()

    try:
        cur_date = con.execute("SELECT max(catalog_date) FROM prices").fetchone()[0]
        if cur_date is None:
            return "Nothing to export: no price data yet."

        products_df = _premodern_products(con)
        prices_df = _latest_prices(con, cur_date, PRICE_FIELD)
        cheapest_df = _cheapest_print_per_card(products_df, prices_df)
        if cheapest_df.empty:
            return "Nothing to export: no matching Premodern cards found."

        cards_df = _most_expensive_cards(cheapest_df, PREMODERN_BULK_TOP_N)

        recent_dates = _recent_catalog_dates(con, PREMODERN_BULK_MAX_HISTORY_DAYS, PREMODERN_BULK_HISTORY_BUFFER)
        history_df = _price_history(con, cards_df["cm_id"].tolist(), min(recent_dates), PRICE_FIELD)
        if history_df.empty:
            return "Nothing to export: no history data found."

        labeled_df = _add_card_labels(history_df, cards_df)
        final_df = _pivot_history(labeled_df, PREMODERN_BULK_MAX_HISTORY_DAYS)

        return _upload_to_sheets(final_df, cur_date, PRICE_FIELD, len(recent_dates), start)
    finally:
        if owns_connection:
            con.close()


if __name__ == "__main__":
    print(export_premodern_bulk_to_gdrive())
