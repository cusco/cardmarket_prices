"""Compare three trend-detection methods against the same 800 cards used in the premodern_bulk export.

A side-by-side evaluation, meant to answer one question: of these, which actually best flags a Premodern card
worth watching? Same 800-card selection and price history as export_premodern_bulk.py (its helpers are imported
directly rather than duplicated), so all three numbers below come from identical inputs. Each is computed for
every window in COMPARISON_WINDOWS_DAYS (3/7/15/30/60 days), so all three are directly comparable at a given
window - one column group per window, three columns per group, sortable by whichever window/metric matters.

- ROC_calc: Rate of Change - the plain two-point % change from the start of the window to today. This is the same
  calculation find_spikes() already does; a single endpoint pair, no smoothing, sensitive to a noisy day on either
  end.
- Linear_regression_calc: an ordinary-least-squares trend line fit across the same window, projected across it and
  expressed as a %, so it lands in the same units as ROC_calc. Uses every day in the window instead of just the two
  endpoints, so one noisy day matters less.
- MACD_calc: the classic technical-analysis momentum indicator (12-day EMA minus 26-day EMA), expressed as a % of
  the latest price. Its own 12/26-day periods don't change with the window - only how much history is fed into it
  does - so 3/7/15-day columns are usually blank (not even enough rows for the 26-day slow EMA to warm up) and 30
  is marginal; 60 is the one actually worth reading. It's a fundamentally different kind of signal (momentum, not a
  %-change-over-a-fixed-period), included so its behaviour can be compared against the other two, not because it's
  expected to read on the same scale.

Two composite columns average Linear_regression_calc (the steadiest of the three, see its docstring above) across
groups of windows: Linear_regression_avg_long_calc (15/30/60d - the medium/long-term read) and
Linear_regression_avg_short_calc (3/7/15d - the short-term read). 15d deliberately sits in both: it's the hinge
between "reacting fast" and "confirming a sustained move." momentum_label buckets every card by comparing those
two (see _momentum_label()'s docstring) - "Turning up"/"Accelerating" for a card that's just starting to move (buy
before it's priced in), "Decelerating"/"Reversing" for one that's already had its run and is cooling off (a good
point to sell), "Flat/falling" otherwise. The whole point of this export is to sort/filter by this column.

Single-day price glitches (one bad/thin listing spiking a card's `trend` for a single day, then vanishing) would
otherwise dominate every one of the above - a real example: Lord of Atlantis (5th Edition) sat at ~EUR 3-7 for
months, then Cardmarket's own trend line spiked briefly north of EUR 130 for a single day. To stop that from
swamping every window, every signal is computed on a trailing PRICE_SPIKE_FILTER_DAYS-day *median* of the price
series, not the raw daily price - a median rejects a single outlier day outright (median of [7, 7, 130] is 7) while
still tracking a move that holds for two or more consecutive days. The raw, unfiltered day-over-day change is still
reported as pct_change_1d so a real glitch stays visible instead of silently vanishing - it just no longer corrupts
the trend numbers.

The uploaded sheet is also formatted, not just written: header cells are colour-coded per window (see
WINDOW_HEADER_COLORS) so it's visually obvious which three columns belong to which window, and every %-based
column gets a red/white/green conditional gradient (below zero -> above zero) so a glance at the colour tells you
the direction without reading the number. momentum_label gets its own traffic-light colouring (see
MOMENTUM_LABEL_COLORS) since it's the column meant to answer "is this one worth acting on."
"""

import time

import gspread
import numpy as np
import pandas as pd
from gspread.exceptions import APIError, GSpreadException

from constants import (
    GOOGLE_SECRET_CREDENTIALS,
    PREMODERN_BULK_HISTORY_BUFFER,
    PREMODERN_BULK_MAX_HISTORY_DAYS,
    PREMODERN_BULK_TOP_N,
    PRICE_FIELD,
    SETS,
    SIGNAL_COMPARISON_WORKSHEET,
    SPREADSHEET_ID,
)
from db import get_connection
from export_premodern_bulk import (
    _cheapest_print_per_card,
    _latest_prices,
    _most_expensive_cards,
    _premodern_products,
    _price_history,
    _recent_catalog_dates,
)

COMPARISON_WINDOWS_DAYS = (3, 7, 15, 30, 60)
LONG_AVG_WINDOWS_DAYS = (15, 30, 60)
SHORT_AVG_WINDOWS_DAYS = (3, 7, 15)
PRICE_SPIKE_FILTER_DAYS = 3  # trailing-median window used to reject single-day price glitches
MACD_FAST_SPAN = 12
MACD_SLOW_SPAN = 26

# Columns that aren't a %-change/momentum metric, so they're excluded from the red/white/green gradient formatting.
NON_METRIC_COLUMNS = ("set_name", "card_name", "momentum_label", "latest_price_date", "floor_price")

# One header colour per window, short (cool) to long (warm), so scanning left-to-right across a window's three
# columns (ROC/regression/MACD) is visually obvious without reading every header.
WINDOW_HEADER_COLORS = {
    3: {"red": 0.80, "green": 0.90, "blue": 1.00},
    7: {"red": 0.80, "green": 0.97, "blue": 0.97},
    15: {"red": 0.85, "green": 0.97, "blue": 0.85},
    30: {"red": 1.00, "green": 0.97, "blue": 0.80},
    60: {"red": 1.00, "green": 0.90, "blue": 0.80},
}
NEUTRAL_HEADER_COLOR = {"red": 0.93, "green": 0.93, "blue": 0.93}
HEADLINE_HEADER_COLOR = {"red": 0.85, "green": 0.80, "blue": 0.95}  # momentum_label + the two composite averages

# Background colour per momentum_label value - a traffic-light read: green means "buy before it rises further",
# red means "already ran, consider selling", grey means "nothing actionable".
MOMENTUM_LABEL_COLORS = {
    "Turning up": {"red": 0.72, "green": 0.88, "blue": 0.80},
    "Accelerating": {"red": 0.85, "green": 0.94, "blue": 0.85},
    "Decelerating": {"red": 1.00, "green": 0.95, "blue": 0.80},
    "Reversing": {"red": 0.98, "green": 0.80, "blue": 0.80},
    "Flat/falling": {"red": 0.93, "green": 0.93, "blue": 0.93},
}


def _despike(card_history: pd.DataFrame) -> pd.DataFrame:
    """Replace each day's price with its trailing PRICE_SPIKE_FILTER_DAYS-day median, to reject one-day glitches."""

    card_history = card_history.sort_values("catalog_date").copy()
    card_history["price"] = card_history["price"].rolling(PRICE_SPIKE_FILTER_DAYS, min_periods=1).median()
    return card_history


def _pct_change_1d(card_history: pd.DataFrame) -> float:
    """Raw (unfiltered) % change from the previous snapshot to the latest one - a glitch shows up here directly."""

    card_history = card_history.sort_values("catalog_date")
    if len(card_history) < 2:
        return np.nan

    latest_price, previous_price = card_history["price"].iloc[-1], card_history["price"].iloc[-2]
    return (latest_price - previous_price) / previous_price * 100


def _roc(card_history: pd.DataFrame, window_days: int) -> float:
    """Rate of Change: % change from the closest snapshot at/before `window_days` ago, to the latest price."""

    card_history = card_history.sort_values("catalog_date")
    latest_date = card_history["catalog_date"].iloc[-1]
    latest_price = card_history["price"].iloc[-1]

    baseline_cutoff = latest_date - pd.Timedelta(days=window_days)
    baseline_rows = card_history[card_history["catalog_date"] <= baseline_cutoff]
    if baseline_rows.empty:
        return np.nan

    baseline_price = baseline_rows["price"].iloc[-1]
    return (latest_price - baseline_price) / baseline_price * 100


def _linear_regression_pct(card_history: pd.DataFrame, window_days: int) -> float:
    """OLS trend slope over the last `window_days`, projected across that window and expressed as a %."""

    card_history = card_history.sort_values("catalog_date")
    latest_date = card_history["catalog_date"].iloc[-1]
    window = card_history[card_history["catalog_date"] >= latest_date - pd.Timedelta(days=window_days)]
    if len(window) < 2:
        return np.nan

    day_numbers = (window["catalog_date"] - window["catalog_date"].min()).dt.days.to_numpy()
    slope, _intercept = np.polyfit(day_numbers, window["price"].to_numpy(), 1)
    return slope * window_days / window["price"].mean() * 100


def _macd_pct(card_history: pd.DataFrame, window_days: int) -> float:
    """MACD line (fast EMA minus slow EMA) within the last `window_days`, expressed as a % of the latest price."""

    card_history = card_history.sort_values("catalog_date")
    latest_date = card_history["catalog_date"].iloc[-1]
    window = card_history[card_history["catalog_date"] >= latest_date - pd.Timedelta(days=window_days)]
    if len(window) < MACD_SLOW_SPAN:
        return np.nan

    ema_fast = window["price"].ewm(span=MACD_FAST_SPAN, adjust=False).mean()
    ema_slow = window["price"].ewm(span=MACD_SLOW_SPAN, adjust=False).mean()
    macd_line = (ema_fast - ema_slow).iloc[-1]
    latest_price = window["price"].iloc[-1]
    return macd_line / latest_price * 100


def _signal_column_names(window_days: int) -> list:
    """The three column names for one window, e.g. ["ROC_calc_7d", "Linear_regression_calc_7d", "MACD_calc_7d"]."""

    suffix = f"_{window_days}d"
    return [f"ROC_calc{suffix}", f"Linear_regression_calc{suffix}", f"MACD_calc{suffix}"]


def _momentum_label(long_avg_calc: float, short_avg_calc: float) -> str:
    """Classify a card by comparing its long-term (15/30/60d) trend against its short-term (3/7/15d) one.

    Five buckets, meant to directly answer "buy before it rises more" vs "this has probably peaked":
    - "Turning up": flat/falling long-term, now rising short-term - hasn't been priced in yet. Buy candidate.
    - "Accelerating": already rising long-term, and still rising faster short-term. Still a buy candidate, but
      partly priced in already.
    - "Decelerating": rising long-term, but short-term has slowed (still positive, just less so). A card that's
      had its run and may be near a peak - worth watching to sell, not yet urgent.
    - "Reversing": rising long-term, but short-term has now turned flat or negative. The clearest "this has
      peaked" signal - most worth selling into.
    - "Flat/falling": neither window is rising. Nothing actionable either way.
    """

    if pd.isna(long_avg_calc) or pd.isna(short_avg_calc):
        return ""
    if long_avg_calc <= 0 and short_avg_calc > 0:
        return "Turning up"
    if long_avg_calc > 0 and short_avg_calc > long_avg_calc:
        return "Accelerating"
    if long_avg_calc > 0 and short_avg_calc > 0:
        return "Decelerating"
    if long_avg_calc > 0 and short_avg_calc <= 0:
        return "Reversing"
    return "Flat/falling"


def _signal_columns(despiked_history: pd.DataFrame, window_days: int) -> dict:
    """The three computed signal values for one card, one window, keyed by _signal_column_names(window_days)."""

    roc_name, regression_name, macd_name = _signal_column_names(window_days)
    return {
        roc_name: _roc(despiked_history, window_days),
        regression_name: _linear_regression_pct(despiked_history, window_days),
        macd_name: _macd_pct(despiked_history, window_days),
    }


def _compute_signals(history_df: pd.DataFrame) -> pd.DataFrame:
    """Compute ROC/regression/MACD for every card in history_df, for each of COMPARISON_WINDOWS_DAYS.

    One row per cm_id. Also includes pct_change_1d (raw, not despiked) and the two Linear_regression_calc
    composite averages.
    """

    rows = []
    for cm_id, card_history in history_df.groupby("cm_id"):
        card_history = card_history.sort_values("catalog_date")
        row = {"cm_id": cm_id, "pct_change_1d": _pct_change_1d(card_history)}

        despiked_history = _despike(card_history)
        for window_days in COMPARISON_WINDOWS_DAYS:
            row.update(_signal_columns(despiked_history, window_days))

        long_columns = [f"Linear_regression_calc_{w}d" for w in LONG_AVG_WINDOWS_DAYS]
        short_columns = [f"Linear_regression_calc_{w}d" for w in SHORT_AVG_WINDOWS_DAYS]
        row["Linear_regression_avg_long_calc"] = np.nanmean([row[c] for c in long_columns])
        row["Linear_regression_avg_short_calc"] = np.nanmean([row[c] for c in short_columns])
        row["momentum_label"] = _momentum_label(
            row["Linear_regression_avg_long_calc"], row["Linear_regression_avg_short_calc"]
        )

        rows.append(row)
    return pd.DataFrame(rows)


def _build_comparison_table(cards_df: pd.DataFrame, signals_df: pd.DataFrame, latest_price_date) -> pd.DataFrame:
    """Attach card/set labels and the latest price date to the computed signals, and round for display."""

    labels = cards_df[["cm_id", "name", "expansion_id"]].copy()
    labels["set_name"] = labels["expansion_id"].map(lambda eid: SETS.get(eid, {}).get("name", "Unknown"))
    labels = labels.rename(columns={"name": "card_name"})[["cm_id", "card_name", "set_name"]]

    table = cards_df[["cm_id", "floor_price"]].merge(labels, on="cm_id").merge(signals_df, on="cm_id")
    table = table.sort_values("floor_price", ascending=False)
    table.insert(2, "latest_price_date", str(latest_price_date))

    signal_columns = [name for window_days in COMPARISON_WINDOWS_DAYS for name in _signal_column_names(window_days)]
    label_columns = ("set_name", "card_name", "latest_price_date", "momentum_label")
    ordered_columns = [
        "set_name",
        "card_name",
        "momentum_label",
        "latest_price_date",
        "floor_price",
        "pct_change_1d",
        *signal_columns,
        "Linear_regression_avg_long_calc",
        "Linear_regression_avg_short_calc",
    ]
    table = table[ordered_columns]

    numeric_columns = [c for c in ordered_columns if c not in label_columns]
    table[numeric_columns] = table[numeric_columns].round(2)

    return table.reset_index(drop=True).fillna("")


def _header_color_for_column(column: str) -> dict:
    """Which background colour a column's header cell gets - by window if it's a windowed metric, else neutral."""

    for window_days, color in WINDOW_HEADER_COLORS.items():
        if column.endswith(f"_{window_days}d"):
            return color
    if column in ("momentum_label", "Linear_regression_avg_long_calc", "Linear_regression_avg_short_calc"):
        return HEADLINE_HEADER_COLOR
    return NEUTRAL_HEADER_COLOR


def _header_format_requests(sheet_id: int, table: pd.DataFrame) -> list:
    """Bold + colour-code every header cell (by window, see _header_color_for_column), and freeze the header row."""

    requests = [
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                    "startColumnIndex": col_index,
                    "endColumnIndex": col_index + 1,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": _header_color_for_column(column),
                        "textFormat": {"bold": True},
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat)",
            }
        }
        for col_index, column in enumerate(table.columns)
    ]
    requests.append(
        {
            "updateSheetProperties": {
                "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}},
                "fields": "gridProperties.frozenRowCount",
            }
        }
    )
    return requests


def _gradient_rule_request(sheet_id: int, col_index: int, num_data_rows: int, rule_index: int) -> dict:
    """A red (below zero) / white (zero) / green (above zero) gradient over one column's data rows."""

    column_range = {
        "sheetId": sheet_id,
        "startRowIndex": 1,
        "endRowIndex": 1 + num_data_rows,
        "startColumnIndex": col_index,
        "endColumnIndex": col_index + 1,
    }
    return {
        "addConditionalFormatRule": {
            "rule": {
                "ranges": [column_range],
                "gradientRule": {
                    "minpoint": {"type": "MIN", "color": {"red": 0.96, "green": 0.62, "blue": 0.60}},
                    "midpoint": {"type": "NUMBER", "value": "0", "color": {"red": 1.0, "green": 1.0, "blue": 1.0}},
                    "maxpoint": {"type": "MAX", "color": {"red": 0.65, "green": 0.86, "blue": 0.65}},
                },
            },
            "index": rule_index,
        }
    }


def _momentum_label_rule_request(
    sheet_id: int, col_index: int, num_data_rows: int, rule_index: int, label: str
) -> dict:
    """Solid background for every cell in the momentum_label column equal to `label`."""

    column_range = {
        "sheetId": sheet_id,
        "startRowIndex": 1,
        "endRowIndex": 1 + num_data_rows,
        "startColumnIndex": col_index,
        "endColumnIndex": col_index + 1,
    }
    return {
        "addConditionalFormatRule": {
            "rule": {
                "ranges": [column_range],
                "booleanRule": {
                    "condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": label}]},
                    "format": {"backgroundColor": MOMENTUM_LABEL_COLORS[label]},
                },
            },
            "index": rule_index,
        }
    }


def _conditional_format_requests(sheet_id: int, table: pd.DataFrame) -> list:
    """Gradient rules for every %-metric column, plus traffic-light rules for momentum_label."""

    num_data_rows = len(table)
    requests = []

    metric_columns = [c for c in table.columns if c not in NON_METRIC_COLUMNS]
    for col_index, column in enumerate(table.columns):
        if column in metric_columns:
            requests.append(_gradient_rule_request(sheet_id, col_index, num_data_rows, len(requests)))

    label_col_index = table.columns.get_loc("momentum_label")
    for label in MOMENTUM_LABEL_COLORS:
        requests.append(_momentum_label_rule_request(sheet_id, label_col_index, num_data_rows, len(requests), label))

    return requests


def _clear_conditional_formats(worksheet) -> list:
    """Delete every existing conditional format rule on the sheet, so re-running this export doesn't stack rules."""

    metadata = worksheet.spreadsheet.fetch_sheet_metadata()
    sheet_properties = next(s for s in metadata["sheets"] if s["properties"]["sheetId"] == worksheet.id)
    existing_rule_count = len(sheet_properties.get("conditionalFormats", []))

    return [
        {"deleteConditionalFormatRule": {"sheetId": worksheet.id, "index": index}}
        for index in reversed(range(existing_rule_count))
    ]


def _apply_formatting(worksheet, table: pd.DataFrame) -> None:
    """Header colour-coding + freeze, and red/white/green + traffic-light conditional formatting."""

    clear_requests = _clear_conditional_formats(worksheet)
    if clear_requests:
        worksheet.spreadsheet.batch_update({"requests": clear_requests})

    requests = _header_format_requests(worksheet.id, table) + _conditional_format_requests(worksheet.id, table)
    worksheet.spreadsheet.batch_update({"requests": requests})


def _upload_to_sheet(table: pd.DataFrame, start: float) -> str:
    """Clear + rewrite the signal-comparison tab, then re-apply its formatting. Returns a status message."""

    try:
        client = gspread.service_account(filename=str(GOOGLE_SECRET_CREDENTIALS))
        sheet = client.open_by_key(SPREADSHEET_ID)

        try:
            worksheet = sheet.worksheet(SIGNAL_COMPARISON_WORKSHEET)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sheet.add_worksheet(title=SIGNAL_COMPARISON_WORKSHEET, rows=1000, cols=len(table.columns))

        worksheet.clear()
        worksheet.update([table.columns.values.tolist()] + table.values.tolist())
        _apply_formatting(worksheet, table)

        elapsed = time.perf_counter() - start
        return f"Success: {len(table)} cards compared in '{SIGNAL_COMPARISON_WORKSHEET}' in {elapsed:.2f}s."
    except (GSpreadException, APIError) as err:
        return f"Google Sheets error: {err}"
    except OSError as err:
        return f"File system error: {err}"


def export_signal_comparison_to_gdrive(con=None) -> str:
    """Rebuild the ROC/regression/MACD comparison table for the same 800 cards export_premodern_bulk.py uses.

    Steps: reuse export_premodern_bulk's card selection and price history -> compute all three signals per card ->
    build a labeled comparison table -> upload.
    """

    start = time.perf_counter()
    owns_connection = con is None
    if owns_connection:
        con = get_connection()

    try:
        cur_date = con.execute("SELECT max(catalog_date) FROM prices").fetchone()[0]
        if cur_date is None:
            return "Nothing to compare: no price data yet."

        products_df = _premodern_products(con)
        prices_df = _latest_prices(con, cur_date, PRICE_FIELD)
        cheapest_df = _cheapest_print_per_card(products_df, prices_df)
        if cheapest_df.empty:
            return "Nothing to compare: no matching Premodern cards found."

        cards_df = _most_expensive_cards(cheapest_df, PREMODERN_BULK_TOP_N)

        recent_dates = _recent_catalog_dates(con, PREMODERN_BULK_MAX_HISTORY_DAYS, PREMODERN_BULK_HISTORY_BUFFER)
        history_df = _price_history(con, cards_df["cm_id"].tolist(), min(recent_dates), PRICE_FIELD)
        if history_df.empty:
            return "Nothing to compare: no history data found."

        history_df["catalog_date"] = pd.to_datetime(history_df["catalog_date"])
        signals_df = _compute_signals(history_df)
        table = _build_comparison_table(cards_df, signals_df, cur_date)

        return _upload_to_sheet(table, start)
    finally:
        if owns_connection:
            con.close()


if __name__ == "__main__":
    print(export_signal_comparison_to_gdrive())
