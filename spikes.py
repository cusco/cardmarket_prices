"""Spike detection: which cards are rising in price while still affordable.

Two independent signals so far, meant to be combined/compared over time rather than treated as the final answer -
see find_price_gaps()'s docstring for where this is headed next.

find_spikes() - week-over-week trend movement. Unlike the original Django project's slope table (fixed 2/7/30-day
calendar windows that return nothing if a card has no second data point inside the window) or its "quick hack"
fallback (counts the last N price *rows*, not the last N *days*, so a gap in history silently mixes old and new
prices together), this uses DuckDB's ASOF JOIN: for each card, find the closest price point at or before
"window_days ago", whatever date that actually lands on, and report the real elapsed days alongside the percentage
change. A comparison spanning a data gap is still visible as such, instead of being silently wrong.

find_price_gaps() - a same-day signal, not a trend over time: how far today's cheapest listing (`low`) has pulled
away from the smoothed `trend` price. Ported from an even older project's heuristic (~/cardmarket_scraper,
src/lib/utils.py's show_spikes()/show_stats()) that scraped Cardmarket's per-card chart widget directly rather than
the bulk price-guide JSON this project uses - the scraping approach doesn't carry over, but the underlying idea
does, and it's data we already have and weren't using.
"""

import duckdb

from constants import (
    DEFAULT_MAX_PRICE,
    DEFAULT_MIN_GAP_ABS,
    DEFAULT_MIN_GAP_PCT,
    DEFAULT_MIN_PCT,
    DEFAULT_MIN_PRICE,
    DEFAULT_WINDOW_DAYS,
    FORMATS,
    PRICE_FIELD,
    SETS,
)
from db import get_connection


def find_spikes(
    con: duckdb.DuckDBPyConnection,
    window_days: int = DEFAULT_WINDOW_DAYS,
    min_price: float = DEFAULT_MIN_PRICE,
    max_price: float = DEFAULT_MAX_PRICE,
    min_pct: float = DEFAULT_MIN_PCT,
    format_name: str | None = "premodern",
    limit: int = 25,
):
    """Return the top cards by % price change over `window_days`, as a DataFrame.

    Both the latest and the baseline price must clear `min_price` (filters out bulk-card noise, where a few cents'
    move looks like a huge percentage), and the latest price must stay under `max_price` ("still affordable").

    `format_name` filters to one of the id sets in constants.FORMATS (e.g. "premodern"); pass None to scan every
    set Cardmarket sells.
    """

    # set_filter/{PRICE_FIELD} only ever interpolate constants.FORMATS/PRICE_FIELD - developer-controlled config,
    # never external input. All actual values are bound parameters below.
    set_filter = ""
    if format_name is not None:
        expansion_ids = FORMATS[format_name]
        ids = ",".join(str(i) for i in expansion_ids)
        set_filter = f"AND pr.expansion_id IN ({ids})"  # nosec B608

    # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
    query = f"""
    WITH latest AS (
        SELECT * FROM prices WHERE catalog_date = (SELECT max(catalog_date) FROM prices)
    ),
    joined AS (
        SELECT
            l.cm_id,
            pr.name,
            pr.expansion_id,
            l.catalog_date AS latest_date,
            l.{PRICE_FIELD} AS latest_price,
            b.catalog_date AS baseline_date,
            b.{PRICE_FIELD} AS baseline_price,
            date_diff('day', b.catalog_date, l.catalog_date) AS elapsed_days,
            (l.{PRICE_FIELD} - b.{PRICE_FIELD}) / b.{PRICE_FIELD} * 100 AS pct_change
        FROM latest l
        JOIN products pr ON pr.cm_id = l.cm_id
        ASOF LEFT JOIN prices b
            ON b.cm_id = l.cm_id AND l.catalog_date - INTERVAL (?) DAY >= b.catalog_date
        WHERE l.{PRICE_FIELD} BETWEEN ? AND ?
          AND b.{PRICE_FIELD} >= ?
          {set_filter}
    )
    SELECT * FROM joined
    WHERE pct_change >= ?
    ORDER BY pct_change DESC
    LIMIT ?
    """  # nosec B608
    return con.execute(query, [window_days, min_price, max_price, min_price, min_pct, limit]).df()


def print_spikes(df) -> None:
    """Pretty-print a find_spikes() result to stdout."""

    if df.empty:
        print("No spiking cards found for the given thresholds.")
        return

    header = "{:<32} {:<10} {:>8} {:>8} {:>8} {:>8}".format("Name", "Set", "Now", "Before", "Days", "Change%")
    print(header)
    print("-" * len(header))
    for row in df.itertuples():
        set_meta = SETS.get(row.expansion_id, {})
        set_label = set_meta.get("code") or set_meta.get("name") or f"(id {row.expansion_id})"
        name = row.name[:29] + "..." if len(row.name) > 32 else row.name
        print(
            "{:<32} {:<10} {:>8.2f} {:>8.2f} {:>8} {:>+7.1f}%".format(
                name, set_label, row.latest_price, row.baseline_price, row.elapsed_days, row.pct_change
            )
        )


def find_price_gaps(
    con: duckdb.DuckDBPyConnection,
    min_price: float = DEFAULT_MIN_PRICE,
    max_price: float = DEFAULT_MAX_PRICE,
    min_gap_pct: float = DEFAULT_MIN_GAP_PCT,
    min_gap_abs: float = DEFAULT_MIN_GAP_ABS,
    format_name: str | None = "premodern",
    limit: int = 25,
):
    """Find cards where today's cheapest listing (`low`) has pulled away from `trend`.

    Not a trend over time like find_spikes() - a single day's snapshot. The idea: `trend` is Cardmarket's smoothed
    price, `low` is what you'd actually pay right now. When `low` sits well above `trend`, the cheap copies have
    already sold and remaining sellers are asking more - a live "the good price is going/gone" signal, complementary
    to (and often earlier than) a week-over-week trend move.

    Both a percentage (`min_gap_pct`) and an absolute euro (`min_gap_abs`) threshold apply, same reasoning as
    find_spikes()'s min_price floor: without the absolute floor, a cent-level gap on a near-free card reads as a
    huge %.

    `min_price`/`max_price` bound `low` (what you'd actually pay); `trend` is separately floored at `min_price` too
    (not just `> 0`) - without that, a real Pioneer example: a card with trend=0.02 and low=16.00 (a near-worthless
    variant with one oddly-priced listing) reported as a "+79900%" gap. Same class of bulk-card noise find_spikes()
    already guards against by flooring both sides of its comparison, this just hadn't been applied here yet.

    This is a first cut at a much bigger question - eventually this project wants a real statistical view across
    trend/low/avg (and their medians) together, not three separate ad hoc signals. Treat this function as a
    building block for that, not the final design.

    Checked empirically against real Premodern data (2026-08): `low > trend` essentially never happens in this
    dataset - 0 of 502 cards in the 1-20 EUR range, and swapping in avg7/avg30 as the baseline instead of trend
    barely changes that (0 and 1 of 502). The old project this idea came from scraped Cardmarket's live per-card
    page; this project ingests one bulk end-of-day snapshot, where `low` and `trend` are computed from the same
    moment together - there's no lag for `low` to spike ahead of. This function is correct and will fire the day
    it's warranted, but don't expect it to surface much on daily-snapshot data the way it did on live-scraped data.
    See CLAUDE.md for where this points next (comparing the *trajectory* of the gap over time, rather than its size
    on a single day, is the more promising direction).
    """

    # set_filter only ever interpolates constants.FORMATS - developer-controlled config, never external input. All
    # actual values are bound parameters below.
    set_filter = ""
    if format_name is not None:
        expansion_ids = FORMATS[format_name]
        ids = ",".join(str(i) for i in expansion_ids)
        set_filter = f"AND pr.expansion_id IN ({ids})"  # nosec B608

    # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
    query = f"""
    WITH latest AS (
        SELECT * FROM prices WHERE catalog_date = (SELECT max(catalog_date) FROM prices)
    ),
    joined AS (
        SELECT
            l.cm_id,
            pr.name,
            pr.expansion_id,
            l.catalog_date,
            l.low,
            l.trend,
            l.low - l.trend AS gap_abs,
            (l.low - l.trend) / l.trend * 100 AS gap_pct
        FROM latest l
        JOIN products pr ON pr.cm_id = l.cm_id
        WHERE l.low IS NOT NULL AND l.trend IS NOT NULL
          AND l.low BETWEEN ? AND ?
          AND l.trend >= ?
          {set_filter}
    )
    SELECT * FROM joined
    WHERE gap_pct >= ? AND gap_abs >= ?
    ORDER BY gap_pct DESC
    LIMIT ?
    """  # nosec B608
    return con.execute(query, [min_price, max_price, min_price, min_gap_pct, min_gap_abs, limit]).df()


def print_price_gaps(df) -> None:
    """Pretty-print a find_price_gaps() result to stdout."""

    if df.empty:
        print("No cards found with a low/trend gap for the given thresholds.")
        return

    header = "{:<32} {:<10} {:>8} {:>8} {:>8}".format("Name", "Set", "Low", "Trend", "Gap%")
    print(header)
    print("-" * len(header))
    for row in df.itertuples():
        set_meta = SETS.get(row.expansion_id, {})
        set_label = set_meta.get("code") or set_meta.get("name") or f"(id {row.expansion_id})"
        name = row.name[:29] + "..." if len(row.name) > 32 else row.name
        print("{:<32} {:<10} {:>8.2f} {:>8.2f} {:>+7.1f}%".format(name, set_label, row.low, row.trend, row.gap_pct))


if __name__ == "__main__":
    connection = get_connection()

    print("=== Trending up (week-over-week) ===")
    print_spikes(find_spikes(connection))

    print()
    print("=== Price gaps (low pulling away from trend) ===")
    print_price_gaps(find_price_gaps(connection))

    connection.close()
