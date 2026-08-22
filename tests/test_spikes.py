from datetime import date

import pandas as pd
import pytest

import spikes
from spikes import find_price_gaps, find_spikes, print_price_gaps, print_spikes


def _seed_card(con, cm_id, name, expansion_id):
    con.execute("INSERT INTO products VALUES (?, ?, ?, 1, 1)", [cm_id, name, expansion_id])


def _seed_price(con, cm_id, catalog_date, trend):
    con.execute(
        "INSERT INTO prices (cm_id, catalog_date, trend) VALUES (?, ?, ?)",
        [cm_id, catalog_date, trend],
    )


def _seed_low_trend(con, cm_id, catalog_date, low, trend):
    con.execute(
        "INSERT INTO prices (cm_id, catalog_date, low, trend) VALUES (?, ?, ?, ?)",
        [cm_id, catalog_date, low, trend],
    )


def test_computes_percent_change_and_elapsed_days(con):
    _seed_card(con, 1, "Rising Card", 16)
    _seed_price(con, 1, date(2026, 1, 1), 1.00)
    _seed_price(con, 1, date(2026, 1, 8), 2.00)  # +100% over exactly 7 days

    df = find_spikes(con, window_days=7, min_price=0.5, max_price=10, min_pct=1, format_name=None)

    assert len(df) == 1
    row = df.iloc[0]
    assert row["latest_price"] == 2.00
    assert row["baseline_price"] == 1.00
    assert row["elapsed_days"] == 7
    assert row["pct_change"] == pytest.approx(100.0)


def test_reports_the_real_gap_not_the_requested_window(con):
    """The whole point of the ASOF join: a comparison across a data gap says so."""

    _seed_card(con, 1, "Gappy Card", 16)
    _seed_price(con, 1, date(2026, 1, 1), 1.00)
    _seed_price(con, 1, date(2026, 3, 1), 2.00)  # 59 days later, not the requested 7

    df = find_spikes(con, window_days=7, min_price=0.5, max_price=10, min_pct=1, format_name=None)

    assert len(df) == 1
    assert df.iloc[0]["elapsed_days"] == 59


def test_excludes_changes_below_min_pct(con):
    _seed_card(con, 1, "Flat Card", 16)
    _seed_price(con, 1, date(2026, 1, 1), 1.00)
    _seed_price(con, 1, date(2026, 1, 8), 1.01)  # +1%, below a 5% threshold

    df = find_spikes(con, window_days=7, min_price=0.5, max_price=10, min_pct=5, format_name=None)

    assert df.empty


def test_excludes_prices_above_max_price(con):
    _seed_card(con, 1, "Expensive Card", 16)
    _seed_price(con, 1, date(2026, 1, 1), 50.00)
    _seed_price(con, 1, date(2026, 1, 8), 100.00)

    df = find_spikes(con, window_days=7, min_price=0.5, max_price=20, min_pct=1, format_name=None)

    assert df.empty


def test_excludes_baseline_prices_below_min_price_bulk_noise(con):
    _seed_card(con, 1, "Bulk Card", 16)
    _seed_price(con, 1, date(2026, 1, 1), 0.05)
    _seed_price(con, 1, date(2026, 1, 8), 5.00)  # huge % move, but starts as bulk-card noise

    df = find_spikes(con, window_days=7, min_price=1.0, max_price=20, min_pct=1, format_name=None)

    assert df.empty


def test_filters_by_format(con, monkeypatch):
    monkeypatch.setattr(spikes, "FORMATS", {"testfmt": {16}})

    _seed_card(con, 1, "In Format", 16)
    _seed_price(con, 1, date(2026, 1, 1), 1.00)
    _seed_price(con, 1, date(2026, 1, 8), 2.00)

    _seed_card(con, 2, "Out Of Format", 999)
    _seed_price(con, 2, date(2026, 1, 1), 1.00)
    _seed_price(con, 2, date(2026, 1, 8), 2.00)

    df = find_spikes(con, window_days=7, min_price=0.5, max_price=10, min_pct=1, format_name="testfmt")

    assert len(df) == 1
    assert df.iloc[0]["cm_id"] == 1


def test_find_price_gaps_computes_gap_pct(con):
    _seed_card(con, 1, "Drying Up Card", 16)
    _seed_low_trend(con, 1, date(2026, 1, 1), low=2.20, trend=2.00)  # low is 10% above trend

    df = find_price_gaps(con, min_price=0.5, max_price=10, min_gap_pct=5, min_gap_abs=0.1, format_name=None)

    assert len(df) == 1
    row = df.iloc[0]
    assert row["low"] == 2.20
    assert row["trend"] == 2.00
    assert row["gap_pct"] == pytest.approx(10.0)


def test_find_price_gaps_excludes_below_min_gap_pct(con):
    _seed_card(con, 1, "Barely Above Trend", 16)
    _seed_low_trend(con, 1, date(2026, 1, 1), low=2.02, trend=2.00)  # +1%, below a 10% threshold

    df = find_price_gaps(con, min_price=0.5, max_price=10, min_gap_pct=10, min_gap_abs=0.1, format_name=None)

    assert df.empty


def test_find_price_gaps_excludes_below_min_gap_abs(con):
    """A big % gap on a near-free card is still noise - the absolute euro floor exists for this."""

    _seed_card(con, 1, "Bulk Card", 16)
    _seed_low_trend(con, 1, date(2026, 1, 1), low=0.06, trend=0.05)  # +20%, but only 1 cent

    df = find_price_gaps(con, min_price=0.01, max_price=10, min_gap_pct=10, min_gap_abs=0.20, format_name=None)

    assert df.empty


def test_find_price_gaps_excludes_near_zero_trend_even_with_expensive_low(con):
    """Real bug found against Pioneer data: trend=0.02, low=16.00 reported as a "+79900%" gap.

    `low` was already floored at min_price; `trend` only required `> 0`, so a
    near-worthless variant with one oddly-priced listing slipped through as a
    huge, meaningless percentage. trend must clear min_price too now.
    """

    _seed_card(con, 1, "Near-Worthless Variant", 16)
    _seed_low_trend(con, 1, date(2026, 1, 1), low=16.00, trend=0.02)

    df = find_price_gaps(con, min_price=1.0, max_price=20, min_gap_pct=10, min_gap_abs=0.20, format_name=None)

    assert df.empty


def test_find_price_gaps_ignores_low_at_or_below_trend(con):
    _seed_card(con, 1, "Stable Card", 16)
    _seed_low_trend(con, 1, date(2026, 1, 1), low=1.90, trend=2.00)  # low below trend, no gap

    df = find_price_gaps(con, min_price=0.5, max_price=10, min_gap_pct=1, min_gap_abs=0.01, format_name=None)

    assert df.empty


def test_find_price_gaps_filters_by_format(con, monkeypatch):
    monkeypatch.setattr(spikes, "FORMATS", {"testfmt": {16}})

    _seed_card(con, 1, "In Format", 16)
    _seed_low_trend(con, 1, date(2026, 1, 1), low=2.20, trend=2.00)

    _seed_card(con, 2, "Out Of Format", 999)
    _seed_low_trend(con, 2, date(2026, 1, 1), low=2.20, trend=2.00)

    df = find_price_gaps(con, min_price=0.5, max_price=10, min_gap_pct=5, min_gap_abs=0.1, format_name="testfmt")

    assert len(df) == 1
    assert df.iloc[0]["cm_id"] == 1


def test_print_spikes_outputs_the_card_and_change(con, capsys):
    _seed_card(con, 1, "Rising Card", 16)
    _seed_price(con, 1, date(2026, 1, 1), 1.00)
    _seed_price(con, 1, date(2026, 1, 8), 2.00)

    df = find_spikes(con, window_days=7, min_price=0.5, max_price=10, min_pct=1, format_name=None)
    print_spikes(df)

    captured = capsys.readouterr()
    assert "Rising Card" in captured.out
    assert "+100.0%" in captured.out


def test_print_spikes_handles_empty_result(capsys):
    print_spikes(pd.DataFrame())

    captured = capsys.readouterr()
    assert "No spiking cards found" in captured.out


def test_print_price_gaps_outputs_the_card_and_gap(con, capsys):
    _seed_card(con, 1, "Drying Up Card", 16)
    _seed_low_trend(con, 1, date(2026, 1, 1), low=2.20, trend=2.00)

    df = find_price_gaps(con, min_price=0.5, max_price=10, min_gap_pct=5, min_gap_abs=0.1, format_name=None)
    print_price_gaps(df)

    captured = capsys.readouterr()
    assert "Drying Up Card" in captured.out
    assert "+10.0%" in captured.out


def test_print_price_gaps_handles_empty_result(capsys):
    print_price_gaps(pd.DataFrame())

    captured = capsys.readouterr()
    assert "No cards found with a low/trend gap" in captured.out
