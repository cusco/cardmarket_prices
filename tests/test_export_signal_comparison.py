from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

import export_premodern_bulk
import export_signal_comparison as comparison
from export_signal_comparison import (
    _apply_formatting,
    _build_comparison_table,
    _clear_conditional_formats,
    _compute_signals,
    _conditional_format_requests,
    _despike,
    _header_color_for_column,
    _header_format_requests,
    _linear_regression_pct,
    _macd_pct,
    _momentum_label,
    _pct_change_1d,
    _roc,
    export_signal_comparison_to_gdrive,
)


def _history(cm_id, prices, start=date(2026, 1, 1)):
    """A per-card price-history frame: one row per day starting at `start`, in the shape _price_history returns."""

    dates = [pd.Timestamp(start) + pd.Timedelta(days=i) for i in range(len(prices))]
    return pd.DataFrame({"cm_id": cm_id, "catalog_date": dates, "price": prices})


def _seed_product(con, cm_id, name, expansion_id, metacard_id):
    con.execute(
        "INSERT INTO products (cm_id, name, expansion_id, metacard_id) VALUES (?, ?, ?, ?)",
        [cm_id, name, expansion_id, metacard_id],
    )


def _seed_price(con, cm_id, catalog_date, trend):
    con.execute(
        "INSERT INTO prices (cm_id, catalog_date, trend) VALUES (?, ?, ?)",
        [cm_id, catalog_date, trend],
    )


# ---- _despike ----


def test_despike_rejects_a_single_day_glitch():
    # Lord of Atlantis-shaped: flat around 7, one single-day spike to 130, no follow-through
    history = _history(1, [7.0] * 10 + [130.0])

    result = _despike(history)

    assert result["price"].iloc[-1] == 7.0  # median of the trailing 3 days [7, 7, 130] rejects the spike


def test_despike_keeps_a_move_that_holds_for_two_days():
    # a real move: two consecutive days at the new, higher level
    history = _history(1, [7.0] * 9 + [20.0, 20.0])

    result = _despike(history)

    assert result["price"].iloc[-1] == 20.0  # median of [7, 20, 20] tracks the sustained move


# ---- _pct_change_1d ----


def test_pct_change_1d_uses_the_raw_unfiltered_price():
    history = _history(1, [7.0] * 10 + [130.0])  # same glitch _despike would reject

    result = _pct_change_1d(history)

    assert result == pytest.approx((130.0 - 7.0) / 7.0 * 100)  # the glitch stays visible here, by design


def test_pct_change_1d_returns_nan_with_fewer_than_two_points():
    history = _history(1, [1.0])

    result = _pct_change_1d(history)

    assert np.isnan(result)


# ---- _roc ----


def test_roc_compares_today_to_the_closest_snapshot_at_or_before_the_window():
    history = _history(1, [1.0] * 7 + [2.0])  # flat at 1.00 for 7 days (day 0-6), then today (day 7) jumps to 2.00

    result = _roc(history, window_days=7)

    assert result == pytest.approx(100.0)


def test_roc_returns_nan_without_enough_history():
    history = _history(1, [1.0, 2.0])  # only 2 days, nothing 7 days back

    result = _roc(history, window_days=7)

    assert np.isnan(result)


# ---- _linear_regression_pct ----


def test_linear_regression_pct_reflects_a_steady_rise():
    # +0.10/day for 8 days (day 0..7): a clean, noise-free trend
    history = _history(1, [1.0 + 0.10 * i for i in range(8)])

    result = _linear_regression_pct(history, window_days=7)

    # slope=0.10/day, mean_price ~= 1.35, projected over 7 days: 0.10*7/1.35*100 ~= 51.85%
    assert result == pytest.approx(0.10 * 7 / pd.Series([1.0 + 0.10 * i for i in range(8)]).mean() * 100)


def test_linear_regression_pct_is_less_swayed_by_one_noisy_endpoint_than_roc():
    # steady rise, but the very last day spikes hard - a single noisy endpoint
    prices = [1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 5.0]
    history = _history(1, prices)

    roc = _roc(history, window_days=7)
    regression = _linear_regression_pct(history, window_days=7)

    assert roc == pytest.approx((5.0 - 1.0) / 1.0 * 100)
    assert regression < roc  # the trend line doesn't chase the single spike as hard as the raw endpoint diff does


def test_linear_regression_pct_returns_nan_with_fewer_than_two_points():
    history = _history(1, [1.0])

    result = _linear_regression_pct(history, window_days=7)

    assert np.isnan(result)


# ---- _macd_pct ----


def test_macd_pct_is_positive_for_a_sustained_uptrend():
    prices = [1.0 + 0.02 * i for i in range(40)]  # 40 days of steady gains, well past MACD_SLOW_SPAN
    history = _history(1, prices)

    result = _macd_pct(history, window_days=60)

    assert result > 0  # fast EMA (reacts quicker) should sit above the slower-moving slow EMA


def test_macd_pct_returns_nan_without_enough_history_in_the_window():
    prices = [1.0 + 0.02 * i for i in range(40)]  # plenty of history overall...
    history = _history(1, prices)

    result = _macd_pct(history, window_days=7)  # ...but restricted to 7 days, well under MACD_SLOW_SPAN (26)

    assert np.isnan(result)


# ---- _compute_signals / _build_comparison_table ----


def test_compute_signals_returns_one_row_per_card_with_a_column_group_per_window_plus_extras():
    history_df = pd.concat([_history(1, [1.0] * 61), _history(2, [2.0] * 61)], ignore_index=True)

    result = _compute_signals(history_df)

    assert sorted(result["cm_id"].tolist()) == [1, 2]
    expected_columns = {
        "cm_id",
        "pct_change_1d",
        "Linear_regression_avg_long_calc",
        "Linear_regression_avg_short_calc",
        "momentum_label",
    } | {name for w in comparison.COMPARISON_WINDOWS_DAYS for name in comparison._signal_column_names(w)}
    assert set(result.columns) == expected_columns


def test_compute_signals_despikes_before_computing_the_windowed_signals():
    # flat at 7 for 60 days, then a single-day glitch to 130 - the windowed signals should not see the spike at all
    history_df = _history(1, [7.0] * 60 + [130.0])

    result = _compute_signals(history_df)

    assert result.loc[0, "pct_change_1d"] == pytest.approx((130.0 - 7.0) / 7.0 * 100)  # glitch visible here...
    assert result.loc[0, "ROC_calc_3d"] == pytest.approx(0.0)  # ...but not here
    assert result.loc[0, "ROC_calc_60d"] == pytest.approx(0.0)


def test_compute_signals_averages_are_the_mean_of_their_window_group():
    history_df = _history(1, [1.0 + 0.05 * i for i in range(61)])  # steady rise, easy to hand-verify

    result = _compute_signals(history_df)

    long_cols = [f"Linear_regression_calc_{w}d" for w in comparison.LONG_AVG_WINDOWS_DAYS]
    short_cols = [f"Linear_regression_calc_{w}d" for w in comparison.SHORT_AVG_WINDOWS_DAYS]
    assert result.loc[0, "Linear_regression_avg_long_calc"] == pytest.approx(result.loc[0, long_cols].mean())
    assert result.loc[0, "Linear_regression_avg_short_calc"] == pytest.approx(result.loc[0, short_cols].mean())


def test_build_comparison_table_labels_orders_by_floor_price_and_adds_the_price_date(monkeypatch):
    monkeypatch.setattr(comparison, "SETS", {10: {"name": "Premodern Set"}})
    monkeypatch.setattr(comparison, "COMPARISON_WINDOWS_DAYS", (7,))

    cards_df = pd.DataFrame(
        [
            {"cm_id": 1, "name": "Cheap Card", "expansion_id": 10, "floor_price": 2.0},
            {"cm_id": 2, "name": "Expensive Card", "expansion_id": 10, "floor_price": 20.0},
        ]
    )
    signals_df = pd.DataFrame(
        [
            {
                "cm_id": 1,
                "pct_change_1d": 0.5,
                "ROC_calc_7d": 1.234,
                "Linear_regression_calc_7d": 2.345,
                "MACD_calc_7d": np.nan,
                "Linear_regression_avg_long_calc": 1.1,
                "Linear_regression_avg_short_calc": 1.2,
                "momentum_label": "Accelerating",
            },
            {
                "cm_id": 2,
                "pct_change_1d": 0.6,
                "ROC_calc_7d": 3.456,
                "Linear_regression_calc_7d": 4.567,
                "MACD_calc_7d": 5.678,
                "Linear_regression_avg_long_calc": 2.1,
                "Linear_regression_avg_short_calc": 2.2,
                "momentum_label": "Decelerating",
            },
        ]
    )

    table = _build_comparison_table(cards_df, signals_df, latest_price_date=date(2026, 1, 8))

    assert table["card_name"].tolist() == ["Expensive Card", "Cheap Card"]
    assert table["set_name"].tolist() == ["Premodern Set", "Premodern Set"]
    assert table["latest_price_date"].tolist() == ["2026-01-08", "2026-01-08"]
    assert table.loc[0, "ROC_calc_7d"] == 3.46
    assert table.loc[1, "MACD_calc_7d"] == ""  # NaN rendered as blank for the sheet
    assert table.loc[0, "momentum_label"] == "Decelerating"  # row 0 is the Expensive Card (cm_id 2) after sorting
    assert table.loc[1, "momentum_label"] == "Accelerating"


# ---- _momentum_label ----


def test_momentum_label_turning_up_when_long_term_flat_but_short_term_rising():
    assert _momentum_label(long_avg_calc=0.0, short_avg_calc=5.0) == "Turning up"
    assert _momentum_label(long_avg_calc=-2.0, short_avg_calc=1.0) == "Turning up"


def test_momentum_label_accelerating_when_short_term_outpaces_a_rising_long_term():
    assert _momentum_label(long_avg_calc=5.0, short_avg_calc=10.0) == "Accelerating"


def test_momentum_label_decelerating_when_still_rising_but_slower_than_long_term():
    assert _momentum_label(long_avg_calc=10.0, short_avg_calc=3.0) == "Decelerating"


def test_momentum_label_reversing_when_long_term_rise_has_now_flipped_negative():
    assert _momentum_label(long_avg_calc=10.0, short_avg_calc=-1.0) == "Reversing"
    assert _momentum_label(long_avg_calc=10.0, short_avg_calc=0.0) == "Reversing"


def test_momentum_label_flat_falling_when_neither_window_is_rising():
    assert _momentum_label(long_avg_calc=0.0, short_avg_calc=0.0) == "Flat/falling"
    assert _momentum_label(long_avg_calc=-5.0, short_avg_calc=-1.0) == "Flat/falling"


def test_momentum_label_blank_when_either_side_is_missing():
    assert _momentum_label(long_avg_calc=np.nan, short_avg_calc=5.0) == ""
    assert _momentum_label(long_avg_calc=5.0, short_avg_calc=np.nan) == ""


# ---- export_signal_comparison_to_gdrive (end to end, upload mocked out) ----


def test_export_signal_comparison_to_gdrive_uploads_the_table(con, monkeypatch):
    monkeypatch.setattr(export_premodern_bulk, "FORMATS", {"premodern": {10}})
    monkeypatch.setattr(comparison, "SETS", {10: {"name": "Premodern Set"}})

    _seed_product(con, 1, "Rising Card", 10, 100)
    start_day = date(2026, 1, 1)
    for i in range(61):
        _seed_price(con, 1, start_day + timedelta(days=i), 1.0 + 0.10 * i)

    captured = {}

    def fake_upload(table, start):
        captured["table"] = table
        return "Success: mocked upload."

    monkeypatch.setattr(comparison, "_upload_to_sheet", fake_upload)

    result = export_signal_comparison_to_gdrive(con=con)

    assert result == "Success: mocked upload."
    assert captured["table"]["card_name"].tolist() == ["Rising Card"]
    assert captured["table"]["latest_price_date"].iloc[0] == str(start_day + timedelta(days=60))
    assert captured["table"]["ROC_calc_7d"].iloc[0] > 0
    assert captured["table"]["MACD_calc_7d"].iloc[0] == ""  # 7-day window is nowhere near MACD's warm-up
    assert captured["table"]["MACD_calc_60d"].iloc[0] != ""  # 61 days is enough for the 60d column to compute


def test_export_signal_comparison_to_gdrive_reports_when_there_is_no_price_data_yet(con, monkeypatch):
    monkeypatch.setattr(export_premodern_bulk, "FORMATS", {"premodern": {10}})

    result = export_signal_comparison_to_gdrive(con=con)

    assert result == "Nothing to compare: no price data yet."


# ---- sheet formatting ----


def test_header_color_for_column_groups_by_window():
    assert _header_color_for_column("ROC_calc_7d") == comparison.WINDOW_HEADER_COLORS[7]
    assert _header_color_for_column("MACD_calc_60d") == comparison.WINDOW_HEADER_COLORS[60]


def test_header_color_for_column_uses_headline_color_for_the_summary_columns():
    assert _header_color_for_column("momentum_label") == comparison.HEADLINE_HEADER_COLOR
    assert _header_color_for_column("Linear_regression_avg_long_calc") == comparison.HEADLINE_HEADER_COLOR
    assert _header_color_for_column("Linear_regression_avg_short_calc") == comparison.HEADLINE_HEADER_COLOR


def test_header_color_for_column_uses_neutral_for_everything_else():
    assert _header_color_for_column("card_name") == comparison.NEUTRAL_HEADER_COLOR
    assert _header_color_for_column("floor_price") == comparison.NEUTRAL_HEADER_COLOR


def test_header_format_requests_covers_every_column_and_freezes_the_header_row():
    table = pd.DataFrame(columns=["card_name", "ROC_calc_7d", "momentum_label"])

    requests = _header_format_requests(sheet_id=99, table=table)

    repeat_cell_requests = [r for r in requests if "repeatCell" in r]
    assert len(repeat_cell_requests) == len(table.columns)
    assert any("updateSheetProperties" in r for r in requests)


def test_conditional_format_requests_skips_non_metric_columns():
    table = pd.DataFrame(
        {
            "set_name": ["s"],
            "card_name": ["c"],
            "momentum_label": ["Turning up"],
            "latest_price_date": ["2026-01-01"],
            "floor_price": [5.0],
            "ROC_calc_7d": [1.0],
        }
    )

    requests = _conditional_format_requests(sheet_id=99, table=table)

    gradient_requests = [r for r in requests if "gradientRule" in r["addConditionalFormatRule"]["rule"]]
    gradient_column_indexes = {
        r["addConditionalFormatRule"]["rule"]["ranges"][0]["startColumnIndex"] for r in gradient_requests
    }
    assert gradient_column_indexes == {table.columns.get_loc("ROC_calc_7d")}


def test_conditional_format_requests_has_one_boolean_rule_per_momentum_label():
    table = pd.DataFrame({"card_name": ["c"], "momentum_label": ["Turning up"], "ROC_calc_7d": [1.0]})

    requests = _conditional_format_requests(sheet_id=99, table=table)

    boolean_requests = [r for r in requests if "booleanRule" in r["addConditionalFormatRule"]["rule"]]
    assert len(boolean_requests) == len(comparison.MOMENTUM_LABEL_COLORS)


def test_conditional_format_requests_indexes_are_sequential_with_no_gaps():
    table = pd.DataFrame({"card_name": ["c"], "momentum_label": ["Turning up"], "ROC_calc_7d": [1.0]})

    requests = _conditional_format_requests(sheet_id=99, table=table)

    indexes = [r["addConditionalFormatRule"]["index"] for r in requests]
    assert indexes == list(range(len(requests)))


def test_clear_conditional_formats_builds_one_delete_per_existing_rule_in_reverse_order():
    class FakeSpreadsheet:
        def fetch_sheet_metadata(self):
            return {"sheets": [{"properties": {"sheetId": 42}, "conditionalFormats": [{}, {}, {}]}]}

    class FakeWorksheet:
        id = 42
        spreadsheet = FakeSpreadsheet()

    requests = _clear_conditional_formats(FakeWorksheet())

    indexes = [r["deleteConditionalFormatRule"]["index"] for r in requests]
    assert indexes == [2, 1, 0]  # deleting highest-index first keeps remaining indexes valid mid-deletion


def test_clear_conditional_formats_is_empty_when_the_sheet_has_no_existing_rules():
    class FakeSpreadsheet:
        def fetch_sheet_metadata(self):
            return {"sheets": [{"properties": {"sheetId": 42}}]}

    class FakeWorksheet:
        id = 42
        spreadsheet = FakeSpreadsheet()

    assert _clear_conditional_formats(FakeWorksheet()) == []


def test_apply_formatting_clears_old_rules_before_sending_new_ones():
    calls = []

    class FakeSpreadsheet:
        def fetch_sheet_metadata(self):
            return {"sheets": [{"properties": {"sheetId": 42}, "conditionalFormats": [{}]}]}

        def batch_update(self, body):
            calls.append(body)

    class FakeWorksheet:
        id = 42
        spreadsheet = FakeSpreadsheet()

    table = pd.DataFrame({"card_name": ["c"], "momentum_label": ["Turning up"], "ROC_calc_7d": [1.0]})
    _apply_formatting(FakeWorksheet(), table)

    assert len(calls) == 2  # one batch to clear the old rule, one to apply header + fresh conditional formatting
    assert calls[0]["requests"][0]["deleteConditionalFormatRule"]["index"] == 0
    assert any("repeatCell" in r for r in calls[1]["requests"])
