from datetime import date

import pandas as pd

import export_premodern_bulk as bulk
from export_premodern_bulk import (
    _add_card_labels,
    _cheapest_print_per_card,
    _most_expensive_cards,
    _pivot_history,
    _price_history,
    _recent_catalog_dates,
    export_premodern_bulk_to_gdrive,
)


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


# ---- _cheapest_print_per_card ----


def test_cheapest_print_per_card_keeps_the_lowest_priced_printing():
    products_df = pd.DataFrame(
        [
            {"cm_id": 1, "name": "Card A Foil", "expansion_id": 10, "metacard_id": 100},
            {"cm_id": 2, "name": "Card A Normal", "expansion_id": 10, "metacard_id": 100},
        ]
    )
    prices_df = pd.DataFrame([{"cm_id": 1, "price": 5.0}, {"cm_id": 2, "price": 3.0}])

    result = _cheapest_print_per_card(products_df, prices_df)

    assert len(result) == 1
    assert result.iloc[0]["cm_id"] == 2
    assert result.iloc[0]["floor_price"] == 3.0


def test_cheapest_print_per_card_breaks_ties_by_lowest_cm_id():
    products_df = pd.DataFrame(
        [
            {"cm_id": 5, "name": "Card A Foil", "expansion_id": 10, "metacard_id": 100},
            {"cm_id": 2, "name": "Card A Normal", "expansion_id": 10, "metacard_id": 100},
        ]
    )
    prices_df = pd.DataFrame([{"cm_id": 5, "price": 4.0}, {"cm_id": 2, "price": 4.0}])

    result = _cheapest_print_per_card(products_df, prices_df)

    assert len(result) == 1
    assert result.iloc[0]["cm_id"] == 2


def test_cheapest_print_per_card_handles_no_matching_prices():
    products_df = pd.DataFrame([{"cm_id": 1, "name": "Card A", "expansion_id": 10, "metacard_id": 100}])
    prices_df = pd.DataFrame(columns=["cm_id", "price"])

    result = _cheapest_print_per_card(products_df, prices_df)

    assert result.empty


# ---- _most_expensive_cards ----


def test_most_expensive_cards_orders_by_floor_price_descending_and_truncates():
    cheapest_df = pd.DataFrame(
        [
            {"cm_id": 1, "floor_price": 10.0},
            {"cm_id": 2, "floor_price": 30.0},
            {"cm_id": 3, "floor_price": 20.0},
        ]
    )

    result = _most_expensive_cards(cheapest_df, top_n=2)

    assert result["cm_id"].tolist() == [2, 3]


# ---- _recent_catalog_dates / _price_history (need the real DuckDB schema) ----


def test_recent_catalog_dates_returns_newest_first_within_the_requested_window(con):
    _seed_product(con, 1, "Card A", 10, 100)
    for day in range(1, 6):
        _seed_price(con, 1, date(2026, 1, day), 1.0)

    result = _recent_catalog_dates(con, days=2, buffer_days=1)

    assert result == [date(2026, 1, 5), date(2026, 1, 4), date(2026, 1, 3)]


def test_price_history_excludes_dates_before_the_threshold_and_untracked_prices(con):
    _seed_product(con, 1, "Card A", 10, 100)
    _seed_price(con, 1, date(2026, 1, 1), 1.0)  # before threshold
    _seed_price(con, 1, date(2026, 1, 2), 0.0)  # untracked, filtered by > 0.01
    _seed_price(con, 1, date(2026, 1, 3), 2.0)

    result = _price_history(con, cm_ids=[1], since_date=date(2026, 1, 2), price_field="trend")

    assert pd.to_datetime(result["catalog_date"]).dt.date.tolist() == [date(2026, 1, 3)]
    assert result["price"].tolist() == [2.0]


# ---- _add_card_labels / _pivot_history ----


def test_add_card_labels_and_pivot_history_shape_the_sheet(monkeypatch):
    monkeypatch.setattr(bulk, "SETS", {10: {"name": "Premodern Set"}})

    cards_df = pd.DataFrame([{"cm_id": 1, "name": "Card A", "expansion_id": 10, "metacard_id": 100}])
    history_df = pd.DataFrame(
        [
            {"cm_id": 1, "catalog_date": date(2026, 1, 1), "price": 1.0},
            {"cm_id": 1, "catalog_date": date(2026, 1, 2), "price": 2.0},
        ]
    )

    labeled_df = _add_card_labels(history_df, cards_df)
    final_df = _pivot_history(labeled_df, max_days=60)

    assert final_df["set_name"].tolist() == ["Premodern Set"]
    assert final_df["card_name"].tolist() == ["Card A"]
    # newest date is the first price column, after the two label columns
    assert final_df.columns[2] == "2026-01-02"
    assert final_df.iloc[0]["2026-01-02"] == 2.0


# ---- export_premodern_bulk_to_gdrive (end to end, upload mocked out) ----


def test_export_premodern_bulk_to_gdrive_uploads_the_top_card(con, monkeypatch):
    monkeypatch.setattr(bulk, "FORMATS", {"premodern": {10}})
    monkeypatch.setattr(bulk, "SETS", {10: {"name": "Premodern Set"}})

    _seed_product(con, 1, "Cheap Card", 10, 100)
    _seed_product(con, 2, "Expensive Card", 10, 200)
    for cm_id, price in ((1, 2.0), (2, 20.0)):
        _seed_price(con, cm_id, date(2026, 1, 1), price)

    captured = {}

    def fake_upload(final_df, cur_date, price_field, recent_dates_count, start):
        captured["final_df"] = final_df
        captured["cur_date"] = cur_date
        captured["price_field"] = price_field
        captured["recent_dates_count"] = recent_dates_count
        return "Success: mocked upload."

    monkeypatch.setattr(bulk, "_upload_to_sheets", fake_upload)

    result = export_premodern_bulk_to_gdrive(con=con)

    assert result == "Success: mocked upload."
    assert captured["cur_date"] == date(2026, 1, 1)
    assert captured["price_field"] == "trend"
    # both cards are the "cheapest printing" of their own metacard, so both should be present, priciest first
    assert captured["final_df"]["card_name"].tolist() == ["Expensive Card", "Cheap Card"]


def test_export_premodern_bulk_to_gdrive_reports_when_there_is_no_price_data_yet(con, monkeypatch):
    monkeypatch.setattr(bulk, "FORMATS", {"premodern": {10}})

    result = export_premodern_bulk_to_gdrive(con=con)

    assert result == "Nothing to export: no price data yet."


def test_export_premodern_bulk_to_gdrive_reports_when_no_premodern_cards_match(con, monkeypatch):
    monkeypatch.setattr(bulk, "FORMATS", {"premodern": {10}})

    _seed_product(con, 1, "Out Of Format Card", 999, 100)
    _seed_price(con, 1, date(2026, 1, 1), 5.0)

    result = export_premodern_bulk_to_gdrive(con=con)

    assert result == "Nothing to export: no matching Premodern cards found."
