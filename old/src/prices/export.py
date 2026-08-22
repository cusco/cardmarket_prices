import time

import gspread
import pandas as pd
from django.conf import settings
from django.db.models import Min
from django.utils import timezone
from gspread import GSpreadException
from gspread.exceptions import APIError

from prices.constants import LEGAL_PREMODERN_SETS
from prices.models import Catalog, MTGCard, MTGCardPrice

MAX_HISTORICAL_ENTRIES = 60  # pricing columns
TOP_N_COUNT = 800  # rows

EXCLUDED_EXPANSION_IDS = [
    110,  # Oversized 6x9 Promos
    111,  # Oversized Box Toppers
]

BUFFER = 20


def _get_cheapest_premodern_prints(price_field, cur_date):
    """Find the cheapest print for queryset premodern cards."""
    pm_metacard_ids = (
        MTGCard.objects.filter(expansion_id__in=LEGAL_PREMODERN_SETS)
        .exclude(expansion_id__in=EXCLUDED_EXPANSION_IDS)
        .values_list("metacard_id", flat=True)
        .distinct()
    )

    floors_qs = (
        MTGCardPrice.objects.filter(
            card__metacard_id__in=pm_metacard_ids, catalog_date=cur_date, **{f"{price_field}__gt": 0.01}
        )
        .exclude(card__expansion_id__in=EXCLUDED_EXPANSION_IDS)
        .values("card__metacard_id")
        .annotate(min_price=Min(price_field))
        .order_by("-min_price", "card__metacard_id")[:TOP_N_COUNT]
    )

    target_pairs = {item["card__metacard_id"]: item["min_price"] for item in floors_qs}
    if not target_pairs:
        return [], {}

    candidates = (
        MTGCardPrice.objects.filter(
            card__metacard_id__in=list(target_pairs.keys()),
            catalog_date=cur_date,
            **{f"{price_field}__in": list(target_pairs.values())},
        )
        .select_related("card", "card__expansion")
        .values("card_id", "card__metacard_id", "card__name", "card__expansion__name", price_field)
        .order_by("card__metacard_id", "card_id")
    )

    cheapest_pks = []
    card_metadata = {}
    seen_metacards = set()

    for item in candidates:
        m_id = item["card__metacard_id"]
        c_id = item["card_id"]
        price = item[price_field]

        if target_pairs.get(m_id) == price:
            if m_id not in seen_metacards and len(cheapest_pks) < TOP_N_COUNT:
                seen_metacards.add(m_id)
                card_metadata[c_id] = {"name": item["card__name"], "set_name": item["card__expansion__name"]}
                cheapest_pks.append(c_id)

    return cheapest_pks, card_metadata


def _build_pivot_dataframe(df, card_metadata, price_field):
    """Transform a structured historical dataframe into a pivot table."""
    if df.empty:
        return None

    # Map deterministic metadata
    df["card_name"] = df["card_id"].map(lambda x: card_metadata.get(x, {}).get("name", "Unknown"))
    df["set_name"] = df["card_id"].map(lambda x: card_metadata.get(x, {}).get("set_name", "Unknown"))
    df["date_only"] = pd.to_datetime(df["catalog_date"]).dt.date

    pivot_df = df.pivot_table(
        index=["set_name", "card_name"],
        columns="date_only",
        values=price_field,
        aggfunc="mean",
    )

    pivot_df = pivot_df.reindex(sorted(pivot_df.columns, reverse=True), axis=1)
    pivot_df = pivot_df.iloc[:, :MAX_HISTORICAL_ENTRIES]

    if not pivot_df.empty:
        newest_col = pivot_df.columns[0]
        pivot_df = pivot_df.sort_values(by=newest_col, ascending=False)

    pivot_df.columns = [c.strftime("%Y-%m-%d") if hasattr(c, "strftime") else c for c in pivot_df.columns]
    return pivot_df.reset_index().fillna("")


def _upload_to_gsheets(final_df, cur_date, price_field, recent_dates_count, start_time):
    """Handle Google Sheets updates and calculate total end-to-end execution runtime."""
    try:
        gdrive_client = gspread.service_account(filename=settings.GOOGLE_SECRET_CREDENTIALS)
        sheet = gdrive_client.open_by_key("1vQs3vlXHu7BELFoVuK4ysfzMeYHDxMdOWgPAGmPVZjk")

        # 1. Main Data Sheet Upload
        ws_pm_bulk = sheet.worksheet("premodern_bulk")
        ws_pm_bulk.clear()
        ws_pm_bulk.update([final_df.columns.values.tolist()] + final_df.values.tolist())

        # 2. Stop the timer HERE after the upload finishes to get true total duration
        elapsed_seconds = time.perf_counter() - start_time
        mins, secs = divmod(elapsed_seconds, 60)
        runtime_str = f"{int(mins)}m {int(secs)}s" if mins > 0 else f"{secs:.2f} seconds"

        # 3. Status Sheet Update
        ws_status = sheet.worksheet("status")
        ws_status.clear()
        local_now = timezone.localtime(timezone.now())
        ws_status.update(
            [
                ["Metric", "Value"],
                ["Last script run", local_now.strftime("%Y-%m-%d %H:%M:%S")],
                ["Total Export Runtime", runtime_str],  # Total end-to-end execution time
                ["Latest pricing date", cur_date.strftime("%Y-%m-%d %H:%M:%S")],
                ["Price Metric Tracked", price_field.upper()],
                ["Data Window", f"{recent_dates_count} entries"],
                ["Total Cards Tracked", len(final_df)],
            ]
        )
        return f"Success: {len(final_df)} cards uploaded using {price_field} in {runtime_str}."
    except (GSpreadException, APIError) as err:
        return f"Google Sheets Error: {str(err)}"
    except OSError as err:
        return f"File System Error: {str(err)}"


def export_top_cards_to_gdrive():
    """Find the cheapest printing per metacard and track its 60-day history."""
    start_time = time.perf_counter()  # Start the timer at the absolute beginning
    price_field = getattr(settings, "PRICE_FIELD", "trend")

    # 1. Identify the latest price date
    latest_cat = Catalog.objects.filter(catalog_id=Catalog.MTG, catalog_type=Catalog.PRICES).latest("catalog_date")
    cur_date = latest_cat.catalog_date

    # 2. Fetch the top print IDs and metadata
    cheapest_pks, card_metadata = _get_cheapest_premodern_prints(price_field, cur_date)

    if not cheapest_pks:
        return "Error: No matching Premodern cards found."

    # 3. Fetch history targeting only those specific versions
    recent_catalog_dates = list(
        Catalog.objects.filter(catalog_id=Catalog.MTG, catalog_type=Catalog.PRICES)
        .order_by("-catalog_date")
        .values_list("catalog_date", flat=True)[: MAX_HISTORICAL_ENTRIES + BUFFER]
    )

    history_qs = (
        MTGCardPrice.objects.filter(
            catalog_date__in=recent_catalog_dates, card_id__in=cheapest_pks, **{f"{price_field}__gt": 0.01}
        )
        .order_by("card_id", "catalog_date")
        .values_list("card_id", "catalog_date", price_field)
    )

    history_df = pd.DataFrame(list(history_qs), columns=["card_id", "catalog_date", price_field])

    # 4. Transform using Pandas
    final_df = _build_pivot_dataframe(history_df, card_metadata, price_field)
    if final_df is None:
        return "Error: No history data found."

    # 5. Upload via an isolated helper and pass down the initial start timestamp
    return _upload_to_gsheets(final_df, cur_date, price_field, len(recent_catalog_dates), start_time)
