import gspread
import pandas as pd
from django.conf import settings
from django.db.models import Min, Q
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


def export_top_cards_to_gdrive():
    """Find the cheapest printing per metacard and track its 60-day history."""
    price_field = getattr(settings, "PRICE_FIELD", "trend")

    # 1. Get Premodern legal metacard IDs
    pm_metacard_ids = (
        MTGCard.objects.filter(expansion_id__in=LEGAL_PREMODERN_SETS)
        .exclude(expansion_id__in=EXCLUDED_EXPANSION_IDS)
        .values_list("metacard_id", flat=True)
        .distinct()
    )

    # 2. Identify the latest price date
    latest_cat = Catalog.objects.filter(catalog_id=Catalog.MTG, catalog_type=Catalog.PRICES).latest("catalog_date")
    cur_date = latest_cat.catalog_date

    # 3. Find the "Floor Price" for every Metacard dynamically
    # tg: the cheapest print of a card
    floor_filter = {
        "card__metacard_id__in": pm_metacard_ids,
        "catalog_date": cur_date,
        f"{price_field}__gt": 0.01,
    }

    metacard_floors = list(
        MTGCardPrice.objects.filter(**floor_filter)
        .exclude(card__expansion_id__in=EXCLUDED_EXPANSION_IDS)
        .values("card__metacard_id")
        .annotate(cheapest_trend=Min(price_field))
        .order_by("-cheapest_trend")[:TOP_N_COUNT]
    )

    # 4. SURGICAL SELECTION: Identify the specific Card IDs for these floors
    metacard_filter = Q()
    for item in metacard_floors:
        metacard_kwargs = {
            "metacard_id": item["card__metacard_id"],
            f"prices__{price_field}": item["cheapest_trend"],
            "prices__catalog_date": cur_date,
        }
        metacard_filter |= Q(**metacard_kwargs)

    winning_prints_qs = (
        MTGCard.objects.filter(metacard_filter)
        .select_related("expansion")
        .values("cm_id", "metacard_id", "name", "expansion__name")
    )

    cheapest_pks = []
    card_metadata = {}
    for cp in winning_prints_qs:
        m_id = cp["metacard_id"]
        c_id = cp["cm_id"]

        if m_id not in card_metadata:
            card_metadata[c_id] = {"name": cp["name"], "set_name": cp["expansion__name"]}
            cheapest_pks.append(c_id)

    # 5. Fetch history ONLY for these specific versions
    recent_catalog_dates = list(
        Catalog.objects.filter(catalog_id=Catalog.MTG, catalog_type=Catalog.PRICES)
        .order_by("-catalog_date")
        .values_list("catalog_date", flat=True)[: MAX_HISTORICAL_ENTRIES + BUFFER]
    )

    history_filter = {
        "card_id__in": cheapest_pks,
        "catalog_date__in": recent_catalog_dates,
        f"{price_field}__gt": 0.01,
    }

    history_qs = MTGCardPrice.objects.filter(**history_filter).values("card_id", "catalog_date", price_field)

    # 6. Transform using Pandas (Robust version)
    df = pd.DataFrame(list(history_qs))

    if df.empty:
        return "Error: No history data found."

    # Map metadata and normalize dates to actual Python date objects
    df["card_name"] = df["card_id"].map(lambda x: card_metadata[x]["name"])
    df["set_name"] = df["card_id"].map(lambda x: card_metadata[x]["set_name"])
    df["date_only"] = pd.to_datetime(df["catalog_date"]).dt.date  # NOQA

    # Create Pivot Table: Using 'mean' to handle multiple scrapes per day
    pivot_df = df.pivot_table(
        index=["set_name", "card_name"],
        columns="date_only",
        values=price_field,
        aggfunc="mean",
    )

    # 1. Sort columns newest to oldest first
    pivot_df = pivot_df.reindex(sorted(pivot_df.columns, reverse=True), axis=1)

    # 2. THE TRIM: Take only the first X columns (the most recent ones)
    pivot_df = pivot_df.iloc[:, :MAX_HISTORICAL_ENTRIES]

    # 3. Sort Rows by price (using the actual newest column available)
    newest_col = pivot_df.columns[0]
    pivot_df = pivot_df.sort_values(by=newest_col, ascending=False)

    # 4. Format for Google Sheets
    pivot_df.columns = [c.strftime("%Y-%m-%d") for c in pivot_df.columns]
    final_df = pivot_df.reset_index().fillna("")

    # 7. Upload to Google Sheets
    try:
        gdrive_client = gspread.service_account(filename=settings.GOOGLE_SECRET_CREDENTIALS)
        sheet = gdrive_client.open_by_key("1vQs3vlXHu7BELFoVuK4ysfzMeYHDxMdOWgPAGmPVZjk")

        # Main Data Sheet
        ws_pm_bulk = sheet.worksheet("premodern_bulk")
        ws_pm_bulk.clear()
        ws_pm_bulk.update([final_df.columns.values.tolist()] + final_df.values.tolist())

        # Status Sheet
        ws_status = sheet.worksheet("status")
        ws_status.clear()
        local_now = timezone.localtime(timezone.now())
        ws_status.update(
            [
                ["Metric", "Value"],
                ["Last script run", local_now.strftime("%Y-%m-%d %H:%M:%S")],
                ["Latest pricing date", cur_date.strftime("%Y-%m-%d %H:%M:%S")],
                ["Price Metric Tracked", price_field.upper()],
                ["Data Window", f"{len(recent_catalog_dates)} entries"],
                ["Total Cards Tracked", len(final_df)],
            ]
        )

        return f"Success: {len(final_df)} cards uploaded using {price_field}."
    except (GSpreadException, APIError) as err:
        return f"Google Sheets Error: {str(err)}"
    except OSError as err:
        return f"File System Error: {str(err)}"
