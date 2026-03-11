import gspread
import pandas as pd
import pytz
from django.conf import settings
from django.db.models import Min, Q
from django.db.models.functions import TruncDate

from prices.constants import LEGAL_PREMODERN_SETS
from prices.models import Catalog, MTGCard, MTGCardPrice

germany_tz = pytz.timezone('Europe/Berlin')


def update_top_200_price_matrix():
    """
    Fetches the top 200 Premodern cards based on the lowest print price,
    tracks 30-day history, and updates Google Sheets.
    """

    # 1. Get the list of metacard_ids that are legal in Premodern
    # We define LEGAL_PREMODERN_SETS globally or import it
    pm_metacard_ids = (
        MTGCard.objects.filter(expansion_id__in=LEGAL_PREMODERN_SETS).values_list("metacard_id", flat=True).distinct()
    )

    # 2. Identify the latest price date from the Catalog
    latest_catalog = Catalog.objects.filter(catalog_id=Catalog.MTG, catalog_type=Catalog.PRICES).latest("catalog_date")
    cur_date = latest_catalog.catalog_date

    # 3. Find the top 200 Meta cards by their "Floor" price (the cheapest print)
    metacard_floors = (
        MTGCardPrice.objects.filter(catalog_date=cur_date, card__metacard_id__in=pm_metacard_ids, trend__gt=0.1)
        .values("card__metacard_id")
        .annotate(cheapest_trend=Min("trend"))
        .order_by("-cheapest_trend")[:200]
    )

    # Extract the IDs for the history query and for the detail lookup
    top_metacard_ids = [item["card__metacard_id"] for item in metacard_floors]

    # 4. Associate Meta cards with the SPECIFIC card that hit that low price
    # We build a filter for the exact (metacard_id + price) pairs found in Step 3
    lookup_filters = Q()
    for item in metacard_floors:
        lookup_filters |= Q(
            metacard_id=item["card__metacard_id"],
            prices__trend=item["cheapest_trend"],
            prices__catalog_date=cur_date,
        )

    top_200_details = {}
    # We order by release_date descending, so if prices are tied, we pick the newest set
    # Using expansion__release_date as a common MTG field name
    details_qs = (
        MTGCard.objects.filter(lookup_filters)
        .select_related("expansion")
        .order_by("metacard_id", "-expansion__release_date")
        .values("metacard_id", "name", "expansion__name")
    )

    for detail in details_qs:
        mid = detail["metacard_id"]
        if mid not in top_200_details:
            top_200_details[mid] = {
                "name": detail["name"],
                "set_name": detail["expansion__name"],
            }

    # 5. Fetch the last 60 unique entry dates available in the DB
    recent_dates = (
        Catalog.objects.filter(catalog_id=Catalog.MTG, catalog_type=Catalog.PRICES)
        .order_by("-catalog_date")
        .values_list("catalog_date", flat=True)[:60]
    )

    # Extract the date part and ensure they are unique (just in case)
    target_dates = sorted(list(set(d.date() for d in recent_dates)), reverse=True)

    min_date = min(target_dates)
    min_date = germany_tz.localize(min_date)

    history_qs = (
        MTGCardPrice.objects.filter(
            card__metacard_id__in=top_metacard_ids,
            catalog_date__gte=min_date,
            trend__gt=0.01,
        )
        .annotate(date_only=TruncDate("catalog_date"))
        .values("card__metacard_id", "date_only")
        .annotate(min_daily_trend=Min("trend"))
        .order_by("date_only")
    )

    # 6. Transform data using Pandas
    df = pd.DataFrame(list(history_qs))

    # Map the human-readable names and sets back to the IDs
    df["card_name"] = df["card__metacard_id"].map(lambda x: top_200_details[x]["name"])
    df["set_name"] = df["card__metacard_id"].map(lambda x: top_200_details[x]["set_name"])

    # Pivot the table: Rows are Cards, Columns are Dates
    pivot_df = df.pivot_table(index=["set_name", "card_name"], columns="date_only", values="min_daily_trend")

    # order by most expensive on the latest date
    pivot_df = pivot_df.sort_values(by=cur_date.date(), ascending=False)

    # Sort columns so the latest date is the first column after the name
    pivot_df = pivot_df.reindex(sorted(pivot_df.columns, reverse=True), axis=1)

    # Format dates to string (YYYY-MM-DD) for Google Sheets headers
    pivot_df.columns = [c.strftime("%Y-%m-%d") for c in pivot_df.columns]
    final_df = pivot_df.reset_index()

    # 7. Authentication and Upload
    try:
        gc = gspread.service_account(filename=settings.GOOGLE_SECRET_CREDENTIALS)
        sh = gc.open_by_key("1vQs3vlXHu7BELFoVuK4ysfzMeYHDxMdOWgPAGmPVZjk")
        try:
            worksheet = sh.worksheet("premodern_bulk")
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sh.add_worksheet(title="premodern_bulk", rows=1000, cols=40)

        # Convert DataFrame to a list of lists (Header included)
        data_to_upload = [final_df.columns.values.tolist()] + final_df.values.tolist()

        # Update the sheet
        worksheet.clear()
        worksheet.update(data_to_upload)

        # 8. status
        try:
            status_ws = sh.worksheet("status")
        except gspread.exceptions.WorksheetNotFound:
            status_ws = sh.add_worksheet(title="status", rows="20", cols="5")

        status_data = [
            ["Metric", "Value"],
            ["Last script run (UTC)", pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")],
            ["Latest pricing date", cur_date.strftime("%Y-%m-%d")],
            ["Oldest pricing date", target_dates[-1].strftime("%Y-%m-%d") if target_dates else "N/A"],
            ["Total Premodern Cards", pm_metacard_ids.count()],
            ["Data Window (Columns)", f"{len(target_dates)} day entries entries"],
        ]

        status_ws.clear()
        status_ws.update(status_data)

        return f"Done! {len(final_df)} cards uploaded to GDrive."
    except Exception as e:
        return f"Error: {str(e)}"
