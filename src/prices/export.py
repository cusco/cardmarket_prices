import gspread
import pandas as pd
import pytz
from django.conf import settings
from django.db.models import Min, Q
from django.db.models.functions import TruncDate

from prices.constants import LEGAL_PREMODERN_SETS
from prices.models import Catalog, MTGCard, MTGCardPrice

germany_tz = pytz.timezone('Europe/Berlin')


TOP_N_COUNT = 500


def export_top_cards_to_gdrive():
    """Find premodern lowest price print on premodern cards, upload top expensive ones to google spreadsheets."""

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
        MTGCardPrice.objects.filter(
            card__metacard_id__in=pm_metacard_ids,
            catalog_date=cur_date,
            trend__gt=0.01,
        )
        .values("card__metacard_id")
        .annotate(cheapest_trend=Min("trend"))
        .order_by("-cheapest_trend")[:TOP_N_COUNT]
    )

    # 4. Get specific Card Printing details for the 200 cheapest versions
    top_metacard_ids = [item["card__metacard_id"] for item in metacard_floors]

    # Build the identity filter to match the EXACT printing that hit that price
    identity_filter = Q()
    for item in metacard_floors:
        identity_filter |= Q(
            metacard_id=item["card__metacard_id"], prices__trend=item["cheapest_trend"], prices__catalog_date=cur_date
        )

    details_qs = (
        MTGCard.objects.filter(identity_filter)
        .select_related("expansion")
        .values("metacard_id", "name", "expansion__name")
    )

    top_200_details = {}
    for d in details_qs:
        mid = d["metacard_id"]
        # In case of price ties between sets, we just take the first one found
        if mid not in top_200_details:
            top_200_details[mid] = {"name": d["name"], "set_name": d["expansion__name"]}

    # 5. Fetch the last 60 unique entry dates available in the DB
    recent_catalogs = list(
        Catalog.objects.filter(catalog_id=Catalog.MTG, catalog_type=Catalog.PRICES)
        .order_by("-catalog_date")
        .values_list("catalog_date", flat=True)[:60]
    )

    start_timestamp = min(recent_catalogs)

    history_qs = (
        MTGCardPrice.objects.filter(
            card__metacard_id__in=top_metacard_ids,  # Re-used here!
            catalog_date__gte=start_timestamp,
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
            ["Latest pricing date", cur_date.strftime("%Y-%m-%d %H:%M:%S")],
            ["Oldest pricing date", recent_catalogs[-1].strftime("%Y-%m-%d") if recent_catalogs else "N/A"],
            ["Total Premodern Cards", pm_metacard_ids.count()],
            ["Data Window (Columns)", f"{len(recent_catalogs)} day entries entries"],
        ]

        status_ws.clear()
        status_ws.update(status_data)

        return f"Done! {len(final_df)} cards uploaded to GDrive."
    except Exception as err:  # NOQA
        return f"Error: {str(err)}"
