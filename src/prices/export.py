import gspread
import pandas as pd
import pytz
from django.conf import settings
from django.db.models import Min, Q

from prices.constants import LEGAL_PREMODERN_SETS
from prices.models import Catalog, MTGCard, MTGCardPrice

germany_tz = pytz.timezone("Europe/Berlin")
TOP_N_COUNT = 500


def export_top_cards_to_gdrive():
    """Find premodern lowest price print, upload top expensive ones to google spreadsheets."""

    # 1. Get the list of metacard_ids that are legal in Premodern
    pm_metacard_ids = (
        MTGCard.objects.filter(expansion_id__in=LEGAL_PREMODERN_SETS).values_list("metacard_id", flat=True).distinct()
    )

    # 2. Identify the latest price date
    latest_cat = Catalog.objects.filter(catalog_id=Catalog.MTG, catalog_type=Catalog.PRICES).latest("catalog_date")
    cur_date = latest_cat.catalog_date

    # 3. Find top cards by metacard_id and their Floor price
    metacard_floors = list(
        MTGCardPrice.objects.filter(
            card__metacard_id__in=pm_metacard_ids,
            catalog_date=cur_date,
            trend__gt=0.01,
        )
        .values("card__metacard_id")
        .annotate(cheapest_trend=Min("trend"))
        .order_by("-cheapest_trend")[:TOP_N_COUNT]
    )

    # 4. Get specific Card Printing details
    top_meta_ids = [item["card__metacard_id"] for item in metacard_floors]
    relevant_pks = list(MTGCard.objects.filter(metacard_id__in=top_meta_ids).values_list("cm_id", flat=True))

    identity_filter = Q()
    for item in metacard_floors:
        identity_filter |= Q(
            metacard_id=item["card__metacard_id"],
            prices__trend=item["cheapest_trend"],
            prices__catalog_date=cur_date,
        )

    top_200_details = {}
    for d in (
        MTGCard.objects.filter(identity_filter)
        .select_related("expansion")
        .values("metacard_id", "name", "expansion__name")
    ):
        if d["metacard_id"] not in top_200_details:
            top_200_details[d["metacard_id"]] = {"name": d["name"], "set_name": d["expansion__name"]}

    # 5. Fetch history
    recent_catalogs = list(
        Catalog.objects.filter(catalog_id=Catalog.MTG, catalog_type=Catalog.PRICES)
        .order_by("-catalog_date")
        .values_list("catalog_date", flat=True)[:60]
    )

    history_qs = (
        MTGCardPrice.objects.filter(
            card_id__in=relevant_pks,
            catalog_date__gte=min(recent_catalogs),
            trend__gt=0.01,
        )
        .values("card_id", "catalog_date", "trend")
        .order_by("catalog_date")
    )

    # 6. Transform data using Pandas
    df = pd.DataFrame(list(history_qs))
    df["date_only"] = pd.to_datetime(df["catalog_date"]).dt.date

    card_to_meta = dict(MTGCard.objects.filter(cm_id__in=relevant_pks).values_list("cm_id", "metacard_id"))

    df["meta_id"] = df["card_id"].map(card_to_meta)
    df["card_name"] = df["meta_id"].map(lambda x: top_200_details.get(x, {}).get("name", "Unknown"))
    df["set_name"] = df["meta_id"].map(lambda x: top_200_details.get(x, {}).get("set_name", "Unknown"))

    df = df.groupby(["set_name", "card_name", "date_only"])["trend"].min().reset_index()
    pivot_df = df.pivot(index=["set_name", "card_name"], columns="date_only", values="trend")
    pivot_df = pivot_df.sort_values(by=cur_date.date(), ascending=False)
    pivot_df = pivot_df.reindex(sorted(pivot_df.columns, reverse=True), axis=1)

    pivot_df.columns = [c.strftime("%Y-%m-%d") for c in pivot_df.columns]
    final_df = pivot_df.reset_index()

    # 7. Authentication and Upload
    try:
        gdrive_client = gspread.service_account(filename=settings.GOOGLE_SECRET_CREDENTIALS)
        sheet = gdrive_client.open_by_key("1vQs3vlXHu7BELFoVuK4ysfzMeYHDxMdOWgPAGmPVZjk")

        try:
            worksheet = sheet.worksheet("premodern_bulk")
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sheet.add_worksheet(title="premodern_bulk", rows=1000, cols=40)

        worksheet.clear()
        worksheet.update([final_df.columns.values.tolist()] + final_df.values.tolist())

        # 8. Status update
        try:
            status_ws = sheet.worksheet("status")
        except gspread.exceptions.WorksheetNotFound:
            status_ws = sheet.add_worksheet(title="status", rows=20, cols=5)

        status_ws.clear()
        status_ws.update(
            [
                ["Metric", "Value"],
                ["Last script run (UTC)", pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")],
                ["Latest pricing date", cur_date.strftime("%Y-%m-%d %H:%M:%S")],
                ["Oldest pricing date", recent_catalogs[-1].strftime("%Y-%m-%d") if recent_catalogs else "N/A"],
                ["Total Premodern Cards", pm_metacard_ids.count()],
                ["Data Window", f"{len(recent_catalogs)} entries"],
            ]
        )

        return f"Done! {len(final_df)} cards uploaded."
    except Exception as err:  # NOQA
        return f"Error: {str(err)}"
