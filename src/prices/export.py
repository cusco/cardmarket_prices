from datetime import timedelta

import gspread
import pandas as pd
from django.conf import settings
from django.db.models import Min

from prices.constants import LEGAL_PREMODERN_SETS
from prices.models import MTGCard, MTGCardPrice, Catalog


def update_top_200_price_matrix():
    """
    Fetches the top 200 Premodern cards based on the lowest print price,
    tracks 30-day history, and updates Google Sheets.
    """

    # 1. Get the list of metacard_ids that are legal in Premodern
    # We define LEGAL_PREMODERN_SETS globally or import it
    pm_metacard_ids = (
        MTGCard.objects.filter(expansion_id__in=LEGAL_PREMODERN_SETS).values_list('metacard_id', flat=True).distinct()
    )

    # 2. Identify the latest price date from the Catalog
    latest_catalog = Catalog.objects.filter(catalog_id=Catalog.MTG, catalog_type=Catalog.PRICES).latest('catalog_date')
    cur_date = latest_catalog.catalog_date

    # 3. Find the top 200 Meta cards by their "Floor" price (the cheapest print)
    # We look at ALL prints of the Premodern-legal meta-cards
    metacard_floors = (
        MTGCardPrice.objects.filter(catalog_date=cur_date, card__metacard_id__in=pm_metacard_ids, trend__gt=0.1)
        .values('card__metacard_id')
        .annotate(cheapest_trend=Min('trend'))
        .order_by('-cheapest_trend')[:200]
    )

    top_metacard_ids = [item['card__metacard_id'] for item in metacard_floors]

    # 4. Associate Meta cards with a representative Set and Name
    # Since SQLite doesn't support DISTINCT ON, we fetch all relevant prints
    # and use a Python dictionary to pick the first one we encounter.
    top_200_details = {}
    details_qs = (
        MTGCard.objects.filter(metacard_id__in=top_metacard_ids)
        .select_related('expansion')
        .values('metacard_id', 'name', 'expansion__code')
    )

    for detail in details_qs:
        mid = detail['metacard_id']
        if mid not in top_200_details:
            top_200_details[mid] = {'name': detail['name'], 'set_code': detail['expansion__code']}

    # 5. Fetch 30-day history (The minimum trend per day for each metacard)
    start_date = cur_date - timedelta(days=30)
    history_qs = (
        MTGCardPrice.objects.filter(
            card__metacard_id__in=top_metacard_ids, catalog_date__gte=start_date, trend__gt=0.01
        )
        .values('card__metacard_id', 'catalog_date')
        .annotate(min_daily_trend=Min('trend'))
        .order_by('catalog_date')
    )

    # 6. Transform data using Pandas
    df = pd.DataFrame(list(history_qs))

    # Map the human-readable names and sets back to the IDs
    df['card_name'] = df['card__metacard_id'].map(lambda x: top_200_details[x]['name'])
    df['set_code'] = df['card__metacard_id'].map(lambda x: top_200_details[x]['set_code'])

    # Pivot the table: Rows are Cards, Columns are Dates
    pivot_df = df.pivot_table(index=['set_code', 'card_name'], columns='catalog_date', values='min_daily_trend')

    # order by most expensive on the latest date
    pivot_df = pivot_df.sort_values(by=cur_date, ascending=False)

    # Sort columns so the latest date is the first column after the name
    pivot_df = pivot_df.reindex(sorted(pivot_df.columns, reverse=True), axis=1)

    # Format dates to string (YYYY-MM-DD) for Google Sheets headers
    pivot_df.columns = [c.strftime('%Y-%m-%d') for c in pivot_df.columns]
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

        return f"Done! {len(final_df)} cards uploaded to GDrive."
    except Exception as e:
        return f"Error: {str(e)}"
