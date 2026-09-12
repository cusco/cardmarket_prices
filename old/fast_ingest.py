import os
import gzip
import io
import json
import hashlib
import django
from django.conf import settings
from pathlib import Path

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'cm_prices.settings.prod') # UPDATE THIS
django.setup()

from prices.models import Catalog, MTGCard, MTGCardPrice
from django.db import connection

def fast_ingest_file(file_path):
    print(f"Ingesting {file_path.name}...")
    
    with gzip.open(file_path, "rb") as gz_file:
        content = io.TextIOWrapper(gz_file, encoding="utf-8").read()
    
    data = json.loads(content)
    md5sum = hashlib.md5(content.encode("utf-8"), usedforsecurity=False).hexdigest()
    
    # 1. Logic Check: Already processed?
    if Catalog.objects.filter(md5sum=md5sum, catalog_type='PRICES').exists():
        print("Skipping: Already exists.")
        return
    
    # 2. Logic Check: Match cards (Keep your existing mapping logic)
    catalog_date = data["createdAt"] # Keep your datetime parsing here
    all_cm_ids = [item["idProduct"] for item in data["priceGuides"]]
    card_map = {c.cm_id: c.id for c in MTGCard.objects.filter(cm_id__in=all_cm_ids)}
    
    # 3. Prepare rows for raw insertion
    rows = []
    for item in data["priceGuides"]:
        card_id = card_map.get(item["idProduct"])
        if not card_id: continue
            
        rows.append((
            catalog_date, card_id, item["idProduct"], 
            item.get("avg"), item.get("low"), item.get("trend"),
            item.get("avg1"), item.get("avg7"), item.get("avg30"),
            item.get("avg-foil"), item.get("low-foil"), item.get("trend-foil"),
            item.get("avg1-foil"), item.get("avg7-foil"), item.get("avg30-foil")
        ))
    
    # 4. Raw High-Speed Insert
    with connection.cursor() as cursor:
        cursor.executemany("""
            INSERT IGNORE INTO prices_mtgcardprice 
            (catalog_date, card_id, cm_id, avg, low, trend, avg1, avg7, avg30, 
             avg_foil, low_foil, trend_foil, avg1_foil, avg7_foil, avg30_foil)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, rows)
    
    # 5. Record Catalog entry
    Catalog.objects.create(catalog_date=catalog_date, md5sum=md5sum, catalog_type='PRICES')
    print(f"Inserted {len(rows)} records.")

if __name__ == "__main__":
    # Ensure speed settings are active in SQL console first!
    directory = Path("../local/catalogs")
    for f in sorted(directory.glob("*.json.gz")):
        fast_ingest_file(f)
