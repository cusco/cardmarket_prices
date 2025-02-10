# Generated by Django 5.1.2 on 2025-02-10 12:24

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("prices", "0005_pricing_indexes"),
    ]

    operations = [
        migrations.AddIndex(
            model_name="mtgcardprice",
            index=models.Index(
                fields=["card", "trend", "catalog_date"],
                name="prices_mtgc_card_id_572283_idx",
            ),
        ),
    ]
