# Generated by Django 5.1.2 on 2024-11-11 14:34

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("prices", "0003_mtgset_extra_info"),
    ]

    operations = [
        migrations.CreateModel(
            name="MTGCardPriceSlope",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                (
                    "date_updated",
                    models.DateTimeField(auto_now=True, verbose_name="Last update at"),
                ),
                (
                    "date_created",
                    models.DateTimeField(auto_now_add=True, verbose_name="Created at"),
                ),
                ("obs", models.TextField(blank=True, verbose_name="Observations")),
                ("active", models.BooleanField(default=True, verbose_name="active")),
                ("interval_days", models.PositiveSmallIntegerField()),
                ("slope", models.FloatField()),
                ("percent_change", models.FloatField()),
                (
                    "card",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="price_slopes",
                        to="prices.mtgcard",
                    ),
                ),
            ],
            options={
                "unique_together": {("card", "interval_days")},
            },
        ),
    ]
