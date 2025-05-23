# Generated by Django 5.1.2 on 2025-04-09 11:59

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("prices", "0006_improve_index"),
    ]

    operations = [
        migrations.AddField(
            model_name="mtgcardpriceslope",
            name="final_price",
            field=models.FloatField(default=0),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name="mtgcardpriceslope",
            name="initial_price",
            field=models.FloatField(default=0),
            preserve_default=False,
        ),
    ]
