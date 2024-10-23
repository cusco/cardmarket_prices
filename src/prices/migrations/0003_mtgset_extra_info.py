# Generated by Django 5.1.2 on 2024-10-23 13:16

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("prices", "0002_new_model_MTGSets"),
    ]

    operations = [
        migrations.AddField(
            model_name="mtgset",
            name="code",
            field=models.CharField(max_length=10, null=True, verbose_name="Set Code"),
        ),
        migrations.AddField(
            model_name="mtgset",
            name="is_foil_only",
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name="mtgset",
            name="release_date",
            field=models.DateField(null=True, verbose_name="Set Release"),
        ),
        migrations.AddField(
            model_name="mtgset",
            name="type",
            field=models.CharField(max_length=64, null=True, verbose_name="Set Type"),
        ),
        migrations.AddField(
            model_name="mtgset",
            name="url",
            field=models.URLField(null=True, unique=True, verbose_name="URL"),
        ),
    ]