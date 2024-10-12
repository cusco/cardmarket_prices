from django.contrib import admin
from unfold.admin import ModelAdmin

from prices.models import MTGCard
from prices.services import update_mtg


@admin.register(MTGCard)
class CustomAdminClass(ModelAdmin):
    """Update all cardmarket prices from admin."""

    actions = ['update_all']

    def update_all(self):
        """Update all cardmarket prices from admin."""
        update_mtg()
