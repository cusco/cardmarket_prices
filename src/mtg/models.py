from django.db import models

from lib.models import BaseAbstractModel

# class ScryfallCardManager(models.Manager):
#     # Taken from https://github.com/baronvonvaderham/django-mtg-card-catalog
#
#     def get_or_create_card(self, card_data):
#         """Fetch or create a card based on the provided data dictionary."""
#         card, created = self.update_or_create(
#             id=card_data["id"],
#             defaults=card_data,
#         )
#         return created, card


class ScryfallCard(BaseAbstractModel):
    """Class to contain a local version of the scryfall data to limit the need for external API calls."""

    id = models.UUIDField(primary_key=True, editable=True)
    cardmarket_id = models.PositiveIntegerField()
    oracle_id = models.CharField(max_length=128, null=True)  # NOQA nosemgrep
    name = models.CharField(max_length=256, null=True)  # nosemgrep
    mana_cost = models.CharField(max_length=128, blank=True, null=True)  # NOQA nosemgrep
    cmc = models.PositiveSmallIntegerField(blank=True, null=True)  # NOQA nosemgrep
    types = models.CharField(max_length=256, blank=True, null=True)  # NOQA nosemgrep
    subtypes = models.CharField(max_length=256, blank=True, null=True)  # NOQA nosemgrep
    colors = models.CharField(max_length=128, blank=True, null=True)  # NOQA nosemgrep
    color_identity = models.CharField(max_length=128, blank=True, null=True)  # NOQA nosemgrep
    oracle_text = models.CharField(max_length=2048, blank=True, null=True)  # NOQA nosemgrep

    legalities = models.CharField(max_length=256, blank=True, null=True)  # NOQA nosemgrep
    image_small = models.URLField(blank=True, null=True)  # NOQA nosemgrep
    image_normal = models.URLField(blank=True, null=True)  # NOQA nosemgrep

    # objects = ScryfallCardManager()

    class Meta:
        indexes = [models.Index(fields=['cardmarket_id'], name='idx_scryfallcard_cm_id')]

    def __str__(self):
        """Return string representation of ScryfallCard model."""
        return self.name
