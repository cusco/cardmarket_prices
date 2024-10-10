import uuid

from django.db import models

from lib.models import BaseAbstractModel

# Create your models here.


class Catalog(BaseAbstractModel):
    """Model that represents catalog items."""

    MTG = 1
    WOW = 2
    YUGIOH = 3
    THE_SPOILS = 5
    POKEMON = 6
    FORCE_OF_WILL = 7
    CARDFIGHT_VANGUARD = 8
    FINAL_FANTASY = 9
    WEISS_SCHWARZ = 10
    DRAGON_BORNE = 11
    MY_LITTLE_PONY = 12
    DRAGON_BALL = 13
    STAR_WARS_DESTINY = 15
    FLESH_AND_BLOOD = 16
    DIGIMON = 17
    ONE_PIECE = 18
    LORCANA = 19
    BATTLE_SPIRITS_SAGA = 20
    STAR_WARS_UNLIMITED = 21

    CATALOG_IDS = (
        (MTG, 'Magic: The Gathering'),
        (WOW, 'World of Warcraft'),
        (YUGIOH, 'Yu-Gi-Oh!'),
        (THE_SPOILS, 'The Spoils'),
        (POKEMON, 'Pokémon'),
        (FORCE_OF_WILL, 'Force of Will'),
        (CARDFIGHT_VANGUARD, 'Cardfight!! Vanguard'),
        (FINAL_FANTASY, 'Final Fantasy'),
        (WEISS_SCHWARZ, 'Weiss Schwarz'),
        (DRAGON_BORNE, 'Dragon Borne'),
        (MY_LITTLE_PONY, 'My Little Pony'),
        (DRAGON_BALL, 'Dragon Ball'),
        (STAR_WARS_DESTINY, 'Star Wars: Destiny'),
        (FLESH_AND_BLOOD, 'Flesh and Blood'),
        (DIGIMON, 'Digimon'),
        (ONE_PIECE, 'One Piece'),
        (LORCANA, 'Lorcana'),
        (BATTLE_SPIRITS_SAGA, 'Battle Spirits Saga'),
        (STAR_WARS_UNLIMITED, 'Star Wars Unlimited'),
    )

    PRODUCTS = 1
    PRICES = 2

    CATALOG_TYPES = (
        (PRODUCTS, 'Products'),
        (PRICES, 'Prices'),
    )

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    catalog_date = models.DateTimeField(verbose_name='date published')
    catalog_id = models.PositiveSmallIntegerField(choices=CATALOG_IDS, default=MTG)  # I care about MTG right now
    catalog_type = models.PositiveSmallIntegerField(choices=CATALOG_TYPES)
    md5sum = models.CharField(max_length=32, verbose_name='MD5sum', unique=True)


class MTGCard(BaseAbstractModel):
    """Model representing a MTG card."""

    cm_id = models.PositiveIntegerField(null=False, blank=False, primary_key=True)
    name = models.CharField(max_length=255, null=True)
    slug = models.SlugField(255, unique=True, null=True)  # nosemgrep /en/Magic/Products/Singles/Mirage/Flash

    category_id = models.PositiveIntegerField(default=1)
    expansion_id = models.PositiveIntegerField()
    metacard_id = models.PositiveIntegerField()
    cm_date_added = models.DateTimeField(verbose_name='Date added to cardmarket')

    class Meta:
        indexes = [models.Index(fields=['cm_id'], name='idx_mtgcard_cm_id')]


class MTGCardPrice(BaseAbstractModel):
    """MTG card price model."""

    card = models.ForeignKey(MTGCard, on_delete=models.CASCADE, related_name='prices')
    catalog_date = models.DateTimeField(verbose_name='Catalog Date')
    cm_id = models.IntegerField(verbose_name="cardmarket id")
    avg = models.FloatField(null=True, verbose_name="Average price")
    low = models.FloatField(null=True, verbose_name="Low price")
    trend = models.FloatField(null=True, verbose_name="Trend price")

    avg1 = models.FloatField(null=True, verbose_name="Average price for 1 day")
    avg7 = models.FloatField(null=True, verbose_name="Average price for 7 days")
    avg30 = models.FloatField(null=True, verbose_name="Average price for 30 days")

    avg_foil = models.FloatField(null=True, verbose_name="Foil average price")
    low_foil = models.FloatField(null=True, verbose_name="Foil low price")
    trend_foil = models.FloatField(null=True, verbose_name="Foil trend price")
    avg1_foil = models.FloatField(null=True, verbose_name="Foil average price for 1 day")
    avg7_foil = models.FloatField(null=True, verbose_name="Foil average price for 7 days")
    avg30_foil = models.FloatField(null=True, verbose_name="Foil average price for 30 days")

    class Meta:
        constraints = [models.UniqueConstraint(fields=['catalog_date', 'cm_id'], name='Unique card price per day')]
        indexes = [models.Index(fields=['cm_id'], name='idx_mtgprice_cm_id')]
