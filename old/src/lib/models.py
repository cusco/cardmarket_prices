from django.db import models


class ActiveManager(models.Manager):
    def get_queryset(self):  # pragma: no cover
        """Return queryset with items that are active."""
        return super().get_queryset().filter(active=True)


class BaseAbstractModel(models.Model):
    """Base model for all other that need timestamps."""

    # Timestamp with last object update.
    date_updated = models.DateTimeField('Last update at', auto_now=True)
    # Creation timestamp.
    date_created = models.DateTimeField('Created at', auto_now_add=True)
    # Observations.
    obs = models.TextField('Observations', null=False, blank=True)

    active = models.BooleanField('active', default=True)

    # Managers
    objects = models.Manager()  # default manager.
    actives = ActiveManager()  # active records.

    class Meta:
        """Meta."""

        abstract = True
