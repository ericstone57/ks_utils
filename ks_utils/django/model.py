import shortuuid
from model_utils.models import TimeStampedModel, SoftDeletableModel
from django.db import models


def generate_uuid() -> str:
    """Generate a UUID."""
    return shortuuid.ShortUUID().random(length=12)


class BaseModel(TimeStampedModel):
    id = models.CharField(
        max_length=30,
        primary_key=True,
        default=generate_uuid,
        editable=False
    )

    class Meta:
        abstract = True


class BaseModelSoftDeletable(TimeStampedModel, SoftDeletableModel):
    id = models.CharField(
        max_length=30,
        primary_key=True,
        default=generate_uuid,
        editable=False
    )

    class Meta:
        abstract = True
