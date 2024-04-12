from typing import List, Any

from django.db.models import QuerySet
from ninja import Schema
from ninja.conf import settings
from ninja.pagination import PageNumberPagination
from pydantic import Field


class KSPagination(PageNumberPagination):
    class Input(Schema):
        page: int = Field(1, ge=1)
        per_page: int = Field(settings.PAGINATION_PER_PAGE, ge=1)

    class Output(Schema):
        items: List[Any]
        total: int

    def paginate_queryset(
        self,
        queryset: QuerySet,
        pagination: Input,
        **params: Any,
    ) -> Any:
        offset = (pagination.page - 1) * pagination.per_page
        return {
            "items": queryset[offset: offset + pagination.per_page],
            "total": self._items_count(queryset),
        }
