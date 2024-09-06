from typing import Self

from pydantic import Field

from etl.models.record import Record
from etl.models.types import RecordKey, Summary


class Article(Record):
    """Pydantic Model to hold the contents of a Wikipedia Article."""

    key: RecordKey = Field(..., alias="title")
    summary: Summary | None = None

    @classmethod
    def from_record(cls, *, record: Record, summary: Summary | None) -> Self:

        return cls(title=record.key, url=record.url, summary=summary)
