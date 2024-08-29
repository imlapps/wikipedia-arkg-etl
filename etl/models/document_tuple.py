from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from langchain.docstore.document import Document

from etl.models import WIKIPEDIA_BASE_URL, Record

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass(frozen=True)
class DocumentTuple:
    """A dataclass that holds a tuple of Documents."""

    documents: tuple[Document, ...]

    @classmethod
    def from_records(
        cls, *, records: tuple[Record, ...], record_content: Callable[[Record], str]
    ) -> DocumentTuple:
        """
        Convert Records into Documents and return an instance of DocumentTuple.

        Pass a Record into record_content and use the returned value as a Document's page_content.
        """

        return cls(
            documents=tuple(
                Document(
                    page_content=record_content(record),
                    metadata={"source": WIKIPEDIA_BASE_URL + record.key},
                )
                for record in records
            )
        )
