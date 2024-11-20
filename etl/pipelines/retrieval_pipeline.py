from abc import ABC, abstractmethod

from etl.models import AntiRecommendation
from etl.models.types import RecordsLimit as DocumentsLimit, RecordKey


class RetrievalPipeline(ABC):
    """An interface to build pipelines that retrieve Documents from a VectorStore."""

    @abstractmethod
    def retrieve_documents(
        self,
        *,
        record_key: RecordKey,
        k: DocumentsLimit,
    ) -> tuple[AntiRecommendation, ...]:
        """
        Return a tuple that contains AntiRecommendations of record_key.

        k is the number of Documents to retrieve.
        """
