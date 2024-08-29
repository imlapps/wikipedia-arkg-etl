from langchain_community.vectorstores import VectorStore

from etl.models import WIKIPEDIA_BASE_URL, AntiRecommendation
from etl.models.types import DocumentsLimit, ModelQuestion, RecordKey
from etl.pipelines import RetrievalPipeline


class AntiRecommendationRetrievalPipeline(RetrievalPipeline):
    """
    A concrete implementation of RetrievalPipeline.

    Retrieves anti-recommendations of a Record key using Documents stored in a VectorStore.
    """

    def __init__(self, vector_store: VectorStore) -> None:
        self.__vector_store = vector_store

    def __create_query(
        self,
        *,
        record_key: RecordKey,
        k: DocumentsLimit,
    ) -> ModelQuestion:
        """Return a query for the retrieval algorithm."""
        record_key_with_spaces = record_key.replace("_", " ")

        return f"What are {k} Wikipedia articles that are dissimilar but surprisingly similar to the Wikipedia article {record_key_with_spaces}"

    def retrieve_documents(
        self,
        *,
        record_key: RecordKey,
        k: DocumentsLimit,
    ) -> tuple[AntiRecommendation, ...]:
        """
        Return a tuple that contains anti-recommendations of record_key.

        k is the number of Documents to retrieve.
        """
        return tuple(
            AntiRecommendation(
                key=document_and_similarity_score_tuple[0].metadata["source"][
                    len(WIKIPEDIA_BASE_URL) :
                ],
                document=document_and_similarity_score_tuple[0],
                similarity_score=document_and_similarity_score_tuple[1],
            )
            for document_and_similarity_score_tuple in self.__vector_store.similarity_search_with_score(
                query=self.__create_query(
                    record_key=record_key,
                    k=k,
                ),
                k=k,
            )
        )
