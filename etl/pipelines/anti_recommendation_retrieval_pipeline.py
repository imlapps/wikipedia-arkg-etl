from etl.models import WIKIPEDIA_BASE_URL, AntiRecommendation, RecordKeys
from etl.models.types import DocumentsLimit, ModelQuery, RecordKey
from etl.pipelines import RetrievalPipeline
from etl.resources import RetrievalAlgorithmParameters
from etl.stores import VectorStore


class AntiRecommendationRetrievalPipeline(RetrievalPipeline):
    """
    A concrete implementation of RetrievalPipeline.

    Retrieves anti-recommendations of a Record key using Documents stored in a VectorStore.
    """

    def __init__(
        self,
        *,
        vector_store: VectorStore,
        retrieval_algorithm_parameters: RetrievalAlgorithmParameters,
    ) -> None:
        self.__vector_store = vector_store
        self.__retrieval_algorithm_parameters = retrieval_algorithm_parameters

    def __create_query(
        self,
        *,
        record_key: RecordKey,
        k: DocumentsLimit,
    ) -> ModelQuery:
        """Return a query for the retrieval algorithm."""

        return f"What are {k} Wikipedia articles that are dissimilar but surprisingly similar to the Wikipedia article {RecordKeys.to_prompt_friendly(record_key)}"

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
                score_threshold=self.__retrieval_algorithm_parameters.score_threshold,
                distance_strategy=self.__retrieval_algorithm_parameters.distance_strategy,
            )
        )
