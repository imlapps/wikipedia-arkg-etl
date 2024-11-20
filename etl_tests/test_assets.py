import json
from typing import cast

from langchain.docstore.document import Document
from langchain.schema.runnable import RunnableSequence
from langchain_community.vectorstores import FAISS
from pytest_mock import MockFixture

from etl.assets import (
    documents_of_wikipedia_articles_with_summaries,
    wikipedia_anti_recommendations,
    wikipedia_anti_recommendations_json_file,
    wikipedia_arkg_asset_factory,
    wikipedia_articles_from_storage,
    wikipedia_articles_vector_store,
    wikipedia_articles_with_summaries,
    wikipedia_articles_with_summaries_json_file,
)
from etl.models import (
    AntiRecommendationGraphTuple,
    DocumentTuple,
    RecordTuple,
    wikipedia,
)
from etl.models.types import (
    AntiRecommendationKey,
    NonBlankString as ModelResponse,
    NonBlankString as RdfFileExtension,
    RdfMimeType,
    NonBlankString as RdfSerializationName,
    RecordKey,
    NonBlankString as SparqlQuery,
)
from etl.namespaces import ARKG
from etl.resources import (
    InputConfig,
    OpenaiSettings,
    OutputConfig,
    RetrievalAlgorithmParameters,
)
from etl.stores import ArkgStore, VectorStore


def test_wikipedia_articles_from_storage(input_config: InputConfig) -> None:
    """Test that wikipedia_articles_from_storage successfully materializes a tuple of Wikipedia articles."""

    assert isinstance(
        wikipedia_articles_from_storage(input_config).records[0], wikipedia.Article  # type: ignore[attr-defined]
    )


def test_wikipedia_articles_with_summaries(
    session_mocker: MockFixture,
    openai_settings: OpenaiSettings,
    tuple_of_articles_with_summaries: tuple[wikipedia.Article, ...],
    article_with_summary: wikipedia.Article,
    openai_model_response: ModelResponse,
) -> None:
    """Test that wikipedia_articles_with_summaries succesfully materializes a tuple of Wikipedia articles with summaries."""

    # Mock RunnableSequence.invoke and return a ModelResponse
    session_mocker.patch.object(
        RunnableSequence, "invoke", return_value=openai_model_response
    )

    assert (
        wikipedia_articles_with_summaries(  # type: ignore[attr-defined]
            RecordTuple(records=tuple_of_articles_with_summaries),
            openai_settings,
        )
        .records[0]
        .model_dump(by_alias=True)["summary"]
        == article_with_summary.summary
    )


def test_wikipedia_articles_with_summaries_json_file(
    tuple_of_articles_with_summaries: tuple[wikipedia.Article, ...],
    output_config: OutputConfig,
    openai_settings: OpenaiSettings,  # noqa: ARG001
) -> None:
    """Test that wikipedia_articles_with_summaries_json_file writes articles to a JSON file."""

    wikipedia_articles_with_summaries_json_file(
        RecordTuple(records=tuple_of_articles_with_summaries), output_config
    )

    with output_config.parse().wikipedia_articles_with_summaries_file_path.open() as wikipedia_json_file:

        iter_tuples_of_articles_with_summaries = iter(tuple_of_articles_with_summaries)

        for wikipedia_json_line in wikipedia_json_file:
            wikipedia_json = json.loads(wikipedia_json_line)
            assert wikipedia.Article(**(wikipedia_json)) == next(
                iter_tuples_of_articles_with_summaries
            )


def test_documents_of_wikipedia_articles_with_summaries(
    tuple_of_articles_with_summaries: tuple[wikipedia.Article, ...],
    document_of_article_with_summary: Document,
) -> None:
    """Test that documents_of_wikipedia_articles_with_summaries successfully materializes a tuple of Documents."""

    assert (
        documents_of_wikipedia_articles_with_summaries(  # type: ignore[attr-defined]
            RecordTuple(records=tuple_of_articles_with_summaries),
        ).documents[0]
        == document_of_article_with_summary
    )


def test_wikipedia_articles_vector_store(
    session_mocker: MockFixture,
    openai_settings: OpenaiSettings,
    output_config: OutputConfig,
    faiss: FAISS,
    document_of_article_with_summary: Document,
) -> None:
    """Test that wikipedia_articles_vector_store calls a method that is required to create an embedding store."""

    mock_faiss__from_documents = session_mocker.patch.object(
        FAISS, "from_documents", return_value=faiss
    )

    wikipedia_articles_vector_store(
        output_config,
        openai_settings,
        DocumentTuple(documents=(document_of_article_with_summary,)),
    )

    mock_faiss__from_documents.assert_called_once()


def test_wikipedia_anti_recommendations(
    vector_store: VectorStore,
    article: wikipedia.Article,
    anti_recommendation_graph: tuple[
        tuple[RecordKey, tuple[AntiRecommendationKey, ...]], ...
    ],
    retrieval_algorithm_parameters: RetrievalAlgorithmParameters,
) -> None:
    """Test that wikipedia_anti_recommendations successfully returns anti_recommendation_graphs."""

    assert (
        wikipedia_anti_recommendations(  # type: ignore[attr-defined]
            RecordTuple(records=(article,)),
            retrieval_algorithm_parameters,
            vector_store.descriptor,
        ).anti_recommendation_graphs[0]
        == anti_recommendation_graph[0]
    )


def test_wikipedia_anti_recommendations_json_file(
    output_config: OutputConfig,
    anti_recommendation_graph: tuple[
        tuple[RecordKey, tuple[AntiRecommendationKey, ...]], ...
    ],
) -> None:
    """Test that wikipedia_anti_recommendations_json_file successfully writes an anti_recommendation_graph to a JSON file."""

    wikipedia_anti_recommendations_json_file(
        output_config,
        AntiRecommendationGraphTuple(
            anti_recommendation_graphs=anti_recommendation_graph
        ),
    )

    with output_config.parse().wikipedia_anti_recommendations_file_path.open() as wikipedia_anti_recommendations_file:

        for wikipedia_json_line in wikipedia_anti_recommendations_file:

            assert (
                json.loads(wikipedia_json_line)[0][-1]
                == anti_recommendation_graph[0][0][-1]
            )


def test_wikipedia_arkg_asset_factory(
    output_config: OutputConfig,
    anti_recommendation_key: AntiRecommendationKey,
    anti_recommendation_graph: tuple[
        tuple[RecordKey, tuple[AntiRecommendationKey, ...]], ...
    ],
    anti_recommendation_node_query: SparqlQuery,
    rdf_serialization_tuple: tuple[RdfSerializationName, RdfMimeType, RdfFileExtension],
) -> None:
    """Test that wikipedia_arkg_factory successfully materializes a Wikipedia ARKG and writes it to file."""

    wikipedia_arkg_store_descriptor = cast(
        ArkgStore.Descriptor,
        wikipedia_arkg_asset_factory(*rdf_serialization_tuple)(
            output_config,
            AntiRecommendationGraphTuple(
                anti_recommendation_graphs=anti_recommendation_graph
            ),
        ),
    )

    with ArkgStore.open(wikipedia_arkg_store_descriptor) as wikipedia_arkg_store:
        anti_recommendation_node = next(
            wikipedia_arkg_store.query(anti_recommendation_node_query)  # type: ignore[arg-type]
        )

    assert (
        anti_recommendation_node["anti_recommendation"].value
        == ARKG.anti_recommendation_iri(anti_recommendation_key).value
    )
