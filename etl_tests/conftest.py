import os
from pathlib import Path

import pytest
from faiss import IndexFlatL2
from langchain.docstore.document import Document
from langchain_community.docstore.in_memory import InMemoryDocstore
from langchain_community.vectorstores import FAISS
from langchain_community.vectorstores.utils import DistanceStrategy
from langchain_openai import OpenAIEmbeddings

from etl.databases.arkg_database import ArkgDatabase
from etl.databases.embedding_database import EmbeddingDatabase
from etl.models import (
    WIKIPEDIA_BASE_URL,
    AntiRecommendation,
    rdf_serializations,
    wikipedia,
)
from etl.models.types import (
    AntiRecommendationKey,
    DataFileName,
    ModelResponse,
    RdfFileExtension,
    RdfMimeType,
    RdfSerializationName,
    RecordKey,
    SparqlQuery,
)
from etl.pipelines import (
    AntiRecommendationRetrievalPipeline,
    ArkgBuilderPipeline,
    OpenaiEmbeddingPipeline,
    OpenaiRecordEnrichmentPipeline,
)
from etl.readers import WikipediaReader
from etl.resources import (
    InputConfig,
    OpenaiSettings,
    OutputConfig,
    RetrievalAlgorithmParameters,
)


@pytest.fixture(scope="session")
def data_file_names() -> tuple[DataFileName, ...]:
    """Return a tuple of data file names."""

    return ("mini-wikipedia.output.txt",)


@pytest.fixture(scope="session")
def input_data_files_directory_path() -> Path:
    """Return the Path of input data files."""

    return (
        Path(__file__).parent.parent.absolute()
        / "etl"
        / "data"
        / "input"
        / "data_files"
    )


@pytest.fixture(scope="session")
def input_config(
    data_file_names: tuple[DataFileName, ...],
    input_data_files_directory_path: Path,
) -> InputConfig:
    """
    Return an InputConfig object.
    Skip all tests that use this fixture if input data files are absent from the ETL.
    """

    if input_data_files_directory_path.exists():
        return InputConfig.default(
            data_files_directory_path_default=input_data_files_directory_path,
            data_file_names_default=data_file_names,
        )
    pytest.skip(reason="don't have input data files.")


@pytest.fixture(scope="session")
def output_config() -> OutputConfig:
    """Return an OutputConfig object."""

    return OutputConfig.default(
        output_directory_path_default=Path(__file__).parent.parent.absolute()
        / "etl"
        / "data"
        / "output"
    )


@pytest.fixture(scope="session")
def arkg_config() -> ArkgConfig:
    """Return an ArkgConfig object."""

    return ArkgConfig(rdf_mime_type=RdfMimeType.TURTLE)


@pytest.fixture(scope="session")
def wikipedia_reader(input_config: InputConfig) -> WikipediaReader:
    """Return a WikipediaReaderobject."""

    return WikipediaReader(data_file_paths=input_config.parse().data_file_paths)


@pytest.fixture(scope="session")
def openai_settings() -> OpenaiSettings:
    """
    Return an OpenaiSettings object.
    Skip all tests that use this fixture if OPENAI_API_KEY is not present in the environment variables.
    """

    if "OPENAI_API_KEY" in os.environ:
        return OpenaiSettings(openai_api_key=os.environ.get("OPENAI_API_KEY"))

    pytest.skip(reason="don't have OpenAI key.")


@pytest.fixture(scope="session")
def retrieval_algorithm_parameters() -> RetrievalAlgorithmParameters:
    """Return a RetrievalAlgorithmParameters object."""

    return RetrievalAlgorithmParameters(
        distance_strategy=DistanceStrategy.COSINE,
        score_threshold=0.5,
    )


@pytest.fixture(scope="session")
def openai_record_enrichment_pipeline(
    openai_settings: OpenaiSettings,
) -> OpenaiRecordEnrichmentPipeline:
    """Return an OpenaiRecordEnrichmentPipeline object."""

    return OpenaiRecordEnrichmentPipeline(openai_settings=openai_settings)


@pytest.fixture(scope="session")
def openai_embedding_pipeline(
    openai_settings: OpenaiSettings, output_config: OutputConfig
) -> OpenaiEmbeddingPipeline:
    """Return an OpenaiEmbedddingPipeline object."""

    return OpenaiEmbeddingPipeline(
        openai_settings=openai_settings,
        openai_embeddings_cache_directory_path=output_config.parse().openai_embeddings_cache_directory_path,
    )


@pytest.fixture(scope="session")
def record_key() -> RecordKey:
    """Return a sample record key."""

    return "Mouseion"


@pytest.fixture(scope="session")
def openai_model_response() -> ModelResponse:
    """Return a sample OpenAI summary."""

    return """The Mouseion, established in Alexandria, Egypt, in the 3rd century BCE, was an ancient center of
              learning and research associated with the Library of Alexandria. Founded by Ptolemy I Soter, it
              functioned as a scholarly community akin to a modern university, hosting scholars and scientists
              The Mouseion featured lecture halls, laboratories, and communal dining for resident scholars, fostering
              intellectual exchange. It significantly contributed to advancements in various fields, including mathematics,
              astronomy, medicine, and literature. The institution's decline began with the Roman conquest and other
              sociopolitical changes, but its legacy endures as a symbol of classical knowledge and scholarship.
           """


@pytest.fixture(scope="session")
def article(record_key: RecordKey) -> wikipedia.Article:
    """Return a wikipedia.Article object."""

    return wikipedia.Article(
        title=record_key,
        url=WIKIPEDIA_BASE_URL + record_key.replace(" ", "_"),
    )


@pytest.fixture(scope="session")
def article_with_summary(
    article: wikipedia.Article, openai_model_response: ModelResponse
) -> wikipedia.Article:
    """Return a wikipedia.Article object with a set summary field."""

    article.summary = openai_model_response
    return article


@pytest.fixture(scope="session")
def document_of_article_with_summary(
    article_with_summary: wikipedia.Article,
) -> Document:
    """Return a Document of a wikipedia.Article object with a set summary field."""
    return Document(
        page_content=str(article_with_summary.model_dump().get("summary")),
        metadata={"source": WIKIPEDIA_BASE_URL + article_with_summary.key},
    )


@pytest.fixture(scope="session")
def tuple_of_articles_with_summaries(
    article_with_summary: wikipedia.Article,
) -> tuple[wikipedia.Article, ...]:
    """Return a tuple of wikipedia.Articles that have summaries."""

    return (article_with_summary,)


@pytest.fixture(scope="session")
def embedding_database(
    openai_embedding_pipeline: OpenaiEmbeddingPipeline,
    document_of_article_with_summary: Document,
    output_config: OutputConfig,
) -> EmbeddingDatabase.Descriptor:

    return EmbeddingDatabase(
        embedding_store=openai_embedding_pipeline.create_embedding_store(
            documents=(document_of_article_with_summary,)
        ),
        embeddings_cache_directory_path=output_config.parse().openai_embeddings_cache_directory_path,
    ).descriptor


@pytest.fixture(scope="session")
def faiss(openai_settings: OpenaiSettings) -> FAISS:  # noqa: ARG001
    """Return a FAISS object."""

    return FAISS(
        embedding_function=OpenAIEmbeddings(),
        docstore=InMemoryDocstore(),
        index=IndexFlatL2(42),
        index_to_docstore_id={},
        normalize_L2=False,
    )


@pytest.fixture(scope="session")
def anti_recommendation_retrieval_pipeline(
    faiss: FAISS, retrieval_algorithm_parameters: RetrievalAlgorithmParameters
) -> AntiRecommendationRetrievalPipeline:
    """Return an AntiRecommendationRetrievalPipeline object."""

    return AntiRecommendationRetrievalPipeline(
        vector_store=faiss,
        retrieval_algorithm_parameters=retrieval_algorithm_parameters,
    )


@pytest.fixture(scope="session")
def arkg_builder_pipeline(output_config: OutputConfig) -> ArkgBuilderPipeline:
    """Return an ArkgBuilderPipeline object."""

    return ArkgBuilderPipeline(
        requests_cache_directory=output_config.parse().requests_cache_directory_path
    )


@pytest.fixture(scope="session")
def anti_recommendation_key() -> AntiRecommendationKey:
    "Return a sample anti-recommendation key."

    return "Sankoré_Madrasah"


@pytest.fixture(scope="session")
def anti_recommendation_article(
    anti_recommendation_key: AntiRecommendationKey,
) -> wikipedia.Article:
    """Return a wikipedia.Article object that will be used as an anti-recommendation."""

    return wikipedia.Article(
        title=anti_recommendation_key,
        url=WIKIPEDIA_BASE_URL + anti_recommendation_key.replace(" ", "_"),
        summary="""Sankore Madrasah is an ancient center of learning located in Timbuktu, Mali, and is one of
                   the three prestigious madrassas that comprise the University of Timbuktu. Established in the 14th century,
                   it became a significant institution for higher education, attracting scholars from across Africa and the Islamic world.
                   The curriculum covered a wide range of subjects, including theology, law, mathematics, astronomy, and medicine.
                   Sankore Madrasah played a vital role in the intellectual and cultural flourishing of Timbuktu during its golden age.
                   Today, it stands as a historical symbol of Africa's rich educational heritage.
                """,
    )


@pytest.fixture(scope="session")
def document_of_anti_recommendation_article(
    anti_recommendation_article: wikipedia.Article,
) -> Document:
    """Return a Document of a wikipedia.Article anti-recommendation object."""

    return Document(
        page_content=str(anti_recommendation_article.model_dump().get("summary")),
        metadata={"source": WIKIPEDIA_BASE_URL + anti_recommendation_article.key},
    )


@pytest.fixture(scope="session")
def anti_recommendation(
    anti_recommendation_key: AntiRecommendationKey,
    document_of_anti_recommendation_article: Document,
) -> AntiRecommendation:
    """Return an AntiRecommendation NamedTuple."""

    return AntiRecommendation(
        key=anti_recommendation_key,
        document=document_of_anti_recommendation_article,
        similarity_score=0.82,
    )


@pytest.fixture(scope="session")
def anti_recommendation_graph(
    record_key: RecordKey, anti_recommendation_key: RecordKey
) -> tuple[tuple[RecordKey, tuple[AntiRecommendationKey, ...]], ...]:
    """Return a tuple containing an anti_recommendation_graph."""

    return ((record_key, (anti_recommendation_key,)),)


@pytest.fixture(scope="session")
def rdf_serialization_tuple() -> (
    tuple[RdfSerializationName, RdfMimeType, RdfFileExtension]
):

    return next(iter(rdf_serializations))


@pytest.fixture(scope="session")
def arkg_database(
    output_config: OutputConfig,
    arkg_builder_pipeline: ArkgBuilderPipeline,
    anti_recommendation_graph: tuple[
        tuple[RecordKey, tuple[AntiRecommendationKey, ...]], ...
    ],
    rdf_serialization_tuple: tuple[RdfSerializationName, RdfMimeType, RdfFileExtension],
) -> ArkgDatabase.Descriptor:
    return ArkgDatabase(
        arkg=arkg_builder_pipeline.construct_graph(anti_recommendation_graph),
        arkg_file_path=output_config.parse().wikipedia_arkg_file_path.with_suffix(
            rdf_serialization_tuple[-1]
        ),
    ).descriptor


@pytest.fixture(scope="session")
def record_key_wikidata_identifier() -> RecordKey:
    """Return the Wikipedia identifier of the record_key `Mouseion`"""

    return "Q684645"


@pytest.fixture(scope="session")
def anti_recommendation_node_query(
    record_key_wikidata_identifier: RecordKey,
) -> SparqlQuery:
    """Return a SPARQL query that fetches a record_key's anti-recommendation from an RDFStore."""

    return f"""\
    PREFIX schema: <http://schema.org/>
    PREFIX wd: <http://www.wikidata.org/entity/>

    SELECT ?anti_recommendation WHERE {{?anti_recommendation schema:itemReviewed wd:{record_key_wikidata_identifier}}}
    """
