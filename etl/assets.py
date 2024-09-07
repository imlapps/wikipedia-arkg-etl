import json

from dagster import AssetsDefinition, asset

from etl.databases import ArkgDatabase, EmbeddingDatabase
from etl.models import (
    AntiRecommendationGraphTuple,
    DocumentTuple,
    RecordTuple,
    rdf_serializations,
)
from etl.models.types import RdfFileExtension, RdfMimeType, RdfSerializationName
from etl.pipelines import (
    AntiRecommendationRetrievalPipeline,
    OpenaiRecordEnrichmentPipeline,
)
from etl.readers import WikipediaReader
from etl.resources import (
    InputConfig,
    OpenaiSettings,
    OutputConfig,
    RetrievalAlgorithmParameters,
)


@asset
def wikipedia_articles_from_storage(
    input_config: InputConfig,
) -> RecordTuple:
    """Materialize an asset of Wikipedia articles."""

    return RecordTuple(
        records=tuple(
            WikipediaReader(data_file_paths=input_config.parse().data_file_paths).read()
        )
    )


@asset
def wikipedia_articles_with_summaries(
    wikipedia_articles_from_storage: RecordTuple, openai_settings: OpenaiSettings
) -> RecordTuple:
    """Materialize an asset of Wikipedia articles with summaries."""

    return RecordTuple(
        records=tuple(
            OpenaiRecordEnrichmentPipeline(openai_settings).enrich_record(
                wikipedia_article
            )
            for wikipedia_article in wikipedia_articles_from_storage.records
        )
    )


@asset
def wikipedia_articles_with_summaries_json_file(
    wikipedia_articles_with_summaries: RecordTuple, output_config: OutputConfig
) -> None:
    """Store the asset of Wikipedia articles with summaries as JSON."""

    output_config.parse().record_enrichment_directory_path.mkdir(
        parents=True, exist_ok=True
    )

    with output_config.parse().wikipedia_articles_with_summaries_file_path.open(
        mode="w"
    ) as wikipedia_articles_with_summaries_file:

        wikipedia_articles_with_summaries_file.writelines(
            json.dumps(enriched_wikipedia_article.model_dump(by_alias=True))
            for enriched_wikipedia_article in wikipedia_articles_with_summaries.records
        )


@asset
def documents_of_wikipedia_articles_with_summaries(
    wikipedia_articles_with_summaries: RecordTuple,
) -> DocumentTuple:
    """Materialize an asset of Documents of Wikipedia articles with summaries."""

    return DocumentTuple.from_records(
        records=wikipedia_articles_with_summaries.records,
        record_content=lambda record: str(record.model_dump().get("summary")),
    )


@asset
def wikipedia_articles_embedding_store(
    output_config: OutputConfig,
    openai_settings: OpenaiSettings,
    documents_of_wikipedia_articles_with_summaries: DocumentTuple,
) -> EmbeddingDatabase.Descriptor:
    """Materialize an asset of Wikipedia articles embeddings."""

    return EmbeddingDatabase.create(
        openai_settings=openai_settings,
        documents=documents_of_wikipedia_articles_with_summaries.documents,
        embeddings_cache_directory_path=output_config.parse().openai_embeddings_cache_directory_path,
    ).descriptor


@asset
def wikipedia_anti_recommendations(
    openai_settings: OpenaiSettings,
    wikipedia_articles_from_storage: RecordTuple,
    retrieval_algorithm_parameters: RetrievalAlgorithmParameters,
    wikipedia_articles_embedding_store: EmbeddingDatabase.Descriptor,
    documents_of_wikipedia_articles_with_summaries: DocumentTuple,
) -> AntiRecommendationGraphTuple:
    """Materialize an asset of Wikipedia anti-recommendations."""

    wikipedia_anti_recommendations_embedding_database = EmbeddingDatabase.open(
        openai_settings=openai_settings,
        descriptor=wikipedia_articles_embedding_store,
        documents=documents_of_wikipedia_articles_with_summaries.documents,
    ).read()

    return AntiRecommendationGraphTuple(
        anti_recommendation_graphs=tuple(
            (
                record.key,
                tuple(
                    anti_recommendation.key
                    for anti_recommendation in AntiRecommendationRetrievalPipeline(
                        vector_store=wikipedia_anti_recommendations_embedding_database,
                        retrieval_algorithm_parameters=retrieval_algorithm_parameters,
                    ).retrieve_documents(record_key=record.key, k=7)
                    if anti_recommendation.key != record.key
                ),
            )
            for record in wikipedia_articles_from_storage.records
        )
    )


@asset
def wikipedia_anti_recommendations_json_file(
    output_config: OutputConfig,
    wikipedia_anti_recommendations: AntiRecommendationGraphTuple,
) -> None:
    """Store the asset of Wikipedia anti-recommendations as JSON."""

    output_config.parse().anti_recommendations_directory_path.mkdir(
        parents=True, exist_ok=True
    )

    with output_config.parse().wikipedia_anti_recommendations_file_path.open(
        mode="w"
    ) as wikipedia_anti_recommendations_file:
        wikipedia_anti_recommendations_file.writelines(
            json.dumps(anti_recommendation_graph)
            for anti_recommendation_graph in wikipedia_anti_recommendations.anti_recommendation_graphs
        )


def wikipedia_arkg_factory(
    rdf_serialization_name: RdfSerializationName,
    rdf_mime_type: RdfMimeType,
    rdf_file_extension: RdfFileExtension,
) -> AssetsDefinition:

    @asset(name=f"wikipedia_arkg_with_{rdf_serialization_name}_serialization")
    def wikipedia_arkg(
        output_config: OutputConfig,
        wikipedia_anti_recommendations: AntiRecommendationGraphTuple,
    ) -> ArkgDatabase.Descriptor:
        """Materialize a Wikipedia Anti-Recommendation Knowledge Graph asset."""

        parsed_output_config = output_config.parse()

        wikipedia_arkg_database = ArkgDatabase.create(
            requests_cache_directory=parsed_output_config.requests_cache_directory_path,
            anti_recommendation_graphs=wikipedia_anti_recommendations,
            arkg_file_path=parsed_output_config.wikipedia_arkg_file_path.with_suffix(
                rdf_file_extension
            ),
        )

        wikipedia_arkg_database.write(arkg_mime_type=rdf_mime_type)

        return wikipedia_arkg_database.descriptor

    return wikipedia_arkg


wikipedia_arkg_assets = [
    wikipedia_arkg_factory(
        rdf_serialization_name,
        rdf_mime_type,
        rdf_file_extension,
    )
    for rdf_serialization_name, rdf_mime_type, rdf_file_extension in rdf_serializations
]
