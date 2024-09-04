import json

from dagster import asset

from etl.models import AntiRecommendationGraphTuple, DocumentTuple, RecordTuple
from etl.pipelines import (
    AntiRecommendationRetrievalPipeline,
    ArkgBuilderPipeline,
    OpenaiEmbeddingPipeline,
    OpenaiRecordEnrichmentPipeline,
)
from etl.readers import WikipediaReader
from etl.resources import (
    ArkgConfig,
    InputConfig,
    OutputConfig,
    OpenaiSettings,
    RetrievalAlgorithmParameters,
)
from etl.databases import EmbeddingStore, ArkgStore


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
) -> EmbeddingStore.Descriptor:
    """Materialize an asset of Wikipedia articles embeddings."""

    return EmbeddingStore.create(
        openai_settings=openai_settings,
        embeddings_cache_directory_path=output_config,
        documents=documents_of_wikipedia_articles_with_summaries,
    ).descriptor


@asset
def wikipedia_anti_recommendations(
    openai_settings: OpenaiSettings,
    embedding_store: EmbeddingStore.Descriptor,
    wikipedia_articles_from_storage: RecordTuple,
    retrieval_algorithm_parameters: RetrievalAlgorithmParameters,
    documents_of_wikipedia_articles_with_summaries: DocumentTuple,
) -> AntiRecommendationGraphTuple:
    """Materialize an asset of Wikipedia anti-recommendations."""

    wikipedia_anti_recommendations_embedding_store = EmbeddingStore.open(
        descriptor=embedding_store,
        openai_settings=openai_settings,
        documents=documents_of_wikipedia_articles_with_summaries,
    ).load()

    return AntiRecommendationGraphTuple(
        anti_recommendation_graphs=tuple(
            (
                record.key,
                tuple(
                    anti_recommendation.key
                    for anti_recommendation in AntiRecommendationRetrievalPipeline(
                        vector_store=wikipedia_anti_recommendations_embedding_store,
                        retrieval_algorithm_settings=retrieval_algorithm_parameters,
                    ).retrieve_documents(record_key=record.key, k=7)
                    if anti_recommendation.key != record.key
                ),
            )
            for record in wikipedia_articles_from_storage.records
        )
    )


@asset
def wikipedia_anti_recommendations_json_file(
    wikipedia_anti_recommendations: AntiRecommendationGraphTuple,
    output_config: OutputConfig,
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


@asset
def wikipedia_arkg(
    wikipedia_anti_recommendations: AntiRecommendationGraphTuple,
    arkg_config: ArkgConfig,
    output_config: OutputConfig,
) -> None:
    """Materialize a Wikipedia Anti-Recommendation Knowledge Graph asset."""

    ArkgBuilderPipeline(
        requests_cache_directory=output_config.parse().requests_cache_directory_path
    ).construct_graph(wikipedia_anti_recommendations.anti_recommendation_graphs).dump(
        output=output_config.parse().wikipedia_arkg_file_path,
        mime_type=arkg_config.rdf_mime_type,
    )
