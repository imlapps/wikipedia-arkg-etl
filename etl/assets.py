import json
from typing import cast

from dagster import AssetsDefinition, asset

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
from etl.stores import ArkgStore, VectorStore


@asset
def wikipedia_articles_from_storage(
    input_config: InputConfig,
) -> RecordTuple:
    """Materialize an asset of Wikipedia articles."""

    parsed_input_config = input_config.parse()

    return RecordTuple(
        records=tuple(
            WikipediaReader(
                data_file_paths=parsed_input_config.data_file_paths,
                records_limit=parsed_input_config.records_limit,
            ).read()
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
def wikipedia_articles_vector_store(
    output_config: OutputConfig,
    openai_settings: OpenaiSettings,
    documents_of_wikipedia_articles_with_summaries: DocumentTuple,
) -> VectorStore.Descriptor:
    """Materialize an asset of Wikipedia articles embeddings."""

    with VectorStore.create(
        openai_settings=openai_settings,
        documents=documents_of_wikipedia_articles_with_summaries.documents,
        output_config=output_config,
    ) as vector_store:
        vector_store.save_local()

        return cast(VectorStore.Descriptor, vector_store.descriptor)


@asset
def wikipedia_anti_recommendations(
    wikipedia_articles_from_storage: RecordTuple,
    retrieval_algorithm_parameters: RetrievalAlgorithmParameters,
    wikipedia_articles_vector_store: VectorStore.Descriptor,
) -> AntiRecommendationGraphTuple:
    """Materialize an asset of Wikipedia anti-recommendations."""

    with VectorStore.open(wikipedia_articles_vector_store) as wikipedia_vector_store:

        return AntiRecommendationGraphTuple(
            anti_recommendation_graphs=tuple(
                (
                    record.key,
                    tuple(
                        anti_recommendation.key
                        for anti_recommendation in AntiRecommendationRetrievalPipeline(
                            vector_store=wikipedia_vector_store,
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


def wikipedia_arkg_asset_factory(
    rdf_serialization_name: RdfSerializationName,
    rdf_mime_type: RdfMimeType,
    rdf_file_extension: RdfFileExtension,
) -> AssetsDefinition:
    """
    A factory to build Wikipedia ARKG assets.

    Multiple assets are created based on different RDF serialization types.
    Each RDF serialization type is characterized by a (name, mime_type, file_extension) triple.
    """

    @asset(name=f"wikipedia_arkg_with_{rdf_serialization_name}_serialization")
    def wikipedia_arkg(
        output_config: OutputConfig,
        wikipedia_anti_recommendations: AntiRecommendationGraphTuple,
    ) -> ArkgStore.Descriptor:
        """Materialize a Wikipedia Anti-Recommendation Knowledge Graph asset."""

        parsed_output_config = output_config.parse()

        with ArkgStore.create(
            requests_cache_directory_path=parsed_output_config.requests_cache_directory_path,
            anti_recommendation_graphs=wikipedia_anti_recommendations,
            directory_path=parsed_output_config.wikipedia_arkg_store_directory_path,
        ) as wikipedia_arkg_store:

            wikipedia_arkg_store.dump(
                file_path=parsed_output_config.wikipedia_arkg_file_path.with_suffix(
                    rdf_file_extension
                ),
                rdf_mime_type=rdf_mime_type,
            )

            return wikipedia_arkg_store.descriptor

    return wikipedia_arkg


wikipedia_arkg_assets = [
    wikipedia_arkg_asset_factory(*rdf_serialization_tuple)
    for rdf_serialization_tuple in rdf_serializations
]
