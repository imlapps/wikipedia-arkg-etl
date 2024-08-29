from etl.models import ArkgInstance
from etl.models.types import AntiRecommendationKey, RecordKey, SparqlQuery
from etl.pipelines import ArkgBuilderPipeline


def test_construct_graph(
    anti_recommendation_graph: tuple[
        tuple[RecordKey, tuple[AntiRecommendationKey, ...]], ...
    ],
    anti_recommendation_key: AntiRecommendationKey,
    anti_recommendation_node_query: SparqlQuery,
    arkg_builder_pipeline: ArkgBuilderPipeline,
) -> None:
    """Test that ArkgBuilderPipeline.construct_graph returns a RDF Store."""

    anti_recommendation_node = next(
        arkg_builder_pipeline.construct_graph(  # type: ignore[arg-type]
            anti_recommendation_graph
        ).query(query=anti_recommendation_node_query)
    )

    assert (
        anti_recommendation_node["anti_recommendation"].value
        == ArkgInstance.anti_recommendation_iri(anti_recommendation_key).value
    )
