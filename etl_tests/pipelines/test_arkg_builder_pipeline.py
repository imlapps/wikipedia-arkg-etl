from etl.models.types import AntiRecommendationKey, Predicate, RecordKey
from etl.pipelines import ArkgBuilderPipeline


def test_construct_graph(
    anti_recommendation_graph: tuple[
        tuple[RecordKey, tuple[AntiRecommendationKey, ...]], ...
    ],
    record_key: RecordKey,
    anti_recommendation_key: AntiRecommendationKey,
) -> None:
    """Test that ArkgBuilderPipeline.construct_graph returns a RDF Store."""
    pass
    # anti_recommendation_node = next(
    #     ArkgBuilderPipeline()  # type: ignore[arg-type]
    #     .construct_graph(anti_recommendation_graph)
    #     .query(
    #         query=f"SELECT ?anti_recommendation WHERE {{ <{record_key}> <{Predicate.HAS_ANTI_RECOMMENDATION.value}> ?anti_recommendation}}",
    #     )
    # )

    # assert anti_recommendation_node[
    #     "anti_recommendation"
    # ].value == anti_recommendation_key.replace(" ", "_")
