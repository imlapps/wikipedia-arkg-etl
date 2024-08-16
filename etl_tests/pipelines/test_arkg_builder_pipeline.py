from etl.models import ArkgInstance
from etl.models.types import AntiRecommendationKey, RecordKey
from etl.pipelines import ArkgBuilderPipeline


def test_construct_graph(
    anti_recommendation_graph: tuple[
        tuple[RecordKey, tuple[AntiRecommendationKey, ...]], ...
    ],
    record_key: RecordKey,
    anti_recommendation_key: AntiRecommendationKey,
) -> None:
    """Test that ArkgBuilderPipeline.construct_graph returns a RDF Store."""

    query = f"""
    PREFIX schema: <http://schema.org/>
    PREFIX wb: <https://en.wikipedia.org/wiki/>

    SELECT ?anti_recommendation WHERE {{?anti_recommendation schema:itemReviewed wb:{record_key}}}
    """

    anti_recommendation_node = next(
        ArkgBuilderPipeline()  # type: ignore[arg-type]
        .construct_graph(anti_recommendation_graph)
        .query(query=query)
    )

    assert (
        anti_recommendation_node["anti_recommendation"].value
        == ArkgInstance.anti_recommendation_iri(
            anti_recommendation_key.replace(" ", "_")
        ).value
    )
