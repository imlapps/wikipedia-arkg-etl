from typing import Annotated
from pydantic import Field
from pyoxigraph import Literal, NamedNode, Quad, Store

from etl.models import WIKIPEDIA_BASE_URL, RDF_TYPE, ArkgInstance, ArkgSchema
from etl.models.types import AntiRecommendationKey, RecordKey


class ArkgBuilderPipeline:
    """
    A pipeline to build Anti-Recommendation Knowledge Graphs.

    Constructs a RDF store from a tuple of anti-recommendation graphs.
    """

    def __init__(self) -> None:

        self.__store = Store()

    def __add_anti_recommendation_quads_to_store(
        self,
        *,
        anti_recommendation_keys: tuple[AntiRecommendationKey, ...],
        wikipedia_page_url: Annotated[
            str, Field(min_length=1, json_schema_extra={"strip_whitespace": True})
        ],
    ) -> None:

        for anti_recommendation_key in anti_recommendation_keys:

            anti_recommendation_instance = ArkgInstance.anti_recommendation_iri(
                record_key=anti_recommendation_key.replace(" ", "_")
            )

            self.__store.add(
                Quad(
                    anti_recommendation_instance,
                    RDF_TYPE,
                    ArkgSchema.RECOMMENDATION,
                )
            )
            self.__store.add(
                Quad(
                    anti_recommendation_instance,
                    ArkgSchema.ITEMREVIEWED,
                    NamedNode(wikipedia_page_url),
                )
            )

    def construct_graph(
        self,
        graphs: tuple[tuple[RecordKey, tuple[AntiRecommendationKey, ...]], ...],
    ) -> Store:
        """Return a RDF Store populated with anti_recommendation_graphs."""

        for graph in graphs:
            record_key = graph[0].replace(" ", "_")
            wikipedia_page_url = WIKIPEDIA_BASE_URL + record_key

            self.__store.add(
                Quad(
                    NamedNode(wikipedia_page_url),
                    RDF_TYPE,
                    ArkgSchema.WEBPAGE,
                ),
            )

            self.__store.add(
                Quad(
                    NamedNode(wikipedia_page_url),
                    ArkgSchema.TITLE,
                    Literal(record_key),
                )
            )

            self.__store.add(
                Quad(
                    NamedNode(wikipedia_page_url),
                    ArkgSchema.URL,
                    Literal(wikipedia_page_url),
                )
            )

            self.__add_anti_recommendation_quads_to_store(
                anti_recommendation_keys=graph[1], wikipedia_page_url=wikipedia_page_url
            )

        return self.__store
