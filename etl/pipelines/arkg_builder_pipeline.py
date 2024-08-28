from typing import Annotated

from pydantic import Field
from pyoxigraph import Literal, NamedNode, Quad, Store

from etl.models import RDF_TYPE, WIKIPEDIA_BASE_URL, ArkgInstance, Schema
from etl.models.types import AntiRecommendationKey, RecordKey


class ArkgBuilderPipeline:
    """
    A pipeline to build Anti-Recommendation Knowledge Graphs.

    Constructs a RDF Store from a tuple of anti-recommendation graphs.
    """

    def __init__(self) -> None:

        self.__store = Store()

    def __add_anti_recommendation_quads_to_store(
        self,
        *,
        anti_recommendation_keys: tuple[AntiRecommendationKey, ...],
        item_reviewed: Annotated[
            str, Field(min_length=1, json_schema_extra={"strip_whitespace": True})
        ],
    ) -> None:
        """
        Add `anti-recommendation` Quads to the RDF Store.

        `item_reviewed` is the IRI of the item that is being anti-recommended.

        Each anti_recommendation_key has 2 Quad expressions in this method:
        - a `type` Quad that expresses the type of Review the anti_recommendation_key belongs to.
        - an `item-reviewed` Quad that relates the anti_recommendation_key to item that is being anti-recommended.
        """
        for anti_recommendation_key in anti_recommendation_keys:

            anti_recommendation_instance = ArkgInstance.anti_recommendation_iri(
                record_key=anti_recommendation_key.replace(" ", "_")
            )

            self.__store.add(
                Quad(
                    anti_recommendation_instance,
                    RDF_TYPE,
                    Schema.RECOMMENDATION,
                )
            )
            self.__store.add(
                Quad(
                    anti_recommendation_instance,
                    Schema.ITEMREVIEWED,
                    NamedNode(item_reviewed),
                )
            )

    def construct_graph(
        self,
        graphs: tuple[tuple[RecordKey, tuple[AntiRecommendationKey, ...]], ...],
    ) -> Store:
        """
        Return a RDF Store populated with anti_recommendation_graphs.

        Each record_key in the anti_recommendation_graph has 3 Quad expressions in this method:
        - a `type` Quad that expresses the type of CreativeWork the record_key belongs to.
        - a `title` Quad that expresses the title of the record_key.
        - a `url` Quad that expresses the URL of the record_key.

        All anti-recommendations are related to a record_key via the __add_anti_recommendation_quads_to_store method.
        """

        for graph in graphs:
            record_key = graph[0].replace(" ", "_")
            wikipedia_page_url = WIKIPEDIA_BASE_URL + record_key

            self.__store.add(
                Quad(
                    NamedNode(wikipedia_page_url),
                    RDF_TYPE,
                    Schema.WEBPAGE,
                ),
            )

            self.__store.add(
                Quad(
                    NamedNode(wikipedia_page_url),
                    Schema.TITLE,
                    Literal(record_key),
                )
            )

            self.__store.add(
                Quad(
                    NamedNode(wikipedia_page_url),
                    Schema.URL,
                    Literal(wikipedia_page_url),
                )
            )

            self.__add_anti_recommendation_quads_to_store(
                anti_recommendation_keys=graph[1], item_reviewed=wikipedia_page_url
            )

        return self.__store
