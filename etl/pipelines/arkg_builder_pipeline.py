from typing import override

from pyoxigraph import Literal, NamedNode, Quad, Store

from etl.models import WIKIPEDIA_BASE_URL
from etl.models.types import AntiRecommendationKey, Predicate, RecordKey
from etl.pipelines.kg_builder_pipeline import KgBuilderPipeline


class ArkgBuilderPipeline(KgBuilderPipeline):
    """
    A concrete implementation of KgBuilderPipeline.

    Constructs a RDF store from a tuple of anti-recommendation graphs.
    """

    def __init__(self) -> None:
        self.__base_iri = ""
        self.__store = Store()

    def __add_title_quad_to_store(self, record_key: RecordKey) -> None:
        """
        Add a RDF Quad with a `HAS_TITLE` predicate to the RDF Store.

        The subject of the Quad is the Store's base iri + record_key.
        The object of the Quad is the record_key.
        """

        self.__store.add(
            Quad(
                NamedNode(self.__base_iri + record_key.replace(" ", "_")),
                NamedNode(self.__base_iri + Predicate.HAS_TITLE.value),
                Literal(record_key),
            )
        )

    def __add_url_quad_to_store(self, record_key: RecordKey) -> None:
        """
        Add a RDF Quad with a `HAS_URL` predicate to the RDF Store.

        The subject of the Quad is the Store's base iri + record_key.
        The object of the Quad is Wikipedia's base url + record_key.
        """

        self.__store.add(
            Quad(
                NamedNode(self.__base_iri + record_key.replace(" ", "_")),
                NamedNode(self.__base_iri + Predicate.HAS_URL.value),
                Literal(WIKIPEDIA_BASE_URL + record_key),
            )
        )

    def __add_anti_recommendation_quads_to_store(
        self,
        anti_recommendation_graph: tuple[RecordKey, tuple[AntiRecommendationKey, ...]],
    ) -> None:
        """
        Add RDF Quads with `HAS_ANTI_RECOMMENDATION` predicates to the RDF Store.

        The subject of each Quad is the Store's base iri + record_key.
        The object of each Quad is the Store's base iri + anti_recommendation_key.
        """

        for anti_recommendation_key in anti_recommendation_graph[1]:
            self.__store.add(
                Quad(
                    NamedNode(
                        self.__base_iri + anti_recommendation_graph[0].replace(" ", "_")
                    ),
                    NamedNode(
                        self.__base_iri + Predicate.HAS_ANTI_RECOMMENDATION.value
                    ),
                    NamedNode(
                        self.__base_iri + anti_recommendation_key.replace(" ", "_")
                    ),
                )
            )

    @override
    def construct_graph(
        self,
        graphs: tuple[tuple[RecordKey, tuple[AntiRecommendationKey, ...]], ...],
    ) -> Store:
        """Return a RDF Store populated with anti_recommendation_graphs."""

        for graph in graphs:
            self.__add_title_quad_to_store(graph[0])
            self.__add_url_quad_to_store(graph[0])
            self.__add_anti_recommendation_quads_to_store(graph)

        return self.__store
