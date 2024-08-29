from pathlib import Path

from pyoxigraph import Literal, NamedNode, Quad, Store
from requests_cache import CachedSession

from etl.models import ArkgInstance
from etl.models.types import AntiRecommendationKey, RecordKey
from etl.namespaces import RDF, SCHEMA
from etl.namespaces.wd import WD


class ArkgBuilderPipeline:
    """
    A pipeline to build Anti-Recommendation Knowledge Graphs.

    Constructs a RDF Store from a tuple of anti-recommendation graphs.
    """

    def __init__(self, requests_cache_directory: Path) -> None:
        self.__requests_cache_directory = requests_cache_directory
        self.__store = Store()

    def __add_anti_recommendation_quads_to_store(
        self,
        *,
        anti_recommendation_keys: tuple[AntiRecommendationKey, ...],
        item_reviewed: NamedNode,
    ) -> None:
        """
        Add `anti-recommendation` Quads to the RDF Store.

        `item_reviewed` is the IRI of the item that is being anti-recommended.

        Each anti_recommendation_key has 2 Quad expressions in this method:
        - a `type` Quad that expresses the type of Review the anti_recommendation_key belongs to.
        - an `item-reviewed` Quad that relates the anti_recommendation_key to item that is being anti-recommended.
        """
        for anti_recommendation_key in anti_recommendation_keys:

            anti_recommendation_iri = ArkgInstance.anti_recommendation_iri(
                record_key=anti_recommendation_key
            )

            self.__store.add(
                Quad(
                    anti_recommendation_iri,
                    RDF.TYPE,
                    SCHEMA.RECOMMENDATION,
                )
            )
            self.__store.add(
                Quad(
                    anti_recommendation_iri,
                    SCHEMA.ITEMREVIEWED,
                    item_reviewed,
                )
            )

    def __get_wikidata_iri(self, record_key: RecordKey) -> NamedNode:
        """Return an RDF node that contains the Wikidata IRI of record_key."""

        self.__requests_cache_directory.mkdir(exist_ok=True)

        session = CachedSession(
            cache_name=self.__requests_cache_directory / "wikidata_identifiers",
            expire_after=3600,
        )

        response = session.get(
            f"https://en.wikipedia.org/w/api.php?action=query&prop=pageprops&titles={record_key}&format=json"
        )

        wikidata_identifier = next(
            iter(dict(response.json()["query"]["pages"]).values())
        )["pageprops"]["wikibase_item"]

        return NamedNode(WD.BASE_IRI.value + wikidata_identifier)

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
            record_key = graph[0]
            record_key_wikidata_iri = self.__get_wikidata_iri(record_key)

            self.__store.add(
                Quad(
                    record_key_wikidata_iri,
                    RDF.TYPE,
                    SCHEMA.WEBPAGE,
                ),
            )

            self.__store.add(
                Quad(
                    record_key_wikidata_iri,
                    SCHEMA.TITLE,
                    Literal(record_key),
                )
            )

            self.__store.add(
                Quad(
                    record_key_wikidata_iri,
                    SCHEMA.URL,
                    Literal(record_key_wikidata_iri.value),
                )
            )

            self.__add_anti_recommendation_quads_to_store(
                anti_recommendation_keys=graph[1],
                item_reviewed=record_key_wikidata_iri,
            )

        return self.__store
