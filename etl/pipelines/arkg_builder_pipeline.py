from pathlib import Path
import uuid

from pyoxigraph import Literal, NamedNode, Quad, Store
from requests_cache import CachedSession

from etl.models.types import AntiRecommendationKey, RecordKey
from etl.namespaces import ARKG, RDF, SCHEMA, WD, WIKIBASE

from etl.models import WIKIPEDIA_BASE_URL


class ArkgBuilderPipeline:
    """
    A pipeline to build Anti-Recommendation Knowledge Graphs.

    Constructs a RDF Store from a tuple of anti-recommendation graphs.
    """

    def __init__(
        self, *, arkg_store_directory_path: Path, requests_cache_directory_path: Path
    ) -> None:
        arkg_store_directory_path.mkdir(exist_ok=True, parents=True)
        requests_cache_directory_path.mkdir(parents=True, exist_ok=True)
        self.__arkg_store = Store(arkg_store_directory_path)
        self.__cached_session = CachedSession(
            cache_name=requests_cache_directory_path / "wikidata_identifiers",
            expire_after=3600,
        )

    def __add_anti_recommendation_model_to_store(
        self,
        *,
        anti_recommendation_keys: tuple[AntiRecommendationKey, ...],
        record_key_wikidata_iri: NamedNode,
    ) -> None:
        anti_recommendation_iri = ARKG.anti_recommendation_iri(
            anti_recommendation_uuid=uuid.uuid4()
        )

        self.__arkg_store.add(
            Quad(
                anti_recommendation_iri,
                RDF.TYPE,
                SCHEMA.RECOMMENDATION,
            )
        )

        self.__arkg_store.add(
            Quad(
                anti_recommendation_iri,
                SCHEMA.ABOUT,
                record_key_wikidata_iri,
            )
        )

        for anti_recommendation_key in anti_recommendation_keys:
            anti_recommendation_key_wikidata_iri = self.__get_wikidata_iri(
                record_key=anti_recommendation_key
            )
            self.__arkg_store.add(
                Quad(
                    anti_recommendation_iri,
                    SCHEMA.ITEM_REVIEWED,
                    anti_recommendation_key_wikidata_iri,
                )
            )

    def __add_wikidata_entity_model_to_store(
        self, *, record_key_wikidata_iri: NamedNode
    ) -> None:
        self.__arkg_store.add(
            Quad(
                record_key_wikidata_iri,
                RDF.TYPE,
                SCHEMA.THING,
            ),
        )

        self.__arkg_store.add(
            Quad(
                record_key_wikidata_iri,
                RDF.TYPE,
                WIKIBASE.ITEM,
            )
        )

    def __add_wikipedia_model_to_store(
        self, *, record_key: RecordKey, record_key_wikidata_iri: NamedNode
    ) -> None:
        record_key_node = NamedNode(WIKIPEDIA_BASE_URL + record_key)

        self.__arkg_store.add(
            Quad(
                record_key_node,
                RDF.TYPE,
                SCHEMA.ARTICLE,
            ),
        )

        self.__arkg_store.add(
            Quad(
                record_key_node,
                SCHEMA.ABOUT,
                record_key_wikidata_iri,
            ),
        )

        self.__arkg_store.add(
            Quad(
                record_key_node,
                SCHEMA.IN_LANGUAGE,
                Literal("en"),
            ),
        )

        self.__arkg_store.add(
            Quad(
                record_key_node, SCHEMA.IS_PART_OF, Literal("https://en.wikipedia.org/")
            )
        )

        self.__arkg_store.add(
            Quad(record_key_node, SCHEMA.NAME, Literal(record_key, language="en"))
        )

    def __get_wikidata_iri(self, record_key: RecordKey) -> NamedNode:
        """Return a RDF node that contains the Wikidata IRI of record_key."""

        response = self.__cached_session.get(
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
        for graph in graphs:
            record_key = graph[0]
            record_key_wikidata_iri = self.__get_wikidata_iri(record_key)

            self.__add_wikidata_entity_model_to_store(
                record_key_wikidata_iri=record_key_wikidata_iri
            )

            self.__add_wikipedia_model_to_store(
                record_key=record_key, record_key_wikidata_iri=record_key_wikidata_iri
            )

            self.__add_anti_recommendation_model_to_store(
                anti_recommendation_keys=graph[1],
                record_key_wikidata_iri=record_key_wikidata_iri,
            )

        return self.__arkg_store
