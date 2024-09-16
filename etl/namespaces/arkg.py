from pyoxigraph import NamedNode

from etl.models.types import RecordKey


class ARKG:
    """A class containing RDF Nodes of the default ARKG namespace."""

    BASE_IRI = NamedNode(
        "http://imlapps.github.io/anti-recommender/anti-recommendation/"
    )

    @staticmethod
    def anti_recommendation_iri(record_key: RecordKey) -> NamedNode:
        """
        Return an AntiRecommendation NamedNode for an ARKG.
        """
        return NamedNode(ARKG.BASE_IRI.value + record_key)
