from pyoxigraph import NamedNode

from etl.namespaces import ARKG

from .types import RecordKey


class ArkgInstance:
    """A class to create ARKG IRIs."""

    @staticmethod
    def anti_recommendation_iri(record_key: RecordKey) -> NamedNode:
        """
        Return an AntiRecommendation NamedNode for an ARKG.
        """
        return NamedNode(ARKG.BASE_IRI.value + record_key)
