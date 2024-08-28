from pyoxigraph import NamedNode

from .types import RecordKey
from etl.namespaces import ARKG


class ArkgInstance:
    """A class to create ARKG Instances."""

    @staticmethod
    def anti_recommendation_iri(record_key: RecordKey) -> NamedNode:
        """
        Return a NamedNode for an ARKG.
        """
        return NamedNode(ARKG.BASE_IRI.value + record_key)
