from pyoxigraph import NamedNode

from .types import RecordKey


class ArkgInstance:
    """A class to create ARKG Instances."""

    @staticmethod
    def anti_recommendation_iri(record_key: RecordKey) -> NamedNode:
        """
        Return a NamedNode for an ARKG.
        The base IRI for the NamedNode is `http://imlapps.github.io/anti-recommender/anti-recommendation/`
        """
        return NamedNode(
            f"http://imlapps.github.io/anti-recommender/anti-recommendation/{record_key}"
        )
