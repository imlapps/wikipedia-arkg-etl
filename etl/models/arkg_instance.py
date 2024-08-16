from pyoxigraph import NamedNode
from .types import RecordKey


class ArkgInstance:
    @staticmethod
    def anti_recommendation_iri(record_key: RecordKey) -> NamedNode:
        return NamedNode(
            f"http://imlapps.github.io/anti-recommender/anti-recommendation/{record_key}"
        )
