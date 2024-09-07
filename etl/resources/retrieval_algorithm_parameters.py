from dagster import ConfigurableResource
from langchain_community.vectorstores.utils import DistanceStrategy

from etl.models.types import ScoreThreshold


class RetrievalAlgorithmParameters(ConfigurableResource):  # type: ignore[misc]
    """A ConfigurableResource that holds the parameters of retrieval algorithms."""

    distance_strategy: DistanceStrategy
    score_threshold: ScoreThreshold
