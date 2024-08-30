from dagster import ConfigurableResource
from langchain_community.vectorstores.utils import DistanceStrategy

from etl.models.types import ScoreThreshold
from etl.resources import OpenaiSettings


class RetrievalAlgorithmSettings(ConfigurableResource):  # type: ignore[misc]

    distance_strategy: DistanceStrategy
    score_threshold: ScoreThreshold
