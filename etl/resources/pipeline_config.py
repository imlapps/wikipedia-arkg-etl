from dagster import ConfigurableResource
from langchain_community.vectorstores.utils import DistanceStrategy

from etl.models.types import ScoreThreshold
from etl.resources import OpenaiSettings


class PipelineConfig(ConfigurableResource):  # type: ignore[misc]
    """A ConfigurableResource that holds the shared parameters of pipelines."""

    openai_settings: OpenaiSettings

    distance_strategy: DistanceStrategy
    score_threshold: ScoreThreshold
