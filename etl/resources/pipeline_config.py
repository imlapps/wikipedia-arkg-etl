from dagster import ConfigurableResource
from langchain_community.vectorstores.utils import DistanceStrategy

from etl.models.types import EnrichmentType, ScoreThreshold
from etl.resources import OpenaiSettings


class PipelineConfig(ConfigurableResource):  # type: ignore[misc]
    """A ConfigurableResource that holds the shared parameters of Pipelines."""

    openai_settings: OpenaiSettings
    enrichment_type: EnrichmentType
    distance_strategy: DistanceStrategy
    score_threshold: ScoreThreshold
