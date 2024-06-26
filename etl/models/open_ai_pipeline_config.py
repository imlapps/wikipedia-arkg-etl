from dagster import Config, EnvVar

from etl.models.open_ai_settings import OpenAiSettings
from etl.models.types import EnrichmentType, RecordType


class OpenAiPipelineConfig(Config):  # type: ignore
    """A Config subclass that holds the shared parameters of OpenAI Pipelines."""

    openai_settings: OpenAiSettings
    record_type: RecordType
    enrichment_type: EnrichmentType