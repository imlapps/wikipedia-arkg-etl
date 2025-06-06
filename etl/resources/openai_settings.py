from dagster import ConfigurableResource
from pydantic import Field

from typing import Annotated 

from etl.models.types import ApiKey, OpenAiEmbeddingModelName, OpenAiGenerativeModelName

class OpenaiSettings(ConfigurableResource):  # type: ignore[misc]
    """A ConfigurableResource that holds the settings of OpenAI models."""

    base_url: Annotated[str, Field(json_schema_extra={"strip_whitespace": True})] | None = None
    openai_api_key: ApiKey = Field(default=..., description="OpenAI API key")
    embedding_model_name: OpenAiEmbeddingModelName = Field(
        default=OpenAiEmbeddingModelName.TEXT_EMBEDDING_3_LARGE
    )
    generative_model_name: OpenAiGenerativeModelName = Field(
        default=OpenAiGenerativeModelName.GPT_4O
    )
    temperature: int = Field(default=0)
