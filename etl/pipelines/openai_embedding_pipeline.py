from pathlib import Path
from typing import override

from langchain.embeddings import CacheBackedEmbeddings
from langchain.storage import LocalFileStore
from langchain_core.embeddings import Embeddings
from langchain_openai import OpenAIEmbeddings

from etl.models.types.open_ai_embedding_model_name import OpenAiEmbeddingModelName
from etl.pipelines import EmbeddingPipeline
from etl.resources import OpenaiSettings


class OpenaiEmbeddingPipeline(EmbeddingPipeline):
    """
    A concrete implementation of EmbeddingPipeline.

    Uses OpenAI's embedding models to transform Records into embeddings.
    """

    def __init__(
        self,
        *,
        openai_settings: OpenaiSettings,
        openai_embeddings_cache_directory_path: Path,
    ) -> None:
        self.__openai_settings = openai_settings
        self.__openai_embeddings_cache_directory_path = (
            openai_embeddings_cache_directory_path
        )

    @override
    def _create_embedding_model(self) -> Embeddings:
        """Create and return an OpenAI embedding model."""

        return OpenaiEmbeddingPipeline.create_embedding_model(
            openai_embeddings_cache_directory_path=self.__openai_embeddings_cache_directory_path,
            openai_embedding_model_name=self.__openai_settings.embedding_model_name,
        )

    @staticmethod
    def create_embedding_model(
        *,
        openai_embeddings_cache_directory_path: Path,
        openai_embedding_model_name: OpenAiEmbeddingModelName,
    ) -> Embeddings:
        """Create and return an OpenAI embedding model."""

        openai_embeddings_cache_directory_path.mkdir(parents=True, exist_ok=True)

        openai_embeddings_model = OpenAIEmbeddings(
            model=str(openai_embedding_model_name.value)
        )

        return CacheBackedEmbeddings.from_bytes_store(
            openai_embeddings_model,
            LocalFileStore(openai_embeddings_cache_directory_path),
            namespace=openai_embeddings_model.model,
        )
