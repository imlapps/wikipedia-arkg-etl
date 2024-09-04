from typing import override

from langchain.embeddings import CacheBackedEmbeddings
from langchain.storage import LocalFileStore
from langchain_core.embeddings import Embeddings
from langchain_openai import OpenAIEmbeddings

from etl.pipelines import VectorStore
from etl.resources import OpenaiSettings, OutputConfig

from pathlib import Path


class OpenaiEmbeddingPipeline(VectorStore):
    """
    A concrete implementation of EmbeddingPipeline.

    Uses OpenAI's embedding models to transform Records into embeddings.
    """

    def __init__(
        self,
        *,
        openai_settings: OpenaiSettings,
        openai_embeddings_cache_directory_path: Path
    ) -> None:
        self.__openai_settings = openai_settings
        self.__openai_embeddings_cache_directory_path: Path = (
            openai_embeddings_cache_directory_path.mkdir(parents=True, exist_ok=True)
        )

    @override
    def _create_embedding_model(self) -> Embeddings:
        """Create and return an OpenAI embedding model."""

        openai_embeddings_model = OpenAIEmbeddings(
            model=str(self.__openai_settings.embedding_model_name.value)
        )

        return CacheBackedEmbeddings.from_bytes_store(
            openai_embeddings_model,
            LocalFileStore(self.__openai_embeddings_cache_directory_path),
            namespace=openai_embeddings_model.model,
        )
