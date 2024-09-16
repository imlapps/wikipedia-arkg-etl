from dataclasses import dataclass
from pathlib import Path
from typing import Self, cast

import langchain_community.vectorstores as langchain
from langchain.docstore.document import Document
from langchain_community.vectorstores import FAISS
from langchain_community.vectorstores.utils import DistanceStrategy

from etl.models.types.documents_limit import DocumentsLimit
from etl.models.types.model_query import ModelQuery
from etl.models.types.open_ai_embedding_model_name import OpenAiEmbeddingModelName
from etl.models.types.score_threshold import ScoreThreshold
from etl.pipelines import OpenaiEmbeddingPipeline
from etl.resources import OpenaiSettings
from etl.resources.output_config import OutputConfig


class VectorStore:
    """A store that contains vector embeddings."""

    @dataclass(frozen=True)
    class Descriptor:
        """
        A dataclass that holds data needed to load a vector store from local storage.

        A Descriptor contains:
            - directory_path: The Path of the directory that holds the vector store.
            - cache_directory_path: The Path of the directory that holds cached vector embeddings.
            - embedding_model_name: The name of the embedding model used to create the vector embeddings.
        """

        directory_path: Path
        cache_directory_path: Path
        embedding_model_name: OpenAiEmbeddingModelName

    def __init__(
        self,
        *,
        directory_path: Path,
        cache_directory_path: Path,
        store: langchain.VectorStore,
        embedding_model_name: OpenAiEmbeddingModelName,
    ) -> None:
        self.__store = store
        self.__directory_path = directory_path
        self.__cache_directory_path = cache_directory_path
        self.__embedding_model_name = embedding_model_name

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):  # noqa: ANN001
        del self.__store

    @classmethod
    def create(
        cls,
        *,
        documents: tuple[Document, ...],
        openai_settings: OpenaiSettings,
        output_config: OutputConfig,
    ) -> Self:
        """Return a VectorStore that contains a vector store created from an OpenaiEmbeddingPipeline."""

        parsed_output_config = output_config.parse()

        return cls(
            store=OpenaiEmbeddingPipeline(
                openai_settings=openai_settings,
                openai_embeddings_cache_directory_path=parsed_output_config.openai_embeddings_cache_directory_path,
            ).create_vector_store(documents=documents),
            embedding_model_name=openai_settings.embedding_model_name,
            directory_path=parsed_output_config.openai_embeddings_directory_path,
            cache_directory_path=parsed_output_config.openai_embeddings_cache_directory_path,
        )

    @classmethod
    def open(cls, descriptor: Descriptor) -> Self:
        """Return a VectorStore that contains a vector store loaded from local storage."""

        return cls(
            store=FAISS.load_local(
                folder_path=str(descriptor.directory_path),
                embeddings=OpenaiEmbeddingPipeline.create_embedding_model(
                    openai_embedding_model_name=descriptor.embedding_model_name,
                    openai_embeddings_cache_directory_path=descriptor.cache_directory_path,
                ),
                allow_dangerous_deserialization=True,
            ),
            directory_path=descriptor.directory_path,
            cache_directory_path=descriptor.cache_directory_path,
            embedding_model_name=descriptor.embedding_model_name,
        )

    @property
    def descriptor(self) -> Descriptor:
        """The handle of the VectorStore."""

        return VectorStore.Descriptor(
            directory_path=self.__directory_path,
            cache_directory_path=self.__cache_directory_path,
            embedding_model_name=self.__embedding_model_name,
        )

    def save_local(self) -> None:
        """Save the vector store to local storage."""

        cast(FAISS, self.__store).save_local(str(self.__directory_path))

    def similarity_search_with_score(
        self,
        *,
        query: ModelQuery,
        k: DocumentsLimit,
        score_threshold: ScoreThreshold,
        distance_strategy: DistanceStrategy,
    ) -> tuple[tuple[Document, float], ...]:
        """
        Run a similarity search on vector_store.

        Return a tuple of (Document, float) tuples that consist of
        Documents that are most similar to query, and their similarity scores in float.
        """

        return tuple(
            self.__store.similarity_search_with_score(
                k=k,
                query=query,
                distance_strategy=distance_strategy,
                score_threshold=score_threshold,
            )
        )
