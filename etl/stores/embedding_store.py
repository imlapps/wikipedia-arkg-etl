from pathlib import Path
from typing import Self, cast
from dataclasses import dataclass

from langchain.docstore.document import Document
from langchain_community.vectorstores import VectorStore, FAISS
from langchain_core.embeddings import Embeddings

from etl.pipelines import OpenaiEmbeddingPipeline
from etl.resources import OpenaiSettings
from etl.resources.output_config import OutputConfig


class EmbeddingStore:
    """A database that holds an embedding store."""

    @dataclass(frozen=True)
    class Descriptor:
        """A dataclass that holds the Path of the resource stored in database."""

        embedding_model: Embeddings
        embedding_store_directory_path: Path

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):  # noqa: ANN001
        del self.__embedding_store

    def __init__(
        self,
        *,
        embedding_store: VectorStore,
        embedding_store_directory_path: Path,
        embedding_model: Embeddings,
    ) -> None:
        self.__embedding_store = embedding_store
        self.__embedding_store_directory_path = embedding_store_directory_path
        self.__embedding_model = embedding_model

    @classmethod
    def create(
        cls,
        *,
        documents: tuple[Document, ...],
        openai_settings: OpenaiSettings,
        output_config: OutputConfig,
    ) -> Self:
        """
        Return an EmbeddingDatabase that contains an embedding store created by an OpenaiEmbeddingPipeline.
        """

        parsed_output_config = output_config.parse()

        openai_embedding_pipeline = OpenaiEmbeddingPipeline(
            openai_settings=openai_settings,
            openai_embeddings_cache_directory_path=parsed_output_config.openai_embeddings_cache_directory_path,
        )
        embedding_store = cast(
            FAISS,
            openai_embedding_pipeline.create_embedding_store(documents=documents),
        )

        embedding_store_directory_path = (
            parsed_output_config.openai_embeddings_directory_path
        )

        embedding_store.save_local(str(embedding_store_directory_path))

        return cls(
            embedding_store=embedding_store,
            embedding_store_directory_path=embedding_store_directory_path,
            embedding_model=openai_embedding_pipeline.create_embedding_model(),
        )

    @classmethod
    def open(cls, descriptor: Descriptor) -> Self:
        """
        Return an EmbeddingDatabase with an embedding store created by an OpenaiEmbeddingPipeline,
        and an embeddings_cache_directory that is set to descriptor.
        """

        return cls(
            embedding_store=FAISS.load_local(
                folder_path=str(descriptor.embedding_store_directory_path),
                embeddings=descriptor.embedding_model,
            ),
            embedding_store_directory_path=descriptor.embedding_store_directory_path,
            embedding_model=descriptor.embedding_model,
        )

    @property
    def descriptor(self) -> Descriptor:
        """The handle of the embedding store in the EmbeddingDatabase."""

        return EmbeddingStore.Descriptor(
            embedding_store_directory_path=self.__embedding_store_directory_path,
            embedding_model=self.__embedding_model,
        )

    @property
    def embedding_store(
        self,
    ) -> VectorStore:
        return self.__embedding_store
