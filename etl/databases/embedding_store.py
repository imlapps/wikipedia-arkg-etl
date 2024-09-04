from dataclasses import dataclass
from pathlib import Path

from langchain_community.vectorstores import VectorStore
from langchain.docstore.document import Document

from typing import Self
from etl.pipelines import OpenaiEmbeddingPipeline
from etl.pipelines.embedding_pipeline import VectorStore
from etl.resources import OpenaiSettings


class EmbeddingStore(VectorStore):
    @dataclass(frozen=True)
    class Descriptor:
        path: Path

    def __init__(
        self,
        *,
        embedding_store: VectorStore,
        embeddings_cache_directory_path: Path,
    ) -> None:
        self.__embedding_store = embedding_store
        self.__embeddings_cache_directory_path = embeddings_cache_directory_path

    @classmethod
    def create(
        cls,
        *,
        openai_settings: OpenaiSettings,
        embeddings_cache_directory_path: Path,
        documents: tuple[Document, ...],
    ) -> Self:

        return cls(
            embedding_pipeline=OpenaiEmbeddingPipeline(
                openai_settings=openai_settings,
                embeddings_cache_directory_path=embeddings_cache_directory_path,
            ).create_embedding_store(documents=documents),
            embeddings_cache_directory_path=embeddings_cache_directory_path,
        )

    @property
    def descriptor(self) -> Descriptor:

        return EmbeddingStore.Descriptor(self.__embeddings_cache_directory_path)

    @classmethod
    def open(
        cls,
        descriptor: Descriptor,
        openai_settings: OpenaiSettings,
        documents: tuple[Document, ...],
    ) -> Self:

        return cls(
            embedding_pipeline=OpenaiEmbeddingPipeline(
                openai_settings=openai_settings,
                openai_embeddings_cache_directory_path=descriptor.path,
            ).create_embedding_store(documents=documents),
            embeddings_cache_directory_path=descriptor.path,
        )

    def load(
        self,
    ) -> None:
        return self.__embedding_store
