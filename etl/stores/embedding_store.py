from dataclasses import dataclass
from pathlib import Path
from typing import Self, cast

from langchain.docstore.document import Document
from langchain_community.vectorstores import FAISS, VectorStore

from etl.models.types.open_ai_embedding_model_name import OpenAiEmbeddingModelName
from etl.pipelines import OpenaiEmbeddingPipeline
from etl.resources import OpenaiSettings
from etl.resources.output_config import OutputConfig


class EmbeddingStore:

    @dataclass(frozen=True)
    class Descriptor:

        embedding_cache_directory_path: Path
        embedding_store_directory_path: Path
        embedding_model_name: OpenAiEmbeddingModelName

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):  # noqa: ANN001
        del self.__embedding_store

    def __init__(
        self,
        *,
        embedding_store: VectorStore,
        embedding_cache_directory_path: Path,
        embedding_store_directory_path: Path,
        embedding_model_name: OpenAiEmbeddingModelName,
    ) -> None:
        self.__embedding_store = embedding_store
        self.__embedding_cache_directory_path = embedding_cache_directory_path
        self.__embedding_store_directory_path = embedding_store_directory_path
        self.__embedding_model_name = embedding_model_name

    @classmethod
    def create(
        cls,
        *,
        documents: tuple[Document, ...],
        openai_settings: OpenaiSettings,
        output_config: OutputConfig,
    ) -> Self:

        parsed_output_config = output_config.parse()

        embedding_store = OpenaiEmbeddingPipeline(
            openai_settings=openai_settings,
            openai_embeddings_cache_directory_path=parsed_output_config.openai_embeddings_cache_directory_path,
        ).create_embedding_store(documents=documents)

        return cls(
            embedding_store=embedding_store,
            embedding_cache_directory_path=parsed_output_config.openai_embeddings_cache_directory_path,
            embedding_store_directory_path=parsed_output_config.openai_embeddings_directory_path,
            embedding_model_name=openai_settings.embedding_model_name,
        )

    @classmethod
    def open(cls, descriptor: Descriptor) -> Self:

        return cls(
            embedding_store=FAISS.load_local(
                folder_path=str(descriptor.embedding_store_directory_path),
                embeddings=OpenaiEmbeddingPipeline.create_embedding_model(
                    openai_embeddings_cache_directory_path=descriptor.embedding_cache_directory_path,
                    openai_embedding_model_name=descriptor.embedding_model_name,
                ),
                allow_dangerous_deserialization=True,
            ),
            embedding_cache_directory_path=descriptor.embedding_cache_directory_path,
            embedding_store_directory_path=descriptor.embedding_store_directory_path,
            embedding_model_name=descriptor.embedding_model_name,
        )

    def save_local(self) -> None:

        cast(FAISS, self.__embedding_store).save_local(
            str(self.__embedding_store_directory_path)
        )

    @property
    def descriptor(self) -> Descriptor:

        return EmbeddingStore.Descriptor(
            embedding_store_directory_path=self.__embedding_store_directory_path,
            embedding_cache_directory_path=self.__embedding_cache_directory_path,
            embedding_model_name=self.__embedding_model_name,
        )

    @property
    def embedding_store(
        self,
    ) -> VectorStore:

        return self.__embedding_store
