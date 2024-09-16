from abc import ABC, abstractmethod
from typing import final

from langchain.docstore.document import Document
from langchain_community.vectorstores import FAISS, VectorStore
from langchain_core.embeddings import Embeddings


class EmbeddingPipeline(ABC):
    """An interface to build embedding pipelines that transform Records into embeddings."""

    @abstractmethod
    def _create_embedding_model(self) -> Embeddings:
        """Return an embedding model that will be used to create an embedding store."""

    @final
    def create_vector_store(
        self,
        *,
        documents: tuple[Document, ...],
    ) -> VectorStore:
        """Return a vector store that contains Document embeddings."""

        return FAISS.from_documents(
            documents=list(documents),
            embedding=self._create_embedding_model(),
        )
