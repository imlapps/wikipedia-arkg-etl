from abc import ABC, abstractmethod
from typing import final

from langchain.docstore.document import Document
from langchain_community.vectorstores import FAISS
from langchain_core.embeddings import Embeddings
from langchain_core.vectorstores import VectorStore


class EmbeddingPipeline(ABC):
    """An interface to build embedding model pipelines that transform Records into embeddings."""

    @abstractmethod
    def _create_embedding_model(self) -> Embeddings:
        pass

    @final
    def create_embedding_store(self, documents: tuple[Document, ...]) -> VectorStore:

        return FAISS.from_documents(
            documents=list(documents),
            embedding=self._create_embedding_model(),
        )
