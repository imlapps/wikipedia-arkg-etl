from abc import ABC, abstractmethod
from typing import final

from langchain.docstore.document import Document
from langchain_community.vectorstores import FAISS, VectorStore
from langchain_core.embeddings import Embeddings


class EmbeddingPipeline(ABC):
    """An interface to build embedding pipelines that transform Records into embeddings."""

    @abstractmethod
    def create_embedding_model(self) -> Embeddings:
        """Return an embedding model that will be used to create an embedding store."""

    @final
    def create_embedding_store(
        self,
        *,
        documents: tuple[Document, ...],
    ) -> VectorStore:
        """
        Return an embedding store that contains embeddings of Documents.
        """

        return FAISS.from_documents(
            documents=list(documents),
            embedding=self.create_embedding_model(),
        )
