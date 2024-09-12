from langchain.docstore.document import Document
from langchain.embeddings import CacheBackedEmbeddings
from langchain_community.vectorstores import FAISS
from pytest_mock import MockFixture

from etl.pipelines import OpenaiEmbeddingPipeline


def test_create_embedding_store(
    session_mocker: MockFixture,
    document_of_article_with_summary: Document,
    openai_embedding_pipeline: OpenaiEmbeddingPipeline,
) -> None:
    """Test that OpenaiEmbeddingPipeline.create_embedding_store invokes a method that is required to create an embedding store."""

    # Mock FAISS.from_documents
    mock_faiss__from_documents = session_mocker.patch.object(
        FAISS, "from_documents", return_value=None
    )

    openai_embedding_pipeline.create_embedding_store(
        documents=(document_of_article_with_summary,),
    )

    mock_faiss__from_documents.assert_called_once()


def test_create_embedding_model(
    openai_embedding_pipeline: OpenaiEmbeddingPipeline,
) -> None:
    """Test that OpenaiEmbeddingPipeline._create_embedding_model returns an Embedding model that is an instance of CacheBackedEmbeddings."""

    assert isinstance(
        openai_embedding_pipeline.create_embedding_model(),
        CacheBackedEmbeddings,
    )
