from langchain.docstore.document import Document
from langchain.embeddings import CacheBackedEmbeddings
from langchain_community.vectorstores import FAISS
from pytest_mock import MockFixture

from etl.pipelines import OpenaiEmbeddingPipeline
from etl.resources.openai_settings import OpenaiSettings
from etl.resources.output_config import OutputConfig


def test_create_vector_store(
    session_mocker: MockFixture,
    document_of_article_with_summary: Document,
    openai_embedding_pipeline: OpenaiEmbeddingPipeline,
) -> None:
    """Test that OpenaiEmbeddingPipeline.create_vector_store invokes a method that is required to create a vector store."""

    # Mock FAISS.from_documents
    mock_faiss__from_documents = session_mocker.patch.object(
        FAISS, "from_documents", return_value=None
    )

    openai_embedding_pipeline.create_vector_store(
        documents=(document_of_article_with_summary,),
    )

    mock_faiss__from_documents.assert_called_once()


def test_create_embedding_model(
    openai_settings: OpenaiSettings,
    output_config: OutputConfig,
) -> None:
    """Test that OpenaiEmbeddingPipeline.create_embedding_model returns an Embedding model that is an instance of CacheBackedEmbeddings."""

    assert isinstance(
        OpenaiEmbeddingPipeline.create_embedding_model(
            openai_embedding_model_name=openai_settings.embedding_model_name,
            openai_embeddings_cache_directory_path=output_config.parse().openai_embeddings_cache_directory_path,
        ),
        CacheBackedEmbeddings,
    )
