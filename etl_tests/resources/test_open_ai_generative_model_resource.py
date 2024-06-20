from langchain.schema.runnable import RunnableSequence
from pytest_mock import MockFixture

from etl.models import wikipedia
from etl.models.types import ModelResponse
from etl.resources import OpenAiGenerativeModelResource


def test_enrich_records(
    session_mocker: MockFixture,
    openai_generative_model_resource: OpenAiGenerativeModelResource,
    article: wikipedia.Article,
    openai_model_response: ModelResponse,
    article_with_summary: wikipedia.Article,
) -> None:
    """Test that OpenAiGenerativeModelResource.enrich_records returns enriched Records."""

    # Mock RunnableSequence.invoke and return a ModelResponse
    session_mocker.patch.object(
        RunnableSequence, "invoke", return_value=openai_model_response
    )

    assert (
        openai_generative_model_resource.enrich_record(record=article).summary
        == article_with_summary.summary
    )
