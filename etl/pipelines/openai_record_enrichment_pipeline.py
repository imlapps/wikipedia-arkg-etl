from typing import override

from langchain.prompts import PromptTemplate
from langchain.schema import StrOutputParser
from langchain.schema.runnable import RunnablePassthrough, RunnableSerializable
from langchain_openai import ChatOpenAI

from etl.models import Record, RecordKeys, wikipedia
from etl.models.types import ModelQuery, ModelResponse, RecordKey
from etl.pipelines import RecordEnrichmentPipeline
from etl.resources import OpenaiSettings


class OpenaiRecordEnrichmentPipeline(RecordEnrichmentPipeline):
    """
    A concrete implementation of RecordEnrichmentPipeline.

    Uses OpenAI's generative AI models to enrich Records.
    """

    def __init__(self, openai_settings: OpenaiSettings) -> None:
        self.__openai_settings = openai_settings
        self.__template = """\
                Keep the answer as concise as possible.
                Question: {question}
                """

    def __create_question(self, record_key: RecordKey) -> ModelQuery:
        """Return a question for an OpenAI model."""

        return f"In 5 sentences, give a summary of {RecordKeys.to_prompt_friendly(record_key)}'s Wikipedia entry."

    def __create_chat_model(self) -> ChatOpenAI:
        """Return an OpenAI chat model."""

        return ChatOpenAI(
            name=str(self.__openai_settings.generative_model_name.value),
            temperature=self.__openai_settings.temperature,
        )

    def __build_chain(self, model: ChatOpenAI) -> RunnableSerializable:
        """Build a chain that consists of an OpenAI prompt, large language model and an output parser."""

        prompt = PromptTemplate.from_template(self.__template)

        return {"question": RunnablePassthrough()} | prompt | model | StrOutputParser()

    def __generate_response(
        self, *, question: ModelQuery, chain: RunnableSerializable
    ) -> ModelResponse:
        """Invoke the OpenAI large language model and generate a response."""

        return str(chain.invoke(question))

    @override
    def enrich_record(self, record: Record) -> Record:
        """
        Return a wikipedia.Article that has been enriched with a summary
        from OpenAI's generative AI models.
        """

        return wikipedia.Article.from_record(
            record=record,
            summary=self.__generate_response(
                question=self.__create_question(record.key),
                chain=self.__build_chain(self.__create_chat_model()),
            ),
        )
