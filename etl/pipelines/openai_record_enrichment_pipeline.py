from typing import override

from langchain.prompts import PromptTemplate
from langchain.schema import StrOutputParser
from langchain.schema.runnable import RunnablePassthrough, RunnableSerializable
from langchain_openai import ChatOpenAI

from etl.models import Record, wikipedia
from etl.models.types import EnrichmentType, ModelQuestion, ModelResponse, RecordKey
from etl.pipelines import RecordEnrichmentPipeline
from etl.resources import OpenaiPipelineConfig


class OpenaiRecordEnrichmentPipeline(RecordEnrichmentPipeline):
    """
    A concrete implementation of RecordEnrichmentPipeline.

    Uses OpenAI's generative AI models to enrich Records.
    """

    def __init__(self, openai_pipeline_config: OpenaiPipelineConfig) -> None:
        self.__openai_pipeline_config = openai_pipeline_config
        self.__template = """\
                Keep the answer as concise as possible.
                Question: {question}
                """

    def __create_question(self, record_key: RecordKey) -> ModelQuestion:
        """Return a question for an OpenAI model."""

        record_key_with_spaces = record_key.replace("_", " ")

        match self.__openai_pipeline_config.enrichment_type:
            case EnrichmentType.SUMMARY:
                return f"In 5 sentences, give a summary of {record_key_with_spaces} based on {record_key_with_spaces}'s Wikipedia entry."
            case _:
                raise ValueError(
                    f"{self.__openai_pipeline_config.enrichment_type} is an invalid WikipediaTransform enrichment type."
                )

    def __create_chat_model(self) -> ChatOpenAI:
        """Return an OpenAI chat model."""

        return ChatOpenAI(
            name=str(
                self.__openai_pipeline_config.openai_settings.generative_model_name
            ),
            temperature=self.__openai_pipeline_config.openai_settings.temperature,
        )

    def __build_chain(self, model: ChatOpenAI) -> RunnableSerializable:
        """Build a chain that consists of an OpenAI prompt, large language model and an output parser."""

        prompt = PromptTemplate.from_template(self.__template)

        return {"question": RunnablePassthrough()} | prompt | model | StrOutputParser()

    def __generate_response(
        self, *, question: ModelQuestion, chain: RunnableSerializable
    ) -> ModelResponse:
        """Invoke the OpenAI large language model and generate a response."""

        return str(chain.invoke(question))

    @override
    def enrich_record(self, record: Record) -> Record:
        """
        Return a Record that has been enriched using OpenAI's generative AI models.

        Return the original Record if OpenAiPipelineConfig.enrichment_type is not a field of Record.
        """

        if self.__openai_pipeline_config.enrichment_type not in record.model_fields:
            return record

        return wikipedia.Article(
            **(
                record.model_dump(by_alias=True)
                | {
                    self.__openai_pipeline_config.enrichment_type: self.__generate_response(
                        question=self.__create_question(record.key),
                        chain=self.__build_chain(self.__create_chat_model()),
                    )
                }
            )
        )
