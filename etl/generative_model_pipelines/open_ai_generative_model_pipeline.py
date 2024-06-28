from langchain.prompts import PromptTemplate
from langchain.schema import StrOutputParser
from langchain.schema.runnable import RunnablePassthrough, RunnableSerializable
from langchain_openai import ChatOpenAI

from etl.generative_model_pipelines import GenerativeModelPipeline
from etl.models import OpenAiPipelineConfig, Record, wikipedia
from etl.models.types import (
    EnrichmentType,
    ModelQuestion,
    ModelResponse,
    RecordKey,
    RecordType,
)


class OpenAiGenerativeModelPipeline(GenerativeModelPipeline):
    def __init__(self, openai_pipeline_config: OpenAiPipelineConfig) -> None:
        self.__openai_pipeline_config: OpenAiPipelineConfig = openai_pipeline_config
        self.__template = """
                Keep the answer as concise as possible.
                Question: {question}
                """

    def __create_question(self, record_key: RecordKey) -> ModelQuestion:
        """Return a question for an OpenAI model."""

        match self.__openai_pipeline_config.enrichment_type:
            case EnrichmentType.SUMMARY:
                return f"In 5 sentences, give a summary of {record_key} based on {record_key}'s Wikipedia entry."

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

    def enrich_record(self, record: Record) -> Record:
        """
        Return a Record that has been enriched using OpenAI models.

        Return the original Record if OpenAiResourceParams.enrichment_type is not a field of Record.
        """

        if self.__openai_pipeline_config.enrichment_type in record.model_fields:
            match self.__openai_pipeline_config.record_type:
                case RecordType.WIKIPEDIA:
                    return wikipedia.Article(
                        **(
                            record.model_dump(by_alias=True)
                            | {
                                self.__openai_pipeline_config.enrichment_type: self.__generate_response(
                                    question=self.__create_question(record.key),
                                    chain=self.__build_chain(
                                        self.__create_chat_model()
                                    ),
                                )
                            }
                        )
                    )
        return record
