from pathlib import Path

from dagster import Definitions, EnvVar, load_assets_from_modules
from langchain_community.vectorstores.utils import DistanceStrategy

from etl.models.types import EnrichmentType, MimeType

from . import assets
from .jobs import embedding_job, retrieval_job
from .resources import InputConfig, OpenaiPipelineConfig, OpenaiSettings, OutputConfig

openai_settings = OpenaiSettings(openai_api_key=EnvVar("OPENAI_API_KEY").get_value(""))

definitions = Definitions(
    assets=load_assets_from_modules([assets]),
    jobs=[embedding_job, retrieval_job],
    resources={
        "input_config": InputConfig.from_env_vars(
            data_files_directory_path_default=Path(__file__).parent.absolute()
            / "data"
            / "input"
            / "data_files",
            data_file_names_default=("mini-wikipedia.output.txt",),
            distance_strategy_default=DistanceStrategy.EUCLIDEAN_DISTANCE,
            score_threshold_default=0.5,
            mime_type_default=MimeType.TURTLE,
        ),
        "openai_settings": openai_settings,
        "openai_pipeline_config": OpenaiPipelineConfig(
            openai_settings=openai_settings,
            enrichment_type=EnvVar("ETL_ENRICHMENT_TYPE").get_value(
                default=EnrichmentType.SUMMARY
            ),
        ),
        "output_config": OutputConfig.from_env_vars(
            output_directory_path_default=Path(__file__).parent.absolute()
            / "data"
            / "output"
        ),
    },
)
