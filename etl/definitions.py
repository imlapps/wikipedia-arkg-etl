from pathlib import Path

from dagster import Definitions, EnvVar, load_assets_from_modules
from langchain_community.vectorstores.utils import DistanceStrategy

from etl.models.types import RdfMimeType
from etl.resources.input_config import InputConfig

from . import assets
from .jobs import embedding_job, retrieval_job
from .resources import ArkgConfig, OpenaiSettings, OutputConfig, RetrievalPipelineConfig


definitions = Definitions(
    assets=load_assets_from_modules([assets]),
    jobs=[embedding_job, retrieval_job],
    resources={
        "arkg_config": ArkgConfig(
            rdf_mime_type=EnvVar("ETL_RDF_MIME_TYPE").get_value(
                default=RdfMimeType.TURTLE
            )
        ),
        "input_config": InputConfig.from_env_vars(
            data_files_directory_path_default=Path(__file__).parent.absolute()
            / "data"
            / "input"
            / "data_files",
            data_file_names_default=("mini-wikipedia.output.txt",),
        ),
        "openai_settings": OpenaiSettings(
            openai_api_key=EnvVar("OPENAI_API_KEY").get_value("")
        ),
        "output_config": OutputConfig.from_env_vars(
            output_directory_path_default=Path(__file__).parent.absolute()
            / "data"
            / "output"
        ),
        "retrieval_pipeline_config": RetrievalPipelineConfig(
            distance_strategy=EnvVar("ETL_DISTANCE_STRATEGY").get_value(
                default=DistanceStrategy.EUCLIDEAN_DISTANCE
            ),
            score_threshold=float(
                str(EnvVar("ETL_SCORE_THRESHOLD").get_value(default=str(0.5)))
            ),
        ),
    },
)
