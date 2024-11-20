from pathlib import Path

from dagster import Definitions, EnvVar, load_assets_from_modules
from langchain_community.vectorstores.utils import DistanceStrategy

from etl.resources.input_config import InputConfig

from . import assets
from .jobs import arkg_job, embedding_job, retrieval_job
from .resources import OpenaiSettings, OutputConfig, RetrievalAlgorithmParameters

definitions = Definitions(
    assets=load_assets_from_modules([assets]),
    jobs=[embedding_job, retrieval_job, arkg_job],
    resources={
        "input_config": InputConfig.from_env_vars(
            data_directory_path_default=Path(__file__).parent.absolute()
            / "data"
            / "input"
            / "data_files",
            data_file_names_default=("mini-wikipedia.output.txt",),
            records_limit=10,
        ),
        "openai_settings": OpenaiSettings(
            openai_api_key=EnvVar("OPENAI_API_KEY").get_value("")
        ),
        "output_config": OutputConfig.from_env_vars(
            output_directory_path_default=Path(__file__).parent.absolute()
            / "data"
            / "output"
        ),
        "retrieval_algorithm_parameters": RetrievalAlgorithmParameters(
            distance_strategy=EnvVar("ETL_DISTANCE_STRATEGY").get_value(
                default=DistanceStrategy.EUCLIDEAN_DISTANCE
            ),
            score_threshold=float(
                str(EnvVar("ETL_SCORE_THRESHOLD").get_value(default=str(0.5)))
            ),
        ),
    },
)
