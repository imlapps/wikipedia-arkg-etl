from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from dagster import ConfigurableResource, EnvVar

if TYPE_CHECKING:
    from etl.models.types import DataFileName


class InputConfig(ConfigurableResource):  # type: ignore[misc]
    """
    A ConfigurableResource that holds input values for the ETL.

    Properties include:
    - data_files_directory_path: The directory path of input data files,
    - data_file_names: A list of data file names,
    """

    @dataclass(frozen=True)
    class Parsed:
        """
        A dataclass that holds a parsed version of InputConfig's values.
        """

        data_files_directory_path: Path
        data_file_paths: frozenset[Path]

    data_files_directory_path: str
    data_file_names: list[str]

    @classmethod
    def default(
        cls,
        *,
        data_files_directory_path_default: Path,
        data_file_names_default: tuple[DataFileName, ...],
    ) -> InputConfig:
        """Return an InputConfig object using only default parameters."""

        return InputConfig(
            data_files_directory_path=str(data_files_directory_path_default),
            data_file_names=list(data_file_names_default),
        )

    @classmethod
    def from_env_vars(
        cls,
        *,
        data_files_directory_path_default: Path,
        data_file_names_default: tuple[DataFileName, ...],
    ) -> InputConfig:
        """Return an InputConfig object, with parameter values obtained from environment variables."""

        return cls(
            data_files_directory_path=EnvVar("ETL_DATA_FILES_DIRECTORY_PATH").get_value(
                str(data_files_directory_path_default)
            ),
            data_file_names=json.loads(
                str(
                    EnvVar("ETL_DATA_FILE_NAMES").get_value(
                        json.dumps(list(data_file_names_default))
                    )
                )
            ),
        )

    def parse(self) -> Parsed:
        """
        Parse the InputConfig's variables and return a Parsed dataclass.
        """

        return InputConfig.Parsed(
            data_files_directory_path=Path(self.data_files_directory_path),
            data_file_paths=frozenset(
                [
                    Path(self.data_files_directory_path) / data_file_name
                    for data_file_name in self.data_file_names
                ]
            ),
        )
