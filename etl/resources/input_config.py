from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from dagster import ConfigurableResource, EnvVar

if TYPE_CHECKING:
    from etl.models.types import NonBlankString as DataFileName, RecordsLimit


class InputConfig(ConfigurableResource):  # type: ignore[misc]
    """
    A ConfigurableResource that holds input values for the ETL.

    Properties include:
    - data_directory_path: The directory path of input data,
    - data_file_names: A list of data file names,
    - records_limit: The maximum number of records that should be read from a data file.
    """

    @dataclass(frozen=True)
    class Parsed:
        """
        A dataclass that holds a parsed version of InputConfig's values.
        """

        data_directory_path: Path
        data_file_paths: frozenset[Path]
        records_limit: RecordsLimit

    data_directory_path: str
    data_file_names: list[str]
    records_limit: int

    @classmethod
    def default(
        cls,
        *,
        data_directory_path_default: Path,
        data_file_names_default: tuple[DataFileName, ...],
    ) -> InputConfig:
        """Return an InputConfig object using only default parameters."""

        return InputConfig(
            data_directory_path=str(data_directory_path_default),
            data_file_names=list(data_file_names_default),
        )

    @classmethod
    def from_env_vars(
        cls,
        *,
        data_directory_path_default: Path,
        data_file_names_default: tuple[DataFileName, ...],
        records_limit: int,
    ) -> InputConfig:
        """Return an InputConfig object, with parameter values obtained from environment variables."""

        return cls(
            data_directory_path=EnvVar("ETL_DATA_DIRECTORY_PATH").get_value(
                str(data_directory_path_default)
            ),
            data_file_names=json.loads(
                str(
                    EnvVar("ETL_DATA_FILE_NAMES").get_value(
                        json.dumps(list(data_file_names_default))
                    )
                )
            ),
            records_limit=int(
                str(EnvVar("ETL_RECORDS_LIMIT").get_value(str(records_limit)))
            ),
        )

    def parse(self) -> Parsed:
        """
        Parse the InputConfig's variables and return a Parsed dataclass.
        """

        return InputConfig.Parsed(
            data_directory_path=Path(self.data_directory_path),
            data_file_paths=frozenset(
                [
                    Path(self.data_directory_path) / data_file_name
                    for data_file_name in self.data_file_names
                ]
            ),
            records_limit=self.records_limit,
        )
