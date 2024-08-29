from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from dagster import ConfigurableResource, EnvVar


class OutputConfig(ConfigurableResource):  # type: ignore[misc]
    """
    A ConfigurableResource that holds the output directory path of the ETL.
    """

    @dataclass(frozen=True)
    class Parsed:
        """
        A dataclass that contains the output directory Path of the ETL.
        """

        output_directory_path: Path

        @property
        def openai_embeddings_directory_path(self) -> Path:
            """The Path of the directory that contains OpenAI embeddings data."""

            return self.output_directory_path / "openai_embeddings"

        @property
        def openai_embeddings_cache_directory_path(self) -> Path:
            """The Path of the openai_embeddings subdirectory that contains OpenAI embeddings cache."""

            return self.openai_embeddings_directory_path / "openai_embeddings_cache"

        @property
        def record_enrichment_directory_path(self) -> Path:
            """The Path of the directory that contains data on enriched records."""

            return self.output_directory_path / "enriched_records"

        @property
        def anti_recommendations_directory_path(self) -> Path:
            """The Path of the directory that contains data on anti-recommendations."""

            return self.output_directory_path / "anti_recommendations"

        @property
        def requests_cache_directory_path(self) -> Path:
            """The Path of the directory for HTTP request caches."""

            return self.output_directory_path / "requests_cache"

        @property
        def wikipedia_articles_with_summaries_file_path(self) -> Path:
            """The Path of the file that contains Wikipedia articles with summaries."""

            return (
                self.record_enrichment_directory_path
                / "wikipedia_articles_with_summaries.jsonl"
            )

        @property
        def wikipedia_anti_recommendations_file_path(self) -> Path:
            """The Path of the file that contains anti-recommendations of Wikipedia articles."""

            return (
                self.anti_recommendations_directory_path
                / "wikipedia_anti_recommendations.jsonl"
            )

        @property
        def wikipedia_arkg_file_path(self) -> Path:
            """The Path of the file that contains a Wikipedia ARKG."""

            return self.anti_recommendations_directory_path / "wikipedia_arkg.ttl"

    output_directory_path: str

    @classmethod
    def default(cls, *, output_directory_path_default: Path) -> OutputConfig:
        """Return an OutputConfig object."""

        return OutputConfig(output_directory_path=str(output_directory_path_default))

    @classmethod
    def from_env_vars(cls, *, output_directory_path_default: Path) -> OutputConfig:
        """Return an OutputConfig object, with an output directory path obtained from environment variables."""

        return cls(
            output_directory_path=EnvVar("ETL_OUTPUT_DIRECTORY_PATH").get_value(
                str(output_directory_path_default)
            ),
        )

    def parse(self) -> Parsed:
        """
        Return a Parsed dataclass object that contains an output_directory_path that has been converted to a Path.
        """

        return OutputConfig.Parsed(
            output_directory_path=Path(self.output_directory_path)
        )
