from pathlib import Path
from typing import Self

from pyoxigraph import Store

from etl.databases import Database
from etl.models import AntiRecommendationGraphTuple
from etl.models.types import RdfMimeType
from etl.pipelines.arkg_builder_pipeline import ArkgBuilderPipeline


class ArkgDatabase(Database):
    """A database to store an Anti-Recommendation Knowledge Graph."""

    def __init__(self, *, arkg: Store, arkg_file_path: Path):
        self.__arkg = arkg
        self.__arkg_file_path = arkg_file_path

    @property
    def descriptor(self) -> Database.Descriptor:
        """The handle of the ARKG stored in the ArkgDatabase."""

        return ArkgDatabase.Descriptor(self.__arkg_file_path)

    @classmethod
    def create(
        cls,
        *,
        arkg_file_path: Path,
        requests_cache_directory: Path,
        anti_recommendation_graphs: AntiRecommendationGraphTuple,
    ) -> Self:
        """
        Return an ArkgDatabase that contains an ARKG Store constructed with an ArkgBuilderPipeline.
        """

        return cls(
            arkg=ArkgBuilderPipeline(requests_cache_directory).construct_graph(
                anti_recommendation_graphs.anti_recommendation_graphs
            ),
            arkg_file_path=arkg_file_path,
        )

    def write(self, arkg_mime_type: RdfMimeType) -> None:
        """
        Dump the ARKG Store into a file.

        The RDF serialization is determined by arkg_mime_type.
        """
        self.__arkg.dump(
            output=self.__arkg_file_path,
            mime_type=arkg_mime_type,
        )

    @classmethod
    def open(cls, descriptor: Database.Descriptor) -> Self:
        """Return an ArkgDatabase that has an empty RDF Store, and an arkg_file_path that is set to descriptor."""

        return cls(
            arkg=Store(),
            arkg_file_path=descriptor.path,
        )

    def read(self, arkg_mime_type: RdfMimeType) -> Store:
        """
        Load an RDF serialization into an ARKG Store and return it.

        The RDF serialization is determined by arkg_mime_type.
        """
        self.__arkg.load(input=self.__arkg_file_path, mime_type=arkg_mime_type)
        return self.__arkg
