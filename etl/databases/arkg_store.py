from dataclasses import dataclass
from pathlib import Path
from typing import Self

from pyoxigraph import Store
from etl.models import AntiRecommendationGraphTuple
from etl.models.types.rdf_mime_type import RdfMimeType
from etl.pipelines.arkg_builder_pipeline import ArkgBuilderPipeline


class ArkgStore:
    @dataclass(frozen=True)
    class Descriptor:
        path: Path

    def __init__(self, *, arkg: Store, arkg_file_path: Path):
        self.__arkg = arkg
        self.__arkg_file_path = arkg_file_path

    @property
    def descriptor(self) -> Descriptor:
        return ArkgStore.Descriptor(self.__arkg_file_path)

    @classmethod
    def create(
        cls,
        *,
        requests_cache_directory: Path,
        anti_recommendation_graphs: AntiRecommendationGraphTuple,
        arkg_file_path: Path
    ) -> Self:
        cls(
            arkg=ArkgBuilderPipeline(requests_cache_directory).construct_graph(
                anti_recommendation_graphs
            ),
            arkg_file_path=arkg_file_path,
        )

    def dump(self, rdf_mime_type: RdfMimeType) -> None:
        self.__arkg.dump(
            output=self.__arkg_file_path,
            mime_type=rdf_mime_type,
        )

    @classmethod
    def open(cls, descriptor: Descriptor) -> Self:

        return cls(
            arkg=Store(),
            arkg_file_path=descriptor.path,
        )

    def load(self, rdf_mime_type: RdfMimeType) -> Store:

        self.__arkg.load(input=self.__arkg_file_path, mime_type=rdf_mime_type)
        return self.__arkg
