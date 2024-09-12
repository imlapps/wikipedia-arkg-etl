from pathlib import Path
from typing import Self

import pyoxigraph as ox

from dataclasses import dataclass
from etl.models import AntiRecommendationGraphTuple
from etl.models.types import RdfMimeType
from etl.pipelines.arkg_builder_pipeline import ArkgBuilderPipeline


class ArkgStore:
    """A database to store an Anti-Recommendation Knowledge Graph."""

    @dataclass(frozen=True)
    class Descriptor:
        """A dataclass that holds the Path of the resource stored in database."""

        arkg_store_path: Path

    def __init__(self, *, arkg_store: ox.Store, arkg_store_path: Path) -> None:
        self.__arkg_store = arkg_store
        self.__arkg_store_path = arkg_store_path

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:  # noqa: ANN001
        del self.__arkg_store

    @classmethod
    def create(
        cls,
        *,
        arkg_store_path: Path,
        requests_cache_directory: Path,
        anti_recommendation_graphs: AntiRecommendationGraphTuple,
    ) -> Self:
        """
        Return an ArkgDatabase that contains an ARKG Store constructed with an ArkgBuilderPipeline.
        """

        return cls(
            arkg_store=ArkgBuilderPipeline(
                arkg_store_path=arkg_store_path,
                requests_cache_directory=requests_cache_directory,
            ).construct_graph(anti_recommendation_graphs.anti_recommendation_graphs),
            arkg_store_path=arkg_store_path,
        )

    @classmethod
    def open(cls, descriptor: Descriptor) -> Self:
        """Return an ArkgDatabase that has an empty RDF Store, and an arkg_file_path that is set to descriptor."""

        return cls(
            arkg_store=ox.Store(path=descriptor.arkg_store_path),
            arkg_store_path=descriptor.arkg_store_path,
        )

    @property
    def arkg_store(self) -> ox.Store:

        return self.__arkg_store

    @property
    def descriptor(self) -> Descriptor:
        """The handle of the ARKG stored in the ArkgDatabase."""

        return ArkgStore.Descriptor(self.__arkg_store_path)

    def dump(self, arkg_file_path: Path, arkg_mime_type: RdfMimeType) -> None:
        """
        Dump the ARKG Store into a file.

        The RDF serialization is determined by arkg_mime_type.
        """
        self.__arkg_store.dump(
            output=arkg_file_path,
            mime_type=arkg_mime_type,
        )

    def load(self, arkg_file_path: Path, arkg_mime_type: RdfMimeType) -> None:
        """
        Load an RDF serialization into an ARKG Store and return it.

        The RDF serialization is determined by arkg_mime_type.
        """
        self.__arkg_store.load(input=arkg_file_path, mime_type=arkg_mime_type)
