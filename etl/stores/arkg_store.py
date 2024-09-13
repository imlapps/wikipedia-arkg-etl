from dataclasses import dataclass
from pathlib import Path
from typing import Self

import pyoxigraph as ox

from etl.models import AntiRecommendationGraphTuple
from etl.models.types import RdfMimeType, SparqlQuery
from etl.pipelines.arkg_builder_pipeline import ArkgBuilderPipeline


class ArkgStore:
    """A store for an Anti-Recommendation Knowledge Graph."""

    @dataclass(frozen=True)
    class Descriptor:
        """A dataclass that holds the Path of a directory that contains an ARKG."""

        directory_path: Path

    def __init__(self, *, store: ox.Store, directory_path: Path) -> None:
        self.__store = store
        self.__directory_path = directory_path

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:  # noqa: ANN001
        del self.__store

    @classmethod
    def create(
        cls,
        *,
        directory_path: Path,
        requests_cache_directory_path: Path,
        anti_recommendation_graphs: AntiRecommendationGraphTuple,
    ) -> Self:
        """
        Return an ArkgStore that contains an ARKG Store constructed with an ArkgBuilderPipeline.
        """

        return cls(
            store=ArkgBuilderPipeline(
                arkg_store_directory_path=directory_path,
                requests_cache_directory_path=requests_cache_directory_path,
            ).construct_graph(anti_recommendation_graphs.anti_recommendation_graphs),
            directory_path=directory_path,
        )

    @classmethod
    def open(cls, descriptor: Descriptor) -> Self:
        """Return an ArkgStore that contains a RDF Store initialized with descriptor.directory_path."""

        return cls(
            store=ox.Store(path=descriptor.directory_path),
            directory_path=descriptor.directory_path,
        )

    @property
    def descriptor(self) -> Descriptor:
        """The handle of the ArkgStore."""

        return ArkgStore.Descriptor(self.__directory_path)

    def dump(self, file_path: Path, rdf_mime_type: RdfMimeType) -> None:
        """
        Dump the ARKG Store into a file.

        The RDF serialization is determined by arkg_mime_type.
        """
        self.__store.dump(
            output=file_path,
            mime_type=rdf_mime_type,
        )

    def load(self, file_path: Path, rdf_mime_type: RdfMimeType) -> None:
        """
        Load an RDF serialization into an ARKG Store and return it.

        The RDF serialization is determined by arkg_mime_type.
        """
        self.__store.load(input=file_path, mime_type=rdf_mime_type)

    def query(self, query: SparqlQuery) -> ox.QuerySolutions | ox.QueryTriples | bool:
        """Execute a SPARQL 1.1 query."""

        return self.__store.query(query)
