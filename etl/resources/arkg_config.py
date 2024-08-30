from dagster import ConfigurableResource

from etl.models.types import RdfMimeType


class ArkgConfig(ConfigurableResource):  # type: ignore[misc]
    """A ConfigurableResource that contains values used by an ARKG."""

    rdf_mime_type: RdfMimeType