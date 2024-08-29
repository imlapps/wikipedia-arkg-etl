from enum import Enum


class RdfMimeType(str, Enum):
    """An enum of RDF MIME types for an ARKG serialization."""

    N_TRIPLES = "application/n-triples"
    N_QUADS = "application/n-quads"
    TURTLE = "text/turtle"
    APPLICATION_TURTLE = "application/turtle"
    TRIG = "application/trig"
    RDF_XML = "application/rdf+xml"
    APPLICATION_XML = "application/xml"
