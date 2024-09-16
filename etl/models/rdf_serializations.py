from etl.models.types import RdfFileExtension, RdfMimeType, RdfSerializationName

"""
A frozenset containing a tuple of RDF serializations.

Each tuple consists of:
    - The `name` of the RDF serialization.
    - The `mime type` of the RDF serialization.
    - The `extension` of files that contain this RDF serialization.
"""
rdf_serializations: frozenset[
    tuple[RdfSerializationName, RdfMimeType, RdfFileExtension]
] = frozenset(
    [
        ("n_triples", RdfMimeType.N_TRIPLES, ".nt"),
        ("n_quads", RdfMimeType.N_QUADS, ".nq"),
        ("turtle", RdfMimeType.TURTLE, ".ttl"),
        ("trig", RdfMimeType.TRIG, ".trig"),
        ("rdf_xml", RdfMimeType.RDF_XML, ".rdf"),
    ]
)
