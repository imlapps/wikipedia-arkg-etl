from etl.models.types import RdfMimeType

"""###An enum of RDF MIME types for an ARKG serialization."""

rdf_serializations = frozenset(
    [
        ("n_triples", RdfMimeType.N_TRIPLES, ".nt"),
        ("n_quads", RdfMimeType.N_QUADS, ".nq"),
        ("turtle", RdfMimeType.TURTLE, ".ttl"),
        ("trig", RdfMimeType.TRIG, ".trig"),
        ("rdf_xml", RdfMimeType.RDF_XML, ".rdf"),
    ]
)
