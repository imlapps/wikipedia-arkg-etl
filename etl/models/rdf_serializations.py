"""###An enum of RDF MIME types for an ARKG serialization."""

rdf_serializations = frozenset(
    [
        ("n_triples", "application/n-triples", ".nt"),
        ("n_quads", "application/n-quads", ".nq"),
        ("turtle", "text/turtle", ".ttl"),
        ("trig", "application/trig", ".trig"),
        ("rdf_xml", "application/rdf+xml", ".rdf"),
    ]
)
