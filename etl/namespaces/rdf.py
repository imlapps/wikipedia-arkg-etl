from pyoxigraph import NamedNode


class RDF:
    """A class containing RDF Nodes of the default RDF namespace."""

    BASE_IRI = NamedNode("http://www.w3.org/1999/02/22-rdf-syntax-ns#")

    TYPE = NamedNode(BASE_IRI.value + "type")
