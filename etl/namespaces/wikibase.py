from pyoxigraph import NamedNode


class WIKIBASE:
    """A class containing Wikibase RDF Nodes."""

    BASE_IRI = NamedNode("http://wikiba.se/ontology#")

    ITEM = NamedNode(BASE_IRI.value + "Item")
