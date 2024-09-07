from pyoxigraph import NamedNode


class SCHEMA:
    """A class containing Schema.org RDF Nodes."""

    BASE_IRI = NamedNode("http://schema.org/")

    TITLE = NamedNode(BASE_IRI.value + "title")
    URL = NamedNode(BASE_IRI.value + "url")
    RECOMMENDATION = NamedNode(BASE_IRI.value + "Recommendation")
    ITEM_REVIEWED = NamedNode(BASE_IRI.value + "itemReviewed")
    WEB_PAGE = NamedNode(BASE_IRI.value + "WebPage")
