from pyoxigraph import NamedNode


class SCHEMA:
    """A class containing Schema.org RDF Nodes."""

    TITLE = NamedNode("http://schema.org/title")
    URL = NamedNode("http://schema.org/url")
    RECOMMENDATION = NamedNode("http://schema.org/Recommendation")
    ITEMREVIEWED = NamedNode("http://schema.org/itemReviewed")
    WEBPAGE = NamedNode("http://schema.org/WebPage")
