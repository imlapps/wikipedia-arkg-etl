from pyoxigraph import NamedNode


class SCHEMA:
    """A class containing Schema.org RDF Nodes."""

    BASE_IRI = NamedNode("http://schema.org/")

    ABOUT = NamedNode(BASE_IRI.value + "about")
    ARTICLE = NamedNode(BASE_IRI.value + "Article")
    IN_LANGUAGE = NamedNode(BASE_IRI.value + "inLanguage")
    IS_PART_OF = NamedNode(BASE_IRI.value + "isPartOf")
    ITEM_REVIEWED = NamedNode(BASE_IRI.value + "itemReviewed")
    NAME = NamedNode(BASE_IRI.value + "name")
    RECOMMENDATION = NamedNode(BASE_IRI.value + "Recommendation")
    TITLE = NamedNode(BASE_IRI.value + "title")
    THING = NamedNode(BASE_IRI.value + "Thing")
    URL = NamedNode(BASE_IRI.value + "url")
    WEB_PAGE = NamedNode(BASE_IRI.value + "WebPage")
