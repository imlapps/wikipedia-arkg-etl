from typing import Annotated

from pydantic import Field

# Tiny type for a SPARQL query.
SparqlQuery = Annotated[
    str, Field(min_length=1, json_schema_extra={"strip_whitespace": True})
]
