from pydantic import Field
from typing import Annotated

RdfSerializationName = Annotated[
    str, Field(json_schema_extra={"strip_whitespace": True})
]
