from typing import Annotated

from pydantic import Field

RdfSerializationName = Annotated[
    str, Field(json_schema_extra={"strip_whitespace": True})
]
