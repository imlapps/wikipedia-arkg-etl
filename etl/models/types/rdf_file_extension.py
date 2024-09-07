from pydantic import Field
from typing import Annotated

RdfFileExtension = Annotated[str, Field(json_schema_extra={"strip_whitespace": True})]
