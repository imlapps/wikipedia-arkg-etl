from typing import Annotated

from pydantic import Field

# Tiny type to validate strings with stripped whitespaces.
StrippedString = Annotated[str, Field(json_schema_extra={"strip_whitespace": True})]
