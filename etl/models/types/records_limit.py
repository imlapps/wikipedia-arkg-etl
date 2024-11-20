from typing import Annotated

from pydantic import Field

# Tiny type for a limit on a number of Records read.
RecordsLimit = Annotated[int, Field(default=1, ge=1)]
