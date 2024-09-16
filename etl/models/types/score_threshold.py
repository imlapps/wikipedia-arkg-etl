from typing import Annotated

from pydantic import Field

# Tiny type for a score threshold.
ScoreThreshold = Annotated[float, Field(ge=0, le=1)]
