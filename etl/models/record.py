from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_validator

from etl.models.types import RecordKey


class Record(BaseModel):
    """
    Pydantic Model to hold a record.
    `key` is the name of a Record.
    `url` is the URL of a Record.
    """

    key: RecordKey
    url: Annotated[
        str, Field(min_length=1, json_schema_extra={"strip_whitespace": True})
    ]

    model_config = ConfigDict(extra="allow")

    @field_validator("key")
    @classmethod
    def replace_space_with_underscore(cls, record_key: str) -> RecordKey:
        return record_key.replace(" ", "_")
