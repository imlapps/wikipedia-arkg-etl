from etl.models.types import RecordKey


class RecordKeys:
    """A class that contains methods related to a RecordKey."""

    @staticmethod
    def to_prompt_friendly(record_key: RecordKey) -> RecordKey:
        """Return a record key that has been converted to a format that is well suited for LLM prompts."""

        return record_key.replace("_", " ")

    @staticmethod
    def from_prompt_friendly(record_key: RecordKey) -> RecordKey:
        """Return a record key that has been converted from a prompt friendly format."""

        return record_key.replace(" ", "_")
