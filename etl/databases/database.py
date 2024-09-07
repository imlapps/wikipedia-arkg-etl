from dataclasses import dataclass
from pathlib import Path


class Database:
    """A base class for a database in the Wikipedia ARKG ETL."""

    @dataclass(frozen=True)
    class Descriptor:
        """A dataclass that holds the Path of the resource stored in database."""

        path: Path
