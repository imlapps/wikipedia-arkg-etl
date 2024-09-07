from dataclasses import dataclass
from pathlib import Path


class Database:

    @dataclass(frozen=True)
    class Descriptor:
        path: Path
