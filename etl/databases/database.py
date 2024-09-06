from pathlib import Path
from dataclasses import dataclass


class Database:

    @dataclass(frozen=True)
    class Descriptor:
        path: Path
