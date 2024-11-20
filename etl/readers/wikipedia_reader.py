import json
from collections.abc import Iterable
from pathlib import Path
from typing import override

from unidecode import unidecode

from etl.models import wikipedia
from etl.readers import Reader


class WikipediaReader(Reader):
    """
    A concrete implementation of Reader.

    Read in Wikipedia data and yield them as wikipedia.Articles.
    """

    def __init__(self, *, data_file_paths: frozenset[Path], records_limit: int) -> None:
        self.__wikipedia_jsonl_file_paths = data_file_paths
        self.__records_limit = records_limit

    @override
    def read(self) -> Iterable[wikipedia.Article]:
        """Read in Wikipedia data and yield them as wikipedia.Articles."""

        records_count = 0

        for wikipedia_jsonl_file_path in self.__wikipedia_jsonl_file_paths:
            if wikipedia_jsonl_file_path:
                with wikipedia_jsonl_file_path.open(encoding="utf-8") as json_file:

                    for json_line in json_file:

                        if records_count == self.__records_limit:
                            break

                        record_json = json.loads(json_line)

                        if record_json["type"] != "RECORD":
                            continue

                        json_obj = json.loads(
                            unidecode(
                                json.dumps(record_json["record"], ensure_ascii=False)
                            )
                        )

                        records_count += 1
                        yield wikipedia.Article(**(json_obj["abstract_info"]))
