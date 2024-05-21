from dataclasses import dataclass
from typing import Any


@dataclass
class LogEntry:
    value: Any
    term: int


class Log:
    def __init__(self) -> None:
        self.items: list[LogEntry] = []

    @property
    def last_term(self) -> int:
        if self.last_index == 0:
            return 0

        return self.items[self.last_index].term

    @property
    def last_index(self) -> int:
        return len(self.items)
