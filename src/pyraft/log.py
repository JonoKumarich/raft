from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class LogEntry:
    value: Any
    term: int
    commited: bool


class Log:
    def __init__(self) -> None:
        self.items: list[LogEntry] = []

    def get(self, index: int) -> Optional[LogEntry]:
        assert index >= 0, "Index must be greater than zero"

        try:
            return self.items[index - 1]
        except IndexError:
            return None

    @property
    def last_term(self) -> int:
        if self.last_index == 0:
            return 0

        return self.items[self.last_index].term

    @property
    def last_index(self) -> int:
        return len(self.items)

    @property
    def latest_commit(self) -> int:
        return len([item for item in self.items if item.commited])
