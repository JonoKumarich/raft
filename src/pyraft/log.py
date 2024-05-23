from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class LogEntry:
    uuid: str
    value: Any
    term: int
    commited: bool


class RaftLog:
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

    def commit_entry(self, index: int) -> None:

        entry = self.get(index)
        assert entry is not None, "Can not commit emptry entry"

        entry.commited = True
        assert self.get(index).commited == True  # can remove when verified
