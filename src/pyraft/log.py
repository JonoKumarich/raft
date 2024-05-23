from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class LogEntry:
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

    def append_entry(self, value: Any, term: int) -> None:
        assert term >= self.max_term, "Can't append an entry with a lower term"
        self.items.append(LogEntry(value=value, term=term, commited=False))

    @property
    def last_term(self) -> int:
        if self.last_index == 0:
            return 0

        return self.items[self.last_index - 1].term

    @property
    def last_index(self) -> int:
        return len(self.items)

    @property
    def latest_commit(self) -> int:
        return len([item for item in self.items if item.commited])

    @property
    def max_term(self) -> int:
        if len(self.items) == 0:
            return -1

        return max([item.term for item in self.items])

    def commit_entry(self, index: int) -> None:
        # TODO: Assert all preceeding entries are also commited first
        entry = self.get(index)
        assert entry is not None, "Can not commit emptry entry"

        entry.commited = True
        assert self.get(index).commited == True  # can remove when verified

    def delete_existing_from(self, index: int) -> None:
        self.items = self.items[: index - 1]

    def update_commit_index(self, leader_commit: int) -> None:
        pass
