from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class LogEntry:
    value: Any
    term: int
    commited: bool


# TODO: Can test figure 8 in the paper


class AppendEntriedFailedError(Exception):
    pass


class RaftLog:
    def __init__(self) -> None:
        self._items: list[LogEntry] = []

    def __repr__(self) -> str:
        return str(self._items)

    def get(self, index: int) -> Optional[LogEntry]:

        if index <= 0:
            raise IndexError("Only indexes > 0 supported")

        return self._items[index - 1]

    def append_entry(
        self, prev_log_index: int, prev_log_term: int, term: int, entries: list[Any]
    ) -> None:
        entries = [
            LogEntry(value=value, term=term, commited=False) for value in entries
        ]

        if prev_log_index == 0:
            self._items.extend(entries)
            return

        prev_log_entry = self.get(prev_log_index)

        if prev_log_entry is None:
            raise AppendEntriedFailedError("Previous entry in the log is None")

        if prev_log_term != prev_log_entry.term:
            raise AppendEntriedFailedError(
                "Term of previous entry in the log is different from the append request"
            )

        # Rule 3: If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
        new_item_index = prev_log_index + 1
        existing_entry = self.get(new_item_index)
        if existing_entry is not None and existing_entry.term != term:
            self.delete_existing_from(new_item_index)

        self._items.extend(entries)

    @property
    def last_term(self) -> int:
        if self.last_index == 0:
            return 0

        return self._items[self.last_index - 1].term

    @property
    def last_index(self) -> int:
        return len(self._items)

    @property
    def latest_commit(self) -> int:
        return len([item for item in self._items if item.commited])

    @property
    def max_term(self) -> int:
        if len(self._items) == 0:
            return -1

        return max([item.term for item in self._items])

    def commit_entry(self, index: int) -> None:
        # TODO: Assert all preceeding entries are also commited first
        entry = self.get(index)
        assert entry is not None, "Can not commit emptry entry"

        entry.commited = True

    def delete_existing_from(self, index: int) -> None:
        self._items = self._items[: index - 1]
