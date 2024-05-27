from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Optional, Self


class Instruction(Enum):
    GET = "get"
    SET = "set"
    ADD = "add"

    @classmethod
    def from_text(cls, text: str) -> "Instruction":
        return {
            "get": cls.GET,
            "set": cls.SET,
            "add": cls.ADD,
        }[text.lower()]


@dataclass
class Command:
    instruction: Instruction
    key: str
    value: Optional[int]


@dataclass
class LogEntry:
    term: int
    command: Command

    @classmethod
    def from_bytes(cls, input: bytes, term: int) -> "LogEntry":
        instruction, value = input.split(maxsplit=1)
        instruction = Instruction.from_text(instruction.decode())

        match instruction:
            case Instruction.SET:
                key, value = value.split(maxsplit=1)
                command = Command(instruction, key.decode(), int(value))
            case _:
                raise NotImplementedError

        return cls(term, command)

    def __repr__(self) -> str:
        return f"Term {self.term}: {self.command.instruction.value} {self.command.key} {self.command.value}"


# Errors we should have, Terms don't match. Log not caught up
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
        self, prev_log_index: int, prev_log_term: int, entries: list[LogEntry]
    ) -> None:
        if len(entries) == 0:
            return

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
        for i, entry in enumerate(entries, start=1):
            new_item_index = prev_log_index + i
            existing_entry = self.get(new_item_index)
            if existing_entry is not None and existing_entry.term != entry.term:
                self.delete_existing_from(new_item_index)
                break

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
    def max_term(self) -> int:
        if len(self._items) == 0:
            return -1

        return max([item.term for item in self._items])

    def delete_existing_from(self, index: int) -> None:
        self._items = self._items[: index - 1]
