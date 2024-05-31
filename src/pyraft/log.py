from dataclasses import dataclass
from decimal import Inexact
from enum import Enum, auto
from typing import Any, Optional, Self


class MessageArgSizeError(Exception):
    def __init__(self, message: bytes, num_required_args: int) -> None:
        super().__init__(
            f"Number of items in message incorrect. Expected {num_required_args}, received {len(message.split())}. ({message})"
        )


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
                try:
                    key, value = value.split(maxsplit=1)
                    command = Command(instruction, key.decode(), int(value))
                except ValueError as e:
                    raise MessageArgSizeError(input, 3)
            case _:
                raise NotImplementedError

        return cls(term, command)

    def __repr__(self) -> str:
        return f"Term {self.term}: {self.command.instruction.value} {self.command.key} {self.command.value}"


# Errors we should have, Terms don't match. Log not caught up
class AppendEntriesFailedError(Exception):
    pass


class RaftLog:
    def __init__(self) -> None:
        self._items: list[LogEntry] = []

    def __repr__(self) -> str:
        return str(self._items)

    def get(self, index: int) -> LogEntry:
        if index <= 0:
            raise IndexError("Can't index <= 0, this is a one indexed log")

        return self._items[index - 1]

    def append(self, item: LogEntry) -> None:
        assert isinstance(item, LogEntry)
        self._items.append(item)

    def append_entry(
        self, prev_log_index: int, prev_log_term: int, entries: list[LogEntry]
    ) -> None:
        # TODO: SHould this be possible?
        assert (
            prev_log_index <= self.last_index
        ), "something went wrong, prev_log_index should be lower than the list length"

        if len(entries) == 0:
            return

        if prev_log_index == 0:
            self._items.extend(entries)
            return

        prev_log_entry = self.get(prev_log_index)

        if prev_log_entry is None:
            raise AppendEntriesFailedError("Previous entry in the log is None")

        if prev_log_term != prev_log_entry.term:
            raise AppendEntriesFailedError(
                "Term of previous entry in the log is different from the append request"
            )

        # Rule 3: If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
        entries_to_extend = []
        for i, entry in enumerate(entries, start=1):
            if entry.term < self.last_term:
                raise AppendEntriesFailedError(
                    "Can't insert a lower term than the last entry"
                )

            new_item_index = prev_log_index + i

            try:
                existing_entry = self.get(new_item_index)
            except IndexError:
                assert new_item_index > self.last_index, "This should not be possible"
                # Entry does not exist in log
                entries_to_extend.append(entry)
                continue

            if existing_entry.term != entry.term:
                self.delete_existing_from(new_item_index)
                # Can now add all other entries, as rest of list is now empty
                entries_to_extend.extend(entries[i - 1 :])
                break

            # If we arrive here, it means that the entry already exists, don't re-add it

        self._items.extend(entries_to_extend)

    @property
    def last_term(self) -> int:
        if self.last_index == 0:
            return 0

        return self._items[self.last_index - 1].term

    @property
    def last_index(self) -> int:
        return len(self._items)

    @property
    def last_item(self) -> LogEntry:
        return self.get(self.last_index)

    def delete_existing_from(self, index: int) -> None:
        assert 0 < index <= self.last_index
        self._items = self._items[: index - 1]
