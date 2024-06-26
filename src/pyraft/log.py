import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class MessageArgSizeError(Exception):
    def __init__(self, message: bytes, num_required_args: int) -> None:
        super().__init__(
            f"Number of items in message incorrect. Expected {num_required_args}, received {len(message.split())}. ({message})"
        )


class Instruction(Enum):
    GET = "get"
    SET = "set"
    ADD = "add"
    DEL = "del"

    @classmethod
    def from_text(cls, text: str) -> "Instruction":
        return {
            "get": cls.GET,
            "set": cls.SET,
            "add": cls.ADD,
            "del": cls.DEL,
        }[text.lower()]


@dataclass
class Command:
    instruction: Instruction
    key: str
    value: Optional[int] = None


@dataclass
class LogEntry:
    term: int
    command: Command
    id: str = field(default_factory=lambda: str(uuid.uuid1()))

    @classmethod
    def from_bytes(cls, input: bytes, term: int) -> "LogEntry":
        instruction, value = input.split(maxsplit=1)
        instruction = Instruction.from_text(instruction.decode())

        match instruction:
            case Instruction.SET | Instruction.ADD:
                try:
                    key, value = value.split(maxsplit=1)
                    command = Command(instruction, key.decode(), int(value))
                except ValueError:
                    raise MessageArgSizeError(input, 3)
            case Instruction.GET | Instruction.DEL:
                command = Command(instruction, value.decode())
            case _:
                raise ValueError(f"Command {instruction} not valid")

        return cls(term, command)

    def toJSON(self) -> dict:
        entry = self.__dict__.copy()
        entry["command"] = self.command.__dict__.copy()
        entry["command"]["instruction"] = self.command.instruction.value
        return entry


class AppendEntriesFailedError(Exception):
    pass


class RaftLog:
    def __init__(self) -> None:
        self._items: dict[str, LogEntry] = {}

    @property
    def items(self) -> list[LogEntry]:
        return list(self._items.values())

    def __repr__(self) -> str:
        return str(self.items)

    def get(self, index: int) -> LogEntry:
        if index <= 0:
            raise IndexError("Can't index <= 0, this is a one indexed log")

        return self.items[index - 1]

    def get_logs_from(self, index: int) -> list[LogEntry]:
        if index <= 0:
            raise IndexError("Can't index <= 0, this is a one indexed log")

        return self.items[index - 1 :]

    def append(self, item: LogEntry) -> None:
        assert isinstance(item, LogEntry)
        self._items[item.id] = item

    def append_entry(
        self, prev_log_index: int, prev_log_term: int, entries: list[LogEntry]
    ) -> None:
        assert (
            prev_log_index <= self.last_index
        ), "something went wrong, prev_log_index should be lower than the list length"

        if len(entries) == 0:
            return

        if prev_log_index == 0:
            for item in entries:
                self.append(item)
            return

        try:
            prev_log_entry = self.get(prev_log_index)
        except IndexError:
            raise AppendEntriesFailedError("Previous entry in the log is None")

        if prev_log_term != prev_log_entry.term:
            raise AppendEntriesFailedError(
                "Term of previous entry in the log is different from the append request"
            )

        # Rule 3: If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
        entries_to_extend = []
        for i, entry in enumerate(entries, start=1):
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

        for item in entries:
            self.append(item)

    @property
    def last_term(self) -> int:
        if self.last_index == 0:
            return 0

        return self.items[self.last_index - 1].term

    @property
    def last_index(self) -> int:
        return len(self._items)

    @property
    def last_item(self) -> LogEntry:
        return self.get(self.last_index)

    def delete_existing_from(self, index: int) -> None:
        assert 0 < index <= self.last_index
        self._items = {item.id: item for item in self.items[: index - 1]}
