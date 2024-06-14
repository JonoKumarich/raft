import json
from pathlib import Path
from typing import Optional, Protocol

from pyraft.log import Command, Instruction, RaftLog


class DataStore(Protocol):
    def store_term(self, term: int) -> None: ...

    def store_vote(self, vote: Optional[int]) -> None: ...

    def store_log(self, log: RaftLog) -> None: ...

    def apply_command(self, command: Command) -> int | None: ...

    def fetch_term(self) -> int: ...

    def fetch_vote(self) -> Optional[int]: ...

    def fetch_log(self) -> RaftLog: ...


class LocalDataStore(DataStore):
    def __init__(self) -> None:
        self.term = 0
        self.vote = None
        self.log = None
        self.values: dict[str, int]

    def store_term(self, term: int) -> None:
        assert isinstance(term, int)
        self.term = term

    def store_vote(self, vote: Optional[int]) -> None:
        assert isinstance(vote, int)
        self.vote = vote

    def store_log(self, log: RaftLog) -> None:
        assert isinstance(log, RaftLog)
        self.log = log

    def apply_command(self, command: Command) -> int | None:
        match command.instruction:
            case Instruction.SET:
                assert (
                    command.value is not None
                ), "Value must not be done for a SET command"
                self.values[command.key] = command.value
            case Instruction.GET:
                return self.values[command.key]
            case Instruction.ADD:
                assert (
                    command.value is not None
                ), "Value must not be done for an ADD command"
                self.values[command.key] += command.value
            case Instruction.DEL:
                del self.values[command.key]
            case _:
                raise ValueError("Command not recognized")

    def fetch_term(self) -> int:
        return self.term

    def fetch_vote(self) -> Optional[int]:
        return self.vote

    def fetch_log(self) -> RaftLog:
        assert self.log is not None, "Log has not been set yet"
        return self.log


class JSONDataStore(DataStore):
    def __init__(self, filename: str) -> None:
        self.file = Path(filename)
        self._create_base_data()

    def _create_base_data(self) -> None:
        with open(self.file, "w") as output:
            json.dump({"term": None, "vote": None, "log": None, "values": {}}, output)

    def store_term(self, term: int) -> None:
        with open(self.file, "r") as f:
            data = json.load(f)

        data["term"] = term
        with open(self.file, "w") as f:
            json.dump(data, f)

    def store_vote(self, vote: Optional[int]) -> None:
        with open(self.file, "r") as f:
            data = json.load(f)

        data["vote"] = vote
        with open(self.file, "w") as f:
            json.dump(data, f)

    # FIXME: This doesn't seem to happen properly on the leader
    def store_log(self, log: RaftLog) -> None:
        with open(self.file, "r") as f:
            data = json.load(f)

        data["log"] = [entry.toJSON() for entry in log.items]
        with open(self.file, "w") as f:
            json.dump(data, f)

    def apply_command(self, command: Command) -> int | None:
        with open(self.file, "r") as f:
            data = json.load(f)

        match command.instruction:
            case Instruction.SET:
                assert (
                    command.value is not None
                ), "Value must not be done for a SET command"
                data["values"][command.key] = command.value
            case Instruction.GET:
                return data["values"][command.key]
            case Instruction.ADD:
                assert (
                    command.value is not None
                ), "Value must not be done for an ADD command"
                data["values"][command.key] += command.value
            case Instruction.DEL:
                del data["values"][command.key]
            case _:
                raise ValueError("Command not recognized")

        with open(self.file, "w") as f:
            json.dump(data, f)

    def fetch_term(self) -> int:
        with open(self.file, "r") as f:
            data = json.load(f)

        return data["term"]

    def fetch_vote(self) -> Optional[int]:
        with open(self.file, "r") as f:
            data = json.load(f)

        return data["vote"]

    def fetch_log(self) -> RaftLog:
        with open(self.file, "r") as f:
            data = json.load(f)

        return data["log"]
