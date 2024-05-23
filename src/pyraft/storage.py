from typing import Optional, Protocol

from pyraft.log import RaftLog


class DataStore(Protocol):

    def store_term(self, term: int) -> bool: ...

    def store_vote(self, vote: Optional[int]) -> bool: ...

    def store_log(self, log: RaftLog) -> bool: ...

    def fetch_term(self) -> int: ...

    def fetch_vote(self) -> Optional[int]: ...

    def fetch_log(self) -> RaftLog: ...


class LocalDataStore(DataStore):
    def __init__(self) -> None:
        self.term = 0
        self.vote = None
        self.log = None

    def store_term(self, term: int) -> bool:
        assert isinstance(term, int)
        self.term = term
        return True

    def store_vote(self, vote: Optional[int]) -> bool:
        assert isinstance(vote, int)
        self.vote = vote
        return True

    def store_log(self, log: RaftLog) -> bool:
        assert isinstance(log, RaftLog)
        self.log = log
        return True

    def fetch_term(self) -> int:
        return self.term

    def fetch_vote(self) -> Optional[int]:
        return self.vote

    def fetch_log(self) -> RaftLog:
        assert self.log is not None, "Log has not been set yet"
        return self.log


class SQLLiteDataStore:
    pass
