from typing import Any

from pydantic import BaseModel


class AppendEntries(BaseModel):
    term: int
    leader_id: int
    prev_log_index: int
    prev_log_term: int
    entries: list[Any]
    leader_commit: int

    @property
    def log_entry_starting_index(self) -> index:
        return self.prev_log_index


class AppendEntriesResponse(BaseModel):
    term: int
    success: bool


class RequestVote(BaseModel):
    term: int
    candidate_id: int
    last_log_index: int
    last_log_term: int


class RequestVoteResponse(BaseModel):
    server_id: int
    term: int
    vote_granted: bool
