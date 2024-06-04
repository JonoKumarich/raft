from typing import Any

from pydantic import BaseModel

from pyraft.log import LogEntry


class AppendEntries(BaseModel):
    uuid: str | None
    term: int
    leader_id: int
    prev_log_index: int
    prev_log_term: int
    entries: list[LogEntry]
    leader_commit: int


class AppendEntriesResponse(BaseModel):
    server_id: int
    uuid: str | None
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
