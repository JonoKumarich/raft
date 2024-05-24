from typing import Any

from pydantic import BaseModel


class AppendEntries(BaseModel):
    uuid: str
    term: int
    leader_id: int
    prev_log_index: int
    prev_log_term: int
    entries: list[Any]
    leader_commit: int


class AppendEntriesResponse(BaseModel):
    uuid: str
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
