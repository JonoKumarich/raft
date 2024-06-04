import pytest

from pyraft.log import Command, Instruction, LogEntry, RaftLog
from pyraft.message import (
    AppendEntries,
    AppendEntriesResponse,
    RequestVote,
    RequestVoteResponse,
)
from pyraft.state import MachineState, RaftMachine


def test_heartbeat_doesnt_change_state():
    m = RaftMachine(0, 3)
    m.convert_to_leader()
    m.update_term(1)
    next_index = m.next_index
    match_index = m.match_index
    res = AppendEntriesResponse(
        server_id=1, uuid=None, term=m.current_term, success=True
    )
    m.handle_append_entries_response(res)
    assert next_index == m.next_index
    assert match_index == m.match_index


def test_fail_decremrements_next_index(): ...


def test_success_increments_indexes(): ...
