import pytest

from pyraft.log import Command, Instruction, LogEntry, RaftLog
from pyraft.message import AppendEntries, RequestVote, RequestVoteResponse
from pyraft.state import MachineState, RaftMachine


def test_request_vote_is_invalid_lower_term():
    machine = RaftMachine(0, 3)
    machine.update_term(3)

    req = RequestVote(term=2, candidate_id=1, last_log_term=0, last_log_index=0)
    assert not machine._request_vote_valid(req)


def test_request_vote_is_invalid_already_voted():
    machine = RaftMachine(0, 3)
    machine.voted_for = 2
    req = RequestVote(term=1, candidate_id=1, last_log_term=0, last_log_index=0)
    assert not machine._request_vote_valid(req)


def test_request_vote_handles_different_log_info():
    machine = RaftMachine(0, 3)
    machine.log.append_entry(
        0,
        0,
        [
            LogEntry(1, Command(Instruction.SET, "foo", 1)),
            LogEntry(2, Command(Instruction.SET, "foo", 1)),
        ],
    )

    # Last log term out of date
    req = RequestVote(term=2, candidate_id=1, last_log_term=1, last_log_index=2)
    assert not machine._request_vote_valid(req)

    # Last log index out of date
    req = RequestVote(term=2, candidate_id=1, last_log_term=2, last_log_index=1)
    assert not machine._request_vote_valid(req)

    # Corrected request
    req = RequestVote(term=2, candidate_id=1, last_log_term=2, last_log_index=2)
    assert machine._request_vote_valid(req)


def test_handle_request_vote_rejected(mocker):
    machine = RaftMachine(0, 3)
    machine.update_term(1)
    req = RequestVote(term=2, candidate_id=1, last_log_term=2, last_log_index=2)

    validity = mocker.patch("pyraft.state.RaftMachine._request_vote_valid")
    validity.return_value = False

    assert machine.handle_request_vote(req) == RequestVoteResponse(
        server_id=machine.server_id, term=machine.current_term, vote_granted=False
    )
    assert machine.current_term == 1


def test_handle_request_vote_successful_updates_state():
    machine = RaftMachine(0, 3)
    machine.increment_clock()
    machine.log.append_entry(
        0,
        0,
        [
            LogEntry(1, Command(Instruction.SET, "foo", 1)),
            LogEntry(1, Command(Instruction.SET, "foo", 1)),
        ],
    )
    assert machine.current_term == 0
    req = RequestVote(term=2, candidate_id=1, last_log_term=2, last_log_index=2)
    res = machine.handle_request_vote(req)
    assert machine.current_term == 2
    assert machine.voted_for == 1
    assert machine.clock == 0

    assert res == RequestVoteResponse(
        server_id=machine.server_id, term=machine.current_term, vote_granted=True
    )
