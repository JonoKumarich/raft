import pytest

from pyraft.log import Command, Instruction, LogEntry, RaftLog
from pyraft.message import AppendEntries, RequestVote, RequestVoteResponse
from pyraft.state import MachineState, RaftMachine


def test_skips_if_not_leader():
    machine = RaftMachine(0, 3)

    response = RequestVoteResponse(server_id=1, term=0, vote_granted=True)
    assert machine.handle_request_vote_response(response) is None


def test_becoming_leader_and_sending_entries():
    machine = RaftMachine(0, 5)
    machine.attempt_candidacy()

    assert (
        machine.handle_request_vote_response(
            RequestVoteResponse(
                server_id=1, term=machine.current_term, vote_granted=True
            )
        )
        is None
    )

    assert isinstance(
        machine.handle_request_vote_response(
            RequestVoteResponse(
                server_id=2, term=machine.current_term, vote_granted=True
            )
        ),
        AppendEntries,
    )

    machine.election_timeout = 1000

    assert machine.is_leader
    assert machine.handle_tick() is None


def test_uncessful_votes_skipped():
    machine = RaftMachine(0, 5)
    machine.attempt_candidacy()

    assert (
        machine.handle_request_vote_response(
            RequestVoteResponse(
                server_id=1, term=machine.current_term, vote_granted=False
            )
        )
        is None
    )

    assert (
        machine.handle_request_vote_response(
            RequestVoteResponse(
                server_id=2, term=machine.current_term, vote_granted=False
            )
        )
        is None
    )

    assert not machine.is_leader
