import pytest

from pyraft.log import Command, Instruction, LogEntry, RaftLog
from pyraft.message import AppendEntries, RequestVote, RequestVoteResponse
from pyraft.state import MachineState, RaftMachine


def test_num_votes_received_calculates_correctly():
    machine = RaftMachine(0, 5)

    assert machine.num_votes_received == 0
    machine.add_vote(2)
    machine.add_vote(1)

    assert machine.num_votes_received == 2


def test_num_votes_duplicate_votes_not_counted():
    machine = RaftMachine(0, 5)

    machine.add_vote(1)
    machine.add_vote(1)

    assert machine.num_votes_received == 1


def test_num_votes_resets_on_new_candidacy():
    machine = RaftMachine(0, 5)

    machine.add_vote(1)
    machine.add_vote(2)
    machine.attempt_candidacy()

    assert machine.num_votes_received == 1
    assert machine.votes[machine.server_id]


def test_vote_majority_calculation():
    machine = RaftMachine(0, 5)

    machine.add_vote(0)
    machine.add_vote(1)
    assert not (machine.has_majority or machine.is_leader)
    machine.add_vote(2)
    assert machine.has_majority and machine.is_leader


def test_state_is_candidate():
    machine = RaftMachine(0, 5)
    machine.attempt_candidacy()
    assert machine.state == MachineState.CANDIDATE
    assert machine.is_candidate


def test_state_is_leader():
    machine = RaftMachine(0, 5)
    machine.convert_to_leader()
    assert machine.state == MachineState.LEADER
    assert machine.is_leader


def test_state_is_follower():
    machine = RaftMachine(0, 5)
    assert machine.state == MachineState.FOLLOWER
    assert machine.is_follower


def test_leader_conversion_state_change():
    machine = RaftMachine(0, 5)
    machine.convert_to_leader()
    assert machine.is_leader


def test_leader_conversion_clock_reset():
    machine = RaftMachine(0, 5)
    machine.handle_tick()
    machine.handle_tick()
    assert machine.clock == 2
    machine.convert_to_leader()
    assert machine.clock == 0


def test_leader_conversion_indexes_reset():
    machine = RaftMachine(0, 3)

    assert machine.next_index[1] == 1
    machine.log.append_entry(0, 0, [LogEntry(1, Command(Instruction.SET, "foo", 1))])
    machine.match_index[1] += 1
    assert machine.match_index[1] == 1
    machine.convert_to_leader()
    assert machine.next_index[1] == 2
    assert machine.match_index[1] == 0


def test_increment_clock_until_timeout():
    machine = RaftMachine(0, 1)
    machine.election_timeout = 2

    assert machine.clock == 0
    assert machine.is_follower

    machine.increment_clock()
    assert machine.clock == 1
    assert machine.is_follower

    machine.increment_clock()
    assert machine.clock == 0
    assert machine.is_candidate


def test_incement_clock_no_timeout_for_leader():
    machine = RaftMachine(0, 1)
    machine.convert_to_leader()
    machine.election_timeout = 2

    machine.increment_clock()
    machine.increment_clock()
    machine.increment_clock()

    assert machine.clock == 3
    assert machine.clock > machine.election_timeout
    assert machine.is_leader


def test_attempting_candidacy_converts_to_candidate():
    machine = RaftMachine(0, 3)
    machine.attempt_candidacy()
    assert machine.is_candidate


def test_attempting_candidacy_votes_for_self():
    machine = RaftMachine(0, 3)
    assert machine.num_votes_received == 0
    machine.attempt_candidacy()
    assert machine.num_votes_received == 1


def test_attempting_candidacy_resets_clock():
    machine = RaftMachine(0, 3)
    machine.increment_clock()
    machine.attempt_candidacy()
    assert machine.clock == 0


def test_attempting_candidacy_increments_term():
    machine = RaftMachine(0, 3)
    term = machine.current_term
    machine.attempt_candidacy()

    assert machine.current_term == term + 1


def test_heartbeat_calculation():
    machine = RaftMachine(0, 3)

    for _ in range(machine.heartbeat_freq):
        machine.increment_clock()

    assert machine.is_hearbeat_tick


def test_election_start_calculation():
    machine = RaftMachine(0, 3)

    machine.attempt_candidacy()
    assert machine.is_election_start


def test_clock_reset():
    machine = RaftMachine(0, 3)
    machine.increment_clock()
    machine.reset_clock()
    assert machine.clock == 0


def test_demoting_to_follower_resets_state():
    machine = RaftMachine(0, 3)
    machine.convert_to_leader()
    machine.demote_to_follower()
    assert machine.is_follower


def test_demoting_to_follower_resets_clock():
    machine = RaftMachine(0, 3)
    machine.increment_clock()
    machine.attempt_candidacy()
    machine.demote_to_follower()
    assert machine.clock == 0


def test_demoting_to_follower_fails_for_follower():
    machine = RaftMachine(0, 3)
    with pytest.raises(AssertionError):
        machine.demote_to_follower()


def test_term_update_increases_current_term():
    machine = RaftMachine(0, 3)
    machine.update_term(2)
    assert machine.current_term == 2


def test_term_update_with_same_term_fails():
    machine = RaftMachine(0, 3)
    with pytest.raises(ValueError):
        machine.update_term(0)


def test_term_update_with_smaller_term_fails():
    machine = RaftMachine(0, 3)
    machine.update_term(3)

    with pytest.raises(ValueError):
        machine.update_term(2)


def test_term_update_resets_voted_for():
    machine = RaftMachine(0, 3)
    machine.voted_for = 2

    machine.update_term(2)
    assert machine.voted_for is None


def test_term_update_demotes_leader():
    machine = RaftMachine(0, 3)
    machine.convert_to_leader()

    machine.update_term(2)
    assert machine.is_follower


def test_commit_index_updates():
    machine = RaftMachine(0, 3)
    assert machine.commit_index == 0

    machine.update_commit_index(3, 4)
    assert machine.commit_index == 3

    machine.update_commit_index(5, 4)
    assert machine.commit_index == 4


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


def test_handle_tick_increments_clock():
    machine = RaftMachine(0, 3)
    res = machine.handle_tick()
    assert machine.clock == 1
    assert res is None


def test_handle_tick_triggers_election():
    machine = RaftMachine(0, 3)
    machine.election_timeout = 1
    assert isinstance(machine.handle_tick(), RequestVote)


def test_handle_tick_triggers_heartbeat():
    machine = RaftMachine(0, 3)
    machine.convert_to_leader()
    machine.election_timeout = 1000
    machine.heartbeat_freq = 2

    assert machine.handle_tick() is None
    assert isinstance(machine.handle_tick(), dict)
