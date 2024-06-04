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


def test_commit_index_majority():
    machine = RaftMachine(0, 5)
    machine.match_index[1] = 2
    machine.match_index[2] = 3
    machine.match_index[3] = 4

    assert machine.commit_index_has_majority(1)
    assert machine.commit_index_has_majority(2)
    assert machine.commit_index_has_majority(3)
    assert not machine.commit_index_has_majority(4)
