from pyraft.state import MachineState, RaftMachine

"""
Things to test:
- Term increases when election can't be settled (split vote)
- 
"""


def test_follower_timeout():
    m = RaftMachine(1, 1, 5)
    start_term = m.current_term

    assert m.clock == 0
    for _ in range(4):
        m.increment_clock()
    assert m.clock == 4

    assert m.is_follower
    m.increment_clock()
    assert m.is_candidate
    assert m.clock == 0
    assert m.current_term == start_term + 1
    assert m.num_votes_received == 1
    assert m.num_votes_received


def test_candidacy_and_vote_counter():
    m = RaftMachine(1, 5, 5)

    assert m.num_votes_received == 0
    m.attempt_candidacy()

    assert m.num_votes_received == 1
    m.add_vote(2)
    m.add_vote(2)  # Check for omnipotence
    assert m.num_votes_received == 2

    m.add_vote(3)
    # Now has majority, should be leader
    assert m.num_votes_received == 3
    assert m.is_leader

    m.demote_to_follower()
    m.attempt_candidacy()

    assert m.voted_for == m.server_id
    assert m.is_candidate
    assert m.clock == 0
    assert m.num_votes_received == 1


def test_majority_calculation():
    m = RaftMachine(1, 5, 5)
    m.attempt_candidacy()
    assert not m.has_majority
    m.add_vote(2)
    assert not m.has_majority
    m.add_vote(3)
    assert m.has_majority
    m.add_vote(4)
    assert m.has_majority
    m.demote_to_follower()
    m.attempt_candidacy()
    assert not m.has_majority
