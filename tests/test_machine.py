from pyraft.state import MachineState, RaftMachine


def test_follower_timeout():
    m = RaftMachine(1, 1, 5)
    start_term = m.current_term

    for _ in range(4):
        m.increment_clock()

    assert m.is_follower
    m.increment_clock()
    assert m.is_candidate
    assert m.clock == 0
    assert m.current_term == start_term + 1
    assert m.num_votes_recieved == 1
