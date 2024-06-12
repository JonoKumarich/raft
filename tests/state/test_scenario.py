from pyraft.message import AppendEntries, RequestVote
from pyraft.state import RaftMachine


def test_leader_election():
    m0 = RaftMachine(0, 3)
    m1 = RaftMachine(1, 3)
    m2 = RaftMachine(2, 3)

    m0.election_timeout = 1
    vote = m0.handle_tick()
    assert isinstance(vote, RequestVote)
    assert m0.is_candidate

    m0.handle_request_vote_response(m1.handle_request_vote(vote))
    m0.handle_request_vote_response(m2.handle_request_vote(vote))

    assert m0.is_leader
    assert m0.current_term == 1


def test_basic_append_success_updates_commit_indexes():
    m0 = RaftMachine(0, 3)
    m1 = RaftMachine(1, 3)
    m2 = RaftMachine(2, 3)

    m0.update_term(1)
    m0.convert_to_leader()
    m0.heartbeat_freq = 1
    m0.election_timeout = 1000
    m0.pending_entries.put(b"set foo 1")

    ae = m0.handle_tick()
    assert isinstance(ae, dict)
    send_and_receive(m0, [m1, m2], ae)
    assert m0.commit_index == 1
    assert m1.commit_index == 0
    assert m2.commit_index == 0

    ae = m0.handle_tick()
    assert isinstance(ae, dict)
    send_and_receive(m0, [m1, m2], ae)
    assert m1.commit_index == 1
    assert m2.commit_index == 1


# Testing figure 8 of the paper
def log_replication_figure_8_setup():
    s1 = RaftMachine(0, 5)
    s2 = RaftMachine(1, 5)
    s3 = RaftMachine(2, 5)
    s4 = RaftMachine(3, 5)
    s5 = RaftMachine(4, 5)

    # a) S1 is leader and partially replicates the log entry at index 2.
    s1.update_term(1)
    s1.convert_to_leader()
    s1.election_timeout = 1000
    s1.heartbeat_freq = 1
    s1.pending_entries.put(b"set foo 1")

    ae = s1.handle_tick()
    assert isinstance(ae, dict)
    send_and_receive(s1, [s2, s3, s4, s5], ae)

    s1.current_term += 1
    ae = s1.handle_tick()
    assert isinstance(ae, dict)
    send_and_receive(s1, [s2, s3, s4, s5], ae)
    assert s3.current_term == 2

    s1.pending_entries.put(b"set bar 2")
    ae = s1.handle_tick()
    assert isinstance(ae, dict)
    send_and_receive(s1, [s2], ae)

    for machine in [s1, s2]:
        assert [item.term for item in machine.log.items] == [1, 2]

    for machine in [s3, s4, s5]:
        assert [item.term for item in machine.log.items] == [1]

    #  b) S1 crashes; S5 is elected leader for term 3 with votes from S3, S4, and itself, and accepts a different entry at log index 2.
    s5.election_timeout = 1
    s5.heartbeat_freq = 1
    rv = s5.handle_tick()
    assert isinstance(rv, RequestVote)
    send_and_receive(s5, [s2, s3, s4], rv)
    assert s5.current_term == 3
    assert s5.is_leader

    s5.pending_entries.put(b"set baz 3")
    s5.handle_tick()

    for machine in [s1, s2]:
        assert [item.term for item in machine.log.items] == [1, 2]

    for machine in [s3, s4]:
        assert [item.term for item in machine.log.items] == [1]

    assert [item.term for item in s5.log.items] == [1, 3]

    # c) S5 crashes; S1 restarts, is elected leader, and continues replication. At this point, the log entry from term 2
    # has been replicated on a majority of the servers, but it is not committed.

    s1.mock_reset()
    s1.heartbeat_freq = 1
    s1.election_timeout = 1

    # This will be rejected, but update the term to current
    rv = s1.handle_tick()
    assert isinstance(rv, RequestVote)
    send_and_receive(s1, [s2, s3, s4], rv)

    s1.election_timeout = 1
    # This will now be accepted
    rv = s1.handle_tick()
    assert isinstance(rv, RequestVote)
    send_and_receive(s1, [s2, s3, s4], rv)
    assert s1.is_leader

    for machine in [s1, s2, s3, s4]:
        assert machine.current_term == 4

    # Initial heartbeat
    ae = s1.handle_tick()
    assert isinstance(ae, dict)
    send_and_receive(s1, [s2, s3], ae)

    # Should send missing entry to s3
    ae = s1.handle_tick()
    assert isinstance(ae, dict)
    send_and_receive(s1, [s2, s3], ae)

    s1.pending_entries.put(b"set foo 4")
    s1.handle_tick()
    assert [item.term for item in s1.log.items] == [1, 2, 4]
    assert [item.term for item in s4.log.items] == [1]
    assert [item.term for item in s5.log.items] == [1, 3]

    for machine in [s2, s3]:
        assert [item.term for item in machine.log.items] == [1, 2]

    return s1, s2, s3, s4, s5


def test_log_replication_figure_8_setup():
    log_replication_figure_8_setup()


def test_log_replication_figure_8_path_a():
    s1, s2, s3, s4, s5 = log_replication_figure_8_setup()
    # d) (path A) S1 crashes, S5 could be elected leader (with votes from S2, S3, and S4) and overwrite the entry with its own entry from term 3.

    # S5 back on line and new ae causes demotion
    ae = s5.handle_tick()
    assert isinstance(ae, dict)
    send_and_receive(s5, [s1, s2, s3, s4], ae)
    assert s5.is_follower
    s5.election_timeout = 1
    s5.heartbeat_freq = 1

    # S5 times out and gets promoted to leader
    rv = s5.handle_tick()
    assert isinstance(rv, RequestVote)
    send_and_receive(s5, [s1, s2, s3, s4], rv)
    assert s5.is_leader

    # Heartbeat
    ae = s5.handle_tick()
    assert isinstance(ae, dict)
    send_and_receive(s5, [s1, s2, s3, s4], ae)

    # Should update all index 2 entries
    ae = s5.handle_tick()
    assert isinstance(ae, dict)
    send_and_receive(s5, [s1, s2, s3, s4], ae)

    for machine in [s1, s2, s3, s4, s5]:
        assert [item.term for item in machine.log.items] == [1, 3]


def test_log_replication_figure_8_path_b():
    s1, s2, s3, s4, s5 = log_replication_figure_8_setup()
    # e) (Path B) However, if S1 replicates an entry from its current term on a majority of the servers before crashing,
    # as in (e), then this entry is committed (S5 cannot win an election). At this point all preceding entries in the log are committed as well.

    # S1 replicates to all other servers
    ae = s1.handle_tick()
    assert isinstance(ae, dict)
    send_and_receive(s1, [s2, s3], ae)
    for machine in [s1, s2, s3]:
        assert [item.term for item in machine.log.items] == [1, 2, 4]

    # S5 Comes back online and gets demoted
    ae = s5.handle_tick()
    assert isinstance(ae, dict)
    send_and_receive(s5, [s1, s2, s3, s4], ae)
    assert s5.is_follower
    s5.election_timeout = 1
    s5.heartbeat_freq = 1

    # S5 cannot be elected leader
    rv = s5.handle_tick()
    assert isinstance(rv, RequestVote)
    send_and_receive(s5, [s1, s2, s3, s4], rv)
    assert s5.is_candidate


def test_multi_entry_append_updates_match_index():
    m = RaftMachine(0, 5)

    assert False


def send_and_receive(
    leader: RaftMachine,
    servers: list[RaftMachine],
    rpc: dict[int, AppendEntries] | RequestVote,
):
    for server in servers:
        if isinstance(rpc, RequestVote):
            res = server.handle_request_vote(rpc)
            leader.handle_request_vote_response(res)
        else:
            res = server.handle_append_entries(rpc[server.server_id])
            leader.handle_append_entries_response(res)
