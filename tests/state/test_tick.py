import pytest

from pyraft.log import Command, Instruction, LogEntry, RaftLog
from pyraft.message import AppendEntries, RequestVote, RequestVoteResponse
from pyraft.state import MachineState, RaftMachine


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
    machine.heartbeat_freq = 3

    machine.handle_tick()
    machine.handle_tick()
    assert isinstance(machine.handle_tick(), dict)


def test_handle_tick_not_leader_doesnt_trigger_heartbeat():
    machine = RaftMachine(0, 3)
    machine.election_timeout = 1000
    machine.heartbeat_freq = 2

    assert machine.handle_tick() is None
    assert machine.handle_tick() is None


def test_handle_tick_heartbeat_empty_queue():
    machine = RaftMachine(0, 3)
    machine.convert_to_leader()
    machine.election_timeout = 1000
    machine.heartbeat_freq = 2

    machine.handle_tick()
    responses = machine.handle_tick()

    assert isinstance(responses, dict)
    assert len(responses) == machine.num_servers - 1
    for key, res in responses.items():
        assert isinstance(res, AppendEntries)
        assert res.entries == []
        assert res.uuid is None
        assert key != machine.server_id


def test_handle_tick_with_pending_adds_to_log_and_sends_response():
    machine = RaftMachine(0, 3)
    machine.update_term(2)
    machine.convert_to_leader()
    machine.election_timeout = 1000
    machine.heartbeat_freq = 2

    machine.pending_entries.put(b"set foo 1")

    assert machine.handle_tick() is None
    append_entries = machine.handle_tick()
    assert machine.log.last_index == 1
    assert machine.log.last_term == 2

    assert machine.log.last_item.command == Command(Instruction.SET, "foo", 1)

    assert isinstance(append_entries, dict)
    for _, entry in append_entries.items():
        assert isinstance(entry, AppendEntries)
        assert entry.entries == [
            LogEntry(
                machine.current_term,
                Command(Instruction.SET, "foo", 1),
                id=machine.log.last_item.id,
            ),
        ]
        assert entry.uuid is not None
        assert entry.prev_log_term == 0
        assert entry.prev_log_index == 0
        assert entry.term == 2


def test_handle_tick_heartbeat_handles_out_of_date_match_index():

    machine = RaftMachine(0, 3)
    machine.update_term(1)
    l1 = LogEntry(machine.current_term, Command(Instruction.SET, "foo", 1))
    l2 = LogEntry(machine.current_term, Command(Instruction.SET, "bar", 1))
    machine.log.append_entry(
        machine.log.last_index,
        machine.log.last_term,
        [l1, l2],
    )
    machine.convert_to_leader()
    machine.election_timeout = 1000
    machine.heartbeat_freq = 2

    machine.next_index[1] = 3
    machine.next_index[2] = 2

    assert machine.log.last_index == 2

    machine.pending_entries.put(b"set foo 2")
    machine.pending_entries.put(b"set bar 2")

    assert machine.handle_tick() is None
    res = machine.handle_tick()
    assert res is not None and not isinstance(res, RequestVote)

    assert len(res[1].entries) == 2
    assert len(res[2].entries) == 3

    assert res[2].entries[0] == l2


def test_append_backfill_entries_correct_prev_values():
    machine = RaftMachine(0, 3)
    machine.update_term(1)
    l1 = LogEntry(machine.current_term, Command(Instruction.SET, "foo", 1))
    l2 = LogEntry(machine.current_term, Command(Instruction.SET, "bar", 1))
    machine.log.append_entry(
        machine.log.last_index,
        machine.log.last_term,
        [l1, l2],
    )
    machine.convert_to_leader()
    machine.current_term = 2
    machine.election_timeout = 1000
    machine.heartbeat_freq = 1
    machine.next_index[1] = 2

    ae = machine.handle_tick()
    assert isinstance(ae, dict)

    assert ae[1].prev_log_term == 1
    assert ae[1].prev_log_index == 1
