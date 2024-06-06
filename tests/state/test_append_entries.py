import pytest

from pyraft.log import Command, Instruction, LogEntry, RaftLog
from pyraft.message import (
    AppendEntries,
    AppendEntriesResponse,
    RequestVote,
    RequestVoteResponse,
)
from pyraft.state import MachineState, RaftMachine


def test_append_entries_resets_timer():
    machine = RaftMachine(0, 3)
    machine.election_timeout = 3
    ae = AppendEntries(
        uuid=None,
        term=1,
        leader_id=1,
        prev_log_index=0,
        prev_log_term=0,
        entries=[],
        leader_commit=0,
    )

    machine.handle_tick()
    machine.handle_tick()
    assert machine.is_follower
    machine.handle_append_entries(ae)
    assert machine.clock == 0
    machine.handle_tick()
    machine.handle_tick()
    assert machine.is_follower


def test_append_entries_invalid_returned(mocker):
    validity = mocker.patch("pyraft.state.RaftMachine._append_entries_valid")
    validity.return_value = False
    ae = AppendEntries(
        uuid=None,
        term=1,
        leader_id=1,
        prev_log_index=0,
        prev_log_term=0,
        entries=[],
        leader_commit=0,
    )
    machine = RaftMachine(0, 3)
    res = machine.handle_append_entries(ae)

    assert res == AppendEntriesResponse(
        server_id=machine.server_id,
        uuid=ae.uuid,
        term=machine.current_term,
        success=False,
    )


def test_append_entries_resets_candidate():
    ae = AppendEntries(
        uuid=None,
        term=1,
        leader_id=1,
        prev_log_index=0,
        prev_log_term=0,
        entries=[],
        leader_commit=0,
    )
    machine = RaftMachine(0, 3)
    machine.attempt_candidacy()
    assert machine.is_candidate
    res = machine.handle_append_entries(ae)
    assert res.success
    assert machine.is_follower


def test_append_entries_higher_term_resets_leader():
    ae = AppendEntries(
        uuid=None,
        term=3,
        leader_id=1,
        prev_log_index=0,
        prev_log_term=0,
        entries=[],
        leader_commit=0,
    )
    machine = RaftMachine(0, 3)
    machine.convert_to_leader()
    assert machine.is_leader
    machine.handle_append_entries(ae)
    assert machine.is_follower
    assert machine.current_term == ae.term


# TODO: test everything in function from leader reset onwards


def test_validity_stale_term():
    ae = AppendEntries(
        uuid=None,
        term=1,
        leader_id=1,
        prev_log_index=0,
        prev_log_term=0,
        entries=[],
        leader_commit=0,
    )

    machine = RaftMachine(0, 3)
    machine.convert_to_leader()
    machine.update_term(2)
    assert not machine._append_entries_valid(ae)


def test_validity_no_prev():
    ae = AppendEntries(
        uuid=None,
        term=2,
        leader_id=1,
        prev_log_index=0,
        prev_log_term=0,
        entries=[],
        leader_commit=0,
    )

    machine = RaftMachine(0, 3)
    machine.convert_to_leader()
    machine.update_term(2)
    assert machine._append_entries_valid(ae)


def test_validity_heartbeat_no_entry_with_outdated_log_fails():
    machine = RaftMachine(0, 3)
    machine.update_term(2)

    machine.log.append_entry(0, 0, [LogEntry(1, Command(Instruction.SET, "foo", 1))])

    ae = AppendEntries(
        uuid=None,
        term=2,
        leader_id=1,
        prev_log_index=2,
        prev_log_term=2,
        entries=[],
        leader_commit=0,
    )

    assert not machine._append_entries_valid(ae)


def test_validity_mismatched_last_terms():
    ae = AppendEntries(
        uuid="abc",
        term=2,
        leader_id=1,
        prev_log_index=1,
        prev_log_term=2,
        entries=[],
        leader_commit=0,
    )

    machine = RaftMachine(0, 3)
    machine.update_term(2)
    machine.log.append_entry(0, 0, [LogEntry(1, Command(Instruction.SET, "foo", 1))])

    assert not machine._append_entries_valid(ae)


def test_validity_missing_last_index():
    ae = AppendEntries(
        uuid="abc",
        term=2,
        leader_id=1,
        prev_log_index=2,
        prev_log_term=2,
        entries=[],
        leader_commit=0,
    )

    machine = RaftMachine(0, 3)
    machine.update_term(2)
    machine.log.append_entry(
        0, 0, [LogEntry(ae.prev_log_term, Command(Instruction.SET, "foo", 1))]
    )

    assert not machine._append_entries_valid(ae)


def test_validity_matching_last_value():
    ae = AppendEntries(
        uuid="abc",
        term=2,
        leader_id=1,
        prev_log_index=1,
        prev_log_term=2,
        entries=[],
        leader_commit=0,
    )

    machine = RaftMachine(0, 3)
    machine.update_term(2)
    machine.log.append_entry(
        0, 0, [LogEntry(ae.prev_log_term, Command(Instruction.SET, "foo", 1))]
    )

    assert machine._append_entries_valid(ae)


def test_validity_overwrite():
    machine = RaftMachine(0, 3)
    machine.update_term(4)
    machine.log.append_entry(
        0,
        0,
        [
            LogEntry(1, Command(Instruction.SET, "foo", 1)),
            LogEntry(2, Command(Instruction.SET, "bar", 1)),
            LogEntry(4, Command(Instruction.SET, "baz", 1)),
        ],
    )

    ae = AppendEntries(
        uuid="abc",
        term=5,
        leader_id=1,
        prev_log_index=1,
        prev_log_term=1,
        entries=[LogEntry(3, Command(Instruction.SET, "new", 1))],
        leader_commit=1,
    )

    assert machine.handle_append_entries(ae).success
    assert [item.term for item in machine.log.items] == [1, 3]


# Server=1 Clock=0 Term=4 MachineState.FOLLOWER  [Term 1: set foo 1 (self.id='10c448e6-228e-11ef-b785-7e7d7d2ee38e'), Term 2: set bar 2 (self.id='10c44d14-228e-11ef-b785-7e7d7d2ee38e')] uuid='3f5742c9-f508-44f9-8389-971e33d3f796' term=4 leader_id=0 prev_log_index=2 prev_log_term=2 entries=[Term 4: set foo 4 (self.id='10c45340-228e-11ef-b785-7e7d7d2ee38e')] leader_commit=1
