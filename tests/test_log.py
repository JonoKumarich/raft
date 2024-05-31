import pytest

from pyraft.log import (
    AppendEntriesFailedError,
    Command,
    Instruction,
    LogEntry,
    MessageArgSizeError,
    RaftLog,
)


def test_log_entry_from_bytes_set_message():
    message = b"set foo 1"
    entry = LogEntry.from_bytes(message, 1)
    assert entry == LogEntry(1, Command(Instruction.SET, "foo", 1))


def test_log_entry_from_bytes_set_message_two_args_fails():
    message = b"set foo"
    with pytest.raises(MessageArgSizeError):
        LogEntry.from_bytes(message, 1)


def test_one_indexed_log_gets_correctly():
    log = RaftLog()
    log.append(LogEntry(1, Command(Instruction.SET, "foo", 1)))
    log.append(LogEntry(1, Command(Instruction.SET, "foo", 2)))
    log.append(LogEntry(1, Command(Instruction.SET, "foo", 3)))

    assert (a := log.get(1)) is not None
    assert (b := log.get(3)) is not None

    assert a.command.value == 1
    assert b.command.value == 3


def test_log_indexed_out_of_bounds_fails():
    log = RaftLog()

    with pytest.raises(IndexError):
        log.get(0)

    command = LogEntry(1, Command(Instruction.SET, "foo", 1))
    log.append(command)
    with pytest.raises(IndexError):
        log.get(0)

    with pytest.raises(IndexError):
        log.get(2)

    assert log.get(1) == command


def test_log_last_term_empty_log():
    log = RaftLog()
    assert log.last_term == 0


def test_log_last_term():
    log = RaftLog()
    log.append(LogEntry(1, Command(Instruction.SET, "foo", 1)))
    log.append(LogEntry(2, Command(Instruction.SET, "foo", 2)))

    assert log.last_term == 2


def test_log_last_index_empty_log():
    log = RaftLog()
    assert log.last_index == 0


def test_log_last_index():
    log = RaftLog()
    log.append(LogEntry(1, Command(Instruction.SET, "foo", 1)))
    log.append(LogEntry(2, Command(Instruction.SET, "foo", 2)))

    assert log.last_index == 2


def test_log_last_item_empty_log():
    log = RaftLog()

    with pytest.raises(IndexError):
        log.last_item


def test_log_last_item():
    log = RaftLog()
    log.append(LogEntry(1, Command(Instruction.SET, "foo", 1)))
    command = LogEntry(2, Command(Instruction.SET, "foo", 2))
    log.append(command)

    assert log.last_item == command


def test_delete_entries_from():
    log = RaftLog()
    log.append(LogEntry(1, Command(Instruction.SET, "foo", 1)))
    log.append(LogEntry(1, Command(Instruction.SET, "foo", 2)))
    log.append(LogEntry(1, Command(Instruction.SET, "foo", 3)))

    assert log.last_index == 3
    log.delete_existing_from(2)
    assert log.last_index == 1


def test_append_entry_with_lower_term_fails():
    log = RaftLog()
    log.append(LogEntry(2, Command(Instruction.SET, "foo", 1)))

    with pytest.raises(AppendEntriesFailedError):
        log.append_entry(1, 2, [LogEntry(1, Command(Instruction.SET, "foo", 1))])


def test_append_entry_empty_log():
    log = RaftLog()
    c1 = LogEntry(1, Command(Instruction.SET, "foo", 1))
    c2 = LogEntry(2, Command(Instruction.SET, "foo", 2))

    log.append_entry(0, 0, [c1, c2])
    assert log.last_index == 2
    assert log.last_term == 2
    assert log.last_item == c2


def test_append_entry_with_mismatched_log_terms_fail():
    log = RaftLog()
    log.append(LogEntry(2, Command(Instruction.SET, "foo", 1)))

    with pytest.raises(AppendEntriesFailedError):
        log.append_entry(1, 3, [LogEntry(2, Command(Instruction.SET, "foo", 1))])


def test_append_entry_with_existing_correct_entries_maintains_order():
    log = RaftLog()
    c1 = LogEntry(1, Command(Instruction.SET, "foo", 1))
    c2 = LogEntry(1, Command(Instruction.SET, "foo", 2))
    c3 = LogEntry(1, Command(Instruction.SET, "foo", 3))
    c4 = LogEntry(2, Command(Instruction.SET, "foo", 4))
    log.append(c1)
    log.append(c2)
    log.append(c3)

    log.append_entry(1, 1, [c2, c3, c4])
    assert log.last_index == 4
    assert log.last_term == 2


def test_append_entry_with_conflicting_entries_overwrites_log():
    log = RaftLog()
    c1 = LogEntry(1, Command(Instruction.SET, "foo", 1))
    c2a = LogEntry(1, Command(Instruction.SET, "foo", 2))
    c2b = LogEntry(2, Command(Instruction.SET, "foo", 3))
    c3 = LogEntry(2, Command(Instruction.SET, "foo", 4))

    log.append_entry(0, 0, [c1, c2a])
    log.append_entry(1, 1, [c2b, c3])

    print(log._items)
    assert log.last_index == 3
    assert log.get(2) == c2b
