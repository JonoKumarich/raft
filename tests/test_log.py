import pytest

from pyraft.log import Command, Instruction, LogEntry, MessageArgSizeError, RaftLog


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
