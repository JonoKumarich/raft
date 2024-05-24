from pyraft.log import RaftLog


def test_delete_existing_from():
    log = RaftLog()
    num_entries = 5

    for _ in range(num_entries):
        log.append_entry(None, 1)

    assert len(log._items) == num_entries
    to_delete = 3
    log.delete_existing_from(to_delete)
    assert len(log._items) == num_entries - to_delete


def test_last_index_amount():
    log = RaftLog()
    num_entries = 5

    for _ in range(num_entries):
        log.append_entry(None, 1)

    assert log.last_index == num_entries


def test_last_log_term_fetch():
    log = RaftLog()

    log.append_entry(None, 1)
    log.append_entry(None, 4)
    log.append_entry(None, 5)

    assert log.last_term == 5
