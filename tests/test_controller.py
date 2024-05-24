import pytest

from pyraft.controller import ActionKind, Controller
from pyraft.message import RequestVote
from pyraft.server import MockServer
from pyraft.state import RaftMachine

NUM_SERVERS = 5


@pytest.fixture
def server_list() -> list[MockServer]:
    server_list = [MockServer(n) for n in range(NUM_SERVERS)]

    for server in server_list:
        for other in server_list:
            if other == server:
                continue
            server.add_server(other)

    return server_list


@pytest.fixture
def controller_list(server_list: list[MockServer]) -> list[Controller]:
    controllers = [
        Controller(
            server=server,
            machine=RaftMachine(server.server_id, num_servers=NUM_SERVERS),
            networked=False,
        )
        for server in server_list
    ]

    return controllers


def test_multiple_candidate_requests(controller_list: list[Controller]):

    c1, c2, c3, c4, c5 = controller_list

    c1.machine.election_timeout = 1
    c2.machine.election_timeout = 1
    c3.machine.election_timeout = 1

    c4.machine.election_timeout = 1000
    c5.machine.election_timeout = 1000

    # C4 votes for C1, and C5 votes for C2 resulting in a stalemate

    c1.machine.increment_clock()
    assert c1.machine.is_candidate
    res = c4.machine.handle_request_vote(
        RequestVote(
            term=c1.machine.current_term,
            candidate_id=c1.machine.server_id,
            last_log_index=0,
            last_log_term=0,
        )
    )
    c1.server.send_to_single_node

    c2.machine.increment_clock()
    assert c2.machine.is_candidate
    c5.machine.handle_request_vote(
        RequestVote(
            term=c2.machine.current_term,
            candidate_id=c2.machine.server_id,
            last_log_index=0,
            last_log_term=0,
        )
    )

    # Assert that c1 and c2 got 1 vote
    assert c1.server.inbox.qsize() == 1
    assert c1.handle_single_message().data.vote_granted
    assert c2.handle_single_message().data.vote_granted
    assert c1.server.inbox.qsize() == 0
    assert c2.server.inbox.qsize() == 0

    c3.tick()

    c1.handle_single_message()
    c2.handle_single_message()
    c4.handle_single_message()
    c5.handle_single_message()

    # Assert c3 got no votes
    assert c3.server.inbox.qsize() == 4
    for _ in range(4):
        assert not c3.handle_single_message().data.vote_granted

    c1.machine.election_timeout = 1
    c1.tick()

    c2.handle_single_message()
    c3.handle_single_message()
    c4.handle_single_message()
    c5.handle_single_message()
    assert c1.server.inbox.qsize() == 4

    for _ in range(4):
        print(c1.handle_single_message())

    assert c1.machine.is_leader


if __name__ == "__main__":
    controllers = controller_list(server_list())

    for i in range(12):
        for controller in controllers:
            controller.tick()

        print("\n".join([str(controller.machine) for controller in controllers]) + "\n")
