import threading

import pytest

from pyraft.controller import Action, ActionKind, Controller
from pyraft.server import MockServer, Server
from pyraft.state import RaftMachine

NUM_SERVERS = 5


# @pytest.fixture
def server_list() -> list[MockServer]:
    server_list = [MockServer(n) for n in range(NUM_SERVERS)]

    for server in server_list:
        for other in server_list:
            if other == server:
                continue
            server.add_server(other)

    return server_list


# @pytest.fixture
def controller_list(server_list: list[MockServer]) -> list[Controller]:
    controllers = [
        Controller(
            server=server,
            machine=RaftMachine(server.server_id, num_servers=NUM_SERVERS),
        )
        for server in server_list
    ]

    for controller in controllers:
        threading.Thread(target=controller.handle_messages, daemon=True).start()
        threading.Thread(target=controller.handle_queue, daemon=True).start()

    return controllers


if __name__ == "__main__":
    controllers = controller_list(server_list())
    for controller in controllers:
        controller.queue.put(Action(ActionKind.TICK, None))

    print([controller.machine for controller in controllers])

    while True:
        pass
