import threading
from concurrent.futures import ThreadPoolExecutor

from pyraft.controller import Controller
from pyraft.server import Server
from pyraft.state import RaftMachine

SERVER_NODES = {
    0: ("127.0.0.1", 20000),
    1: ("127.0.0.1", 20001),
    # 2: ("127.0.0.1", 20002),
    # 3: ("127.0.0.1", 20003),
    # 4: ("127.0.0.1", 20004),
}


class Network:
    def __init__(self, server_nodes: dict[int, tuple[str, int]]) -> None:
        self.network_size = len(server_nodes)
        self.initialized_servers = {
            id: Server(
                ip=host, port=port, server_mappings=server_nodes.copy(), server_id=id
            )
            for id, (host, port) in server_nodes.items()
        }

    def start_servers(self) -> None:
        with ThreadPoolExecutor() as executor:
            executor.map(self.start_sever, self.initialized_servers.values())

    @staticmethod
    def start_sever(server: Server):
        threading.Thread(target=server.run, daemon=True).start()
        controller = Controller(server, RaftMachine())
        controller.run()


if __name__ == "__main__":
    network = Network(server_nodes=SERVER_NODES)
    network.start_servers()
