from concurrent.futures import ThreadPoolExecutor

from pyraft.message import FixedLengthHeaderProtocol
from pyraft.server import Server
from pyraft.storage import DictStore

SERVER_NODES = {
    0: ("127.0.0.1", 20000),
    1: ("127.0.0.1", 20001),
    2: ("127.0.0.1", 20002),
    3: ("127.0.0.1", 20003),
    4: ("127.0.0.1", 20004),
}


class Network:
    def __init__(self, server_nodes: dict[int, tuple[str, int]]) -> None:
        self.network_size = len(server_nodes)
        self.initialized_servers = {
            id: Server(ip=host, port=port) for id, (host, port) in server_nodes.items()
        }

    def start_all(self) -> None:
        with ThreadPoolExecutor() as executor:
            executor.map(self.start_sever, self.initialized_servers.values())

    @staticmethod
    def start_sever(server: Server):
        server.run()


if __name__ == "__main__":
    network = Network(server_nodes=SERVER_NODES)
    network.start_all()
