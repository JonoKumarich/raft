import threading
from concurrent.futures import ThreadPoolExecutor

from pyraft.controller import Controller
from pyraft.server import SocketServer
from pyraft.state import RaftMachine
from pyraft.storage import LocalDataStore

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
            id: SocketServer(
                ip=host, port=port, server_mappings=server_nodes.copy(), server_id=id
            )
            for id, (host, port) in server_nodes.items()
        }
        self.controllers: dict[int, Controller] = {}

    def start_servers(self) -> None:
        threading.Thread(target=self.key_listener, daemon=True).start()

        with ThreadPoolExecutor() as executor:
            executor.map(self.start_sever, self.initialized_servers.values())

    def start_sever(self, server: SocketServer) -> None:
        threading.Thread(target=server.run, daemon=True).start()

        controller = Controller(
            server, RaftMachine(server.server_id, self.network_size), LocalDataStore()
        )
        self.controllers[server.server_id] = controller
        controller.run()

    def key_listener(self) -> None:
        while True:
            keys = input()

            if len(keys) != 2:
                print("Command must be of length 2")
                continue

            command, server_num = keys[0], int(keys[1])

            match command:
                case "s":
                    status = self.controllers[server_num].toggle_active_status()
                    print(f"Server {server_num} status {not status}->{status}")
                case "t":
                    self.controllers[server_num].timeout()
                    print(f"Timed out server {server_num}")
                case "m":
                    # TODO: On m+number, create a client and send a message to that server
                    print("m command not yet implemented")
                case _:
                    print(f"Command not recognised: {command}")


if __name__ == "__main__":
    network = Network(server_nodes=SERVER_NODES)
    network.start_servers()
