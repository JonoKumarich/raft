import random
import string
import threading
from concurrent.futures import ThreadPoolExecutor

from pyraft.controller import Controller
from pyraft.server import SocketServer
from pyraft.state import RaftMachine

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
        self.sent_keys = set()

    def start_servers(self) -> None:
        threading.Thread(target=self.key_listener, daemon=True).start()

        with ThreadPoolExecutor() as executor:
            executor.map(self.start_sever, self.initialized_servers.values())

    def start_sever(self, server: SocketServer) -> None:
        threading.Thread(target=server.run, daemon=True).start()

        controller = Controller(
            server,
            RaftMachine(server.server_id, self.network_size),
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
                    message = self._random_command()
                    self.controllers[server_num].server.inbox.put(message)
                    print(f"Sent message to {message.decode()} server {server_num}")
                case _:
                    print(f"Command not recognised: {command}")

    def _random_command(self) -> bytes:
        command = random.choice(["get", "set", "del", "add"])

        if command == "set":
            key = "".join(random.choice(string.ascii_lowercase) for _ in range(8))
            self.sent_keys.add(key)
        else:
            if len(self.sent_keys) == 0:
                return self._random_command()
            key = random.choice(list(self.sent_keys))

        if command == "del":
            self.sent_keys.remove(key)

        value = (
            f" {random.randint(100000, 999999)}" if command in ("set", "add") else ""
        )

        return f"message {command} {key}{value}".encode()


if __name__ == "__main__":
    network = Network(server_nodes=SERVER_NODES)
    network.start_servers()
