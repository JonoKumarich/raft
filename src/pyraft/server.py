import queue
import socket
import threading
import time

from pyraft.protocol import FixedLengthHeaderProtocol, MessageProtocol

Address = tuple[str, int]


class Server:
    def __init__(
        self,
        ip: str,
        port: int,
        server_mappings: dict[int, Address],
        server_id: int,
        protocol: MessageProtocol = FixedLengthHeaderProtocol(),
        buffer_size: int = 1024,
    ) -> None:
        self.ip = ip
        self.port = port
        self.buffer_size = buffer_size
        self.protocol = protocol
        self.inbox: queue.Queue[tuple[Address, bytes]] = queue.Queue()
        self.outbox: dict[Address, queue.Queue] = {}
        self.connections: dict[Address, socket.socket] = {}
        self.server_mappings = server_mappings
        self.server_id = server_id
        self.active = True

        # We don't want to include itself - maybe a better way to handle this
        del self.server_mappings[server_id]

    def run(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.ip, self.port))

        sock.listen()

        print(f"Server up and running on: {self.ip}:{self.port}")

        while True:
            client, address = sock.accept()
            self.connections[address] = client
            print(
                f"{self.ip}:{self.port} Connection recieved: Address={address[0]} Port={address[1]}"
            )
            threading.Thread(
                target=self.handle_inbox, args=(address,), daemon=True
            ).start()
            threading.Thread(
                target=self.handle_outbox, args=(address,), daemon=True
            ).start()

    def handle_inbox(self, address: Address) -> None:
        client = self.connections[address]
        while True:
            message = self.protocol.receive_message(client)
            self.inbox.put((address, message))

    def handle_outbox(self, address: Address) -> None:
        if address not in self.outbox.keys():
            self.outbox[address] = queue.Queue()

        while True:
            message = self.outbox[address].get()

            if not self.active:
                continue

            try:
                client = self.connections[address]
                if is_socket_closed(client):
                    raise KeyError  # TODO: Handle this a bit beter. ok for now
            except KeyError:
                # Connection should be initialized
                self.connections[address] = open_socket(address)
                client = self.connections[address]

            self.protocol.send_message(client, message)

    def send_to_all_nodes(self, message: bytes) -> None:
        for address in self.server_mappings.values():

            # We need to actually create this outbox as we lazily create connection sockets
            if address not in self.outbox.values():
                threading.Thread(
                    target=self.handle_outbox, args=(address,), daemon=True
                ).start()

            self.outbox[address].put(message)

    def send_to_single_node(self, server_id: int, message: bytes) -> None:
        address = self.server_mappings[server_id]

        if address not in self.outbox.values():
            threading.Thread(
                target=self.handle_outbox, args=(address,), daemon=True
            ).start()

        self.outbox[address].put(message)


def open_socket(address: Address) -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((address[0], address[1]))
    return sock


# https://stackoverflow.com/questions/48024720/python-how-to-check-if-socket-is-still-connected
def is_socket_closed(sock: socket.socket) -> bool:
    try:
        # this will try to read bytes without blocking and also without removing them from buffer (peek only)
        data = sock.recv(16, socket.MSG_DONTWAIT | socket.MSG_PEEK)
        if len(data) == 0:
            return True
    except BlockingIOError:
        return False  # socket is open and reading from it would block
    except ConnectionResetError:
        return True  # socket was closed for some other reason
    except Exception as e:
        print(f"{e}: unexpected exception when checking if a socket is closed")
        return False
    return False
