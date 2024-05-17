import queue
import socket
import threading

from pyraft.message import FixedLengthHeaderProtocol, MessageProtocol
from pyraft.storage import DataStore, DictStore


class Server:
    def __init__(
        self,
        ip: str,
        port: int,
        protocol: MessageProtocol = FixedLengthHeaderProtocol(),
        datastore: DataStore = DictStore(),
        buffer_size: int = 1024,
    ) -> None:
        self.ip = ip
        self.port = port
        self.buffer_size = buffer_size
        self.protocol = protocol
        self.inbox = queue.Queue()
        self.outbox: dict[str, queue.Queue] = {}
        self.data = datastore
        self.connections: dict[str, socket.socket] = {}

    def run(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.ip, self.port))

        threading.Thread(target=self.handle_messages, daemon=True).start()
        sock.listen()

        print(f"Server up and running on: {self.ip}:{self.port}")

        while True:
            client, address = sock.accept()
            self.connections[address] = client
            self.outbox[address] = queue.Queue()
            print(f"Connection recieved: Address={address[0]} Port={address[1]}")
            threading.Thread(
                target=self.handle_inbox, args=(address,), daemon=True
            ).start()
            threading.Thread(
                target=self.handle_outbox, args=(address,), daemon=True
            ).start()

    def handle_inbox(self, address: str) -> None:
        client = self.connections[address]
        while True:
            message = self.protocol.receive_message(client)
            self.inbox.put((address, message))

    def handle_outbox(self, address: str) -> None:
        client = self.connections[address]
        while True:
            message = self.outbox[address].get()
            self.protocol.send_message(client, message)

    def handle_messages(self) -> None:
        while True:
            address, message = self.inbox.get()
            message = message.decode()

            try:
                op, rest = message.split(" ", 1)
            except ValueError:
                self.outbox[address].put(b"Invalid message format")
                continue

            match op.lower():
                case "get":
                    val = self.data.get(rest)

                    if val is None:
                        self.outbox[address].put(b"Key does not exist")
                        continue

                    self.outbox[address].put(val.encode())
                case "del":
                    self.data.delete(rest)
                    self.outbox[address].put(b"ok")
                case "set":
                    try:
                        key, value = rest.split(" ", 1)
                    except ValueError:
                        self.outbox[address].put(b"Invalid message format")
                        continue
                    self.data.set(key, value)
                    self.outbox[address].put(b"ok")
                case "incr":
                    try:
                        key, value = rest.split(" ", 1)
                    except ValueError:
                        self.outbox[address].put(b"Invalid message format")
                        continue

                    self.data.incr(key, value)
                    self.outbox[address].put(b"ok")
                case _:
                    self.outbox[address].put(b"invalid command")
