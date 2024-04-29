import socket
import threading
import queue
from typing import Type

from pyraft.message import MessageProtocol, FixedLengthHeaderProtocol
from pyraft.storage import DataStore, DictStore
from pyraft import consts


class Server:
    def __init__(
        self, 
        protocol: MessageProtocol, 
        datastore: Type[DataStore],
        ip: str = consts.TCP_IP, 
        port: int = consts.TCP_PORT, 
        buffer_size: int = consts.BUFFER_SIZE,
    ) -> None:
        self.ip = ip
        self.port = port
        self.buffer_size = buffer_size
        self.protocol = protocol
        self.queue = queue.Queue()
        self.data = datastore.init()

    def run(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((consts.TCP_IP, consts.TCP_PORT))

        threading.Thread(target=self.handle_messages, daemon=True).start()

        sock.listen()
        while True:
            client, address = sock.accept() # SHould be in while loop
            print(f'Connection recieved: Address={address[0]} Port={address[1]}')
            threading.Thread(target=self.handle_connection, args=(client, ), daemon=True).start()

    def handle_connection(self, client: socket.socket) -> None:
        return_queue = queue.Queue()
        while True:
            message = self.protocol.receive_message(client)
            self.queue.put((return_queue, message))
            response = return_queue.get()
            self.protocol.send_message(client, response)

    def handle_messages(self) -> None:
        while True:
            (return_queue, message) = self.queue.get()
            message = message.decode()

            try:
                op, rest = message.split(' ', 1)
            except ValueError:
                return_queue.put(b'Invalid message format')
                continue


            match op.lower():
                case 'get':
                    val = self.data.get(rest)

                    if val is None:
                        return_queue.put(b'Key does not exist')
                        continue

                    return_queue.put(val.encode())
                case 'del':
                    self.data.delete(rest)
                    return_queue.put(b'ok')
                case 'set':
                    try:
                        key, value = rest.split(' ', 1)
                    except ValueError:
                        return_queue.put(b'Invalid message format')
                        continue
                    self.data.set(key, value)
                    return_queue.put(b'ok')
                case 'incr':
                    try:
                        key, value = rest.split(' ', 1)
                    except ValueError:
                        return_queue.put(b'Invalid message format')
                        continue
                    
                    self.data.incr(key, value)
                    return_queue.put(b'ok')
                case _:
                    return_queue.put(b'invalid command')

 
if __name__ == '__main__':
    server = Server(
        FixedLengthHeaderProtocol(),
        DictStore
    )

    server.run()


