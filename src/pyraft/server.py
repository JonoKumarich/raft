import socket
import threading

from pyraft.message import MessageProtocol, FixedLengthHeaderProtocol
from pyraft import consts


'''
Make KV store
- handle multiple clients
- allow get / set on keys
- allow increment key
'''

class Server:
    def __init__(self, protocol: MessageProtocol, ip: str = TCP_IP, port: int = TCP_PORT, buffer_size: int = BUFFER_SIZE) -> None:
        self.ip = ip
        self.port = port
        self.buffer_size = buffer_size
        self.protocol = protocol

    def run(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((consts.TCP_IP, consts.TCP_PORT))

        sock.listen()
        while True:
            client, address = sock.accept() # SHould be in while loop
            print(f'Connection recieved: Address={address[0]} Port={address[1]}')
            threading.Thread(target=self.handle_connection, args=(client, ), daemon=True).start()

    def handle_connection(self, client: socket.socket) -> None:
        while True:
            msg = self.protocol.receive_message(client)
            print(msg)
            self.protocol.send_message(client, msg)

    # def hjandle_client
        # Parse message, find operations (get, set, del, incr), and perform operation on database
        # THis should just append onto a queue.Queue and then a separate thread called handle_messages will pop off the queue and handle it

if __name__ == '__main__':
    server = Server(FixedLengthHeaderProtocol())
    server.run()



