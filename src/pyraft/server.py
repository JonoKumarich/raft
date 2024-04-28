import socket

from pyraft.message import MessageProtocol, FixedLengthHeaderProtocol
from pyraft.consts import *


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
        sock.bind((TCP_IP, TCP_PORT))

        sock.listen(1)
        client, address = sock.accept() # SHould be in while loop
        while True:
            # Create handle client function and spawn this in a new thread for each connection
            msg = self.protocol.receive_message(client)
            print(msg)
            self.protocol.send_message(client, msg)

    # def hjandle_client
        # Parse message, find operations (get, set, del, incr), and perform operation on database
        # THis should just append onto a queue.Queue and then a separate thread called handle_messages will pop off the queue and handle it

if __name__ == '__main__':
    server = Server(FixedLengthHeaderProtocol())
    server.run()


