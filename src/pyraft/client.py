import socket

from pyraft.message import MessageProtocol, FixedLengthHeaderProtocol
from pyraft.consts import *

class Client:
    def __init__(self, protocol: MessageProtocol) -> None:
        self.protocol = protocol

    def run(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((TCP_IP, TCP_PORT))
            self.protocol.send_message(sock, b"foo bar")
            self.protocol.send_message(sock, b"another message")


if __name__ == '__main__':
    client = Client(FixedLengthHeaderProtocol())
    client.run()

