import socket

from pyraft.message import MessageProtocol, FixedLengthHeaderProtocol
from pyraft import consts

class Client:
    def __init__(self, protocol: MessageProtocol) -> None:
        self.protocol = protocol

    def run(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((consts.TCP_IP, consts.TCP_PORT))

            while True:
                msg = input()
                self.protocol.send_message(sock, msg.encode())
                print(self.protocol.receive_message(sock).decode())


if __name__ == '__main__':
    client = Client(FixedLengthHeaderProtocol())
    client.run()

