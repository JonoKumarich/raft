import socket

from pyraft.protocol import FixedLengthHeaderProtocol, MessageProtocol


class Client:
    def __init__(self, ip: str, port: int, protocol: MessageProtocol) -> None:
        self.protocol = protocol
        self.ip = ip
        self.port = port

    def run(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.ip, self.port))

            while True:
                msg = input()
                self.protocol.send_message(sock, msg.encode())
                print(self.protocol.receive_message(sock).decode())


if __name__ == "__main__":
    client = Client("127.0.0.1", 20000, FixedLengthHeaderProtocol())
    client.run()
