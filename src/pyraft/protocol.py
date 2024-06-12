import socket
from typing import Protocol


class MessageProtocol(Protocol):
    def send_message(self, socket: socket.socket, message: bytes) -> None: ...

    def receive_message(self, socket: socket.socket) -> bytes: ...


class FixedLengthHeaderProtocol(MessageProtocol):
    def __init__(self, header_byte_size: int = 16) -> None:
        self.header_byte_size = header_byte_size

    def send_message(self, socket: socket.socket, message: bytes) -> None:
        if len(message) >= 2**self.header_byte_size:
            raise ValueError("Message too long to be sent")

        size = len(message)
        header = size.to_bytes(length=self.header_byte_size, byteorder="big")
        socket.sendall(header + message)

    def receive_message(self, socket: socket.socket) -> bytes:
        header = self._receive_until(self.header_byte_size, socket)
        message_size = int.from_bytes(header, "big")
        message = self._receive_until(message_size, socket)

        return message

    @staticmethod
    def _receive_until(required_bytes: int, socket: socket.socket) -> bytes:
        buffer = bytearray(required_bytes)

        received_bytes = 0
        while True:
            remaining_bytes = required_bytes - received_bytes
            received_bytes += socket.recv_into(buffer, min(1024, remaining_bytes))

            if received_bytes >= required_bytes:
                break

        return buffer
