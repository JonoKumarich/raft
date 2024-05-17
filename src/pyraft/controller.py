import json
import queue
import threading
import time
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Optional

from pyraft.message import AppendEntries, RequestVote
from pyraft.server import Server
from pyraft.state import RaftMachine


class ActionKind(Enum):
    APPEND_ENTRIES = auto()
    REQUEST_VOTE = auto()
    TICK = auto()


@dataclass
class Action:
    kind: ActionKind
    data: Any


class Controller:
    def __init__(self, server: Server, machine: RaftMachine) -> None:
        self.server = server
        self.machine = machine
        self.queue: queue.Queue[Action] = queue.Queue()
        self.time_dilation = 1.0

    def run(self) -> None:
        threading.Thread(target=self.clock, daemon=True).start()
        threading.Thread(target=self.handle_queue, daemon=True).start()
        threading.Thread(target=self.handle_messages, daemon=True).start()

        while True:
            continue

    def handle_messages(self):
        while True:
            address, message = self.server.inbox.get()
            print(f"received message {message.decode()}")

            try:
                deserialized = json.loads(message)
            except SyntaxError:
                print("Recieved value can't be deserliazlised")
                continue

            try:
                command = deserialized["command"]
                data = deserialized["data"]
            except KeyError:
                print("Error, server message must have command and data fields")
                continue

            match command:
                case "append_entries":
                    append_entry = AppendEntries.model_validate(data)
                    self.queue.put(Action(ActionKind.APPEND_ENTRIES, append_entry))
                case "vote_request":
                    vote_request = RequestVote.model_validate(data)
                    self.queue.put(Action(ActionKind.REQUEST_VOTE, vote_request))
                case _:
                    print(f"Command {command} not recognised")
                    continue

    def clock(self) -> None:
        while True:
            time.sleep(1 * self.time_dilation)
            self.queue.put(Action(ActionKind.TICK, None))

    def handle_queue(self) -> None:
        while True:
            action = self.queue.get()

            match action.kind:
                case ActionKind.APPEND_ENTRIES:
                    self.handle_append_entries(action.data)
                case ActionKind.REQUEST_VOTE:
                    self.handle_vote_request(action.data)
                case ActionKind.TICK:
                    self.handle_tick()

    def handle_append_entries(self, append_entry: AppendEntries):
        raise NotImplementedError

    def handle_vote_request(self, vote_request: RequestVote):
        raise NotImplementedError

    def handle_tick(self):
        print(self.machine.clock)
        start_term = self.machine.current_term
        self.machine.increment_clock()

        if self.machine.current_term > start_term:
            print("Creating vote request")

            data = RequestVote(
                term=self.machine.current_term,
                candidate_id=self.server.server_id,
                last_log_index=0,  # TODO: Need to fill this properly
                last_log_term=0,  # TODO: Need to fill this properly
            )

            self.server.send_to_all_nodes(data.model_dump_json().encode())
            print(self.server.server_mappings)


if __name__ == "__main__":
    host, port = ("127.0.0.1", 20000)
    controller = Controller(
        server=Server(host, port, {0: (host, port)}, 0), machine=RaftMachine()
    )
    controller.run()
