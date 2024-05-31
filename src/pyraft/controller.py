import json
import queue
import threading
import time
import uuid
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Optional

from pyraft.message import (
    AppendEntries,
    AppendEntriesResponse,
    RequestVote,
    RequestVoteResponse,
)
from pyraft.server import Server, SocketServer
from pyraft.state import MachineState, RaftMachine


class ActionKind(Enum):
    APPEND_ENTRIES = auto()
    APPEND_ENTRIES_RESPONSE = auto()
    REQUEST_VOTE = auto()
    REQUEST_VOTE_RESPONSE = auto()
    TICK = auto()
    MESSAGE = auto()


@dataclass
class Action:
    kind: ActionKind
    data: Any


class Controller:
    def __init__(
        self,
        server: Server,
        machine: RaftMachine,
    ) -> None:
        self.server = server
        self.machine = machine
        self.queue: queue.Queue[Action] = queue.Queue()
        self.time_dilation = 0.5
        self.active = True

    def run(self) -> None:
        threading.Thread(target=self.clock, daemon=True).start()
        threading.Thread(target=self.handle_queue, daemon=True).start()
        threading.Thread(target=self.handle_messages, daemon=True).start()

        while True:
            continue

    def toggle_active_status(self) -> bool:
        # TODO: If toggling back on, reset the state machines volatile state to mock a server going down
        self.active = not self.active
        return self.active

    def timeout(self) -> None:
        match self.machine.state:
            case MachineState.FOLLOWER:
                self.machine.clock = self.machine.election_timeout - 1
                self.queue.put(Action(ActionKind.TICK, None))
            case MachineState.CANDIDATE | MachineState.LEADER:
                # TODO: implement this for leader and candidate
                print("Error: Timing out a leader or candidate not currently supported")
            case _:
                print("Unexpected error")

    @staticmethod
    def _process_message(message: bytes) -> Action:
        command, data = message.split(maxsplit=1)
        command = command.decode()

        if command == "message":
            return Action(ActionKind.MESSAGE, data)

        try:
            data = json.loads(data)
        except SyntaxError:
            print("Recieved value can't be deserliazlised")
            raise ValueError

        match command:
            case "append_entries":
                append_entry = AppendEntries.model_validate(data)
                return Action(ActionKind.APPEND_ENTRIES, append_entry)
            case "append_entries_response":
                append_entry_response = AppendEntriesResponse.model_validate(data)
                return Action(ActionKind.APPEND_ENTRIES_RESPONSE, append_entry_response)
            case "request_vote":
                request_vote = RequestVote.model_validate(data)
                return Action(ActionKind.REQUEST_VOTE, request_vote)
            case "request_vote_response":
                request_vote_response = RequestVoteResponse.model_validate(data)
                return Action(ActionKind.REQUEST_VOTE_RESPONSE, request_vote_response)
            case _:
                print(f"Command {command} not recognised")
                raise ValueError

    def handle_single_message(self) -> Action:
        message = self.server.inbox.get()

        action = self._process_message(message)

        if not self.active:
            return action

        self.queue.put(action)

        return action

    def handle_messages(self):
        while True:
            self.handle_single_message()

    def clock(self) -> None:
        while True:
            time.sleep(1 * self.time_dilation)
            self.tick()

    def tick(self) -> None:
        action = Action(ActionKind.TICK, None)
        self.queue.put(action)

    def _handle_item(self, action: Action) -> None:
        if not self.active:
            return

        match action.kind:
            case ActionKind.APPEND_ENTRIES:
                append_messages = self.machine.handle_append_entries(action.data)

                self.server.send_to_single_node(
                    action.data.leader_id,
                    b"append_entries_response "
                    + append_messages.model_dump_json().encode(),
                )
            case ActionKind.APPEND_ENTRIES_RESPONSE:
                self.machine.handle_append_entries_response(action.data)
            case ActionKind.REQUEST_VOTE:
                vote_message = self.machine.handle_request_vote(action.data)
                self.server.send_to_single_node(
                    action.data.candidate_id,
                    b"request_vote_response " + vote_message.model_dump_json().encode(),
                )
            case ActionKind.REQUEST_VOTE_RESPONSE:
                promotion_message = self.machine.handle_request_vote_response(
                    action.data
                )
                if promotion_message is None:
                    return

                self.server.send_to_all_nodes(
                    b"append_entries " + promotion_message.model_dump_json().encode()
                )
            case ActionKind.TICK:
                tick_return = self.machine.handle_tick()
                if tick_return is None:
                    return

                if isinstance(tick_return, dict):
                    for server, append_entries in tick_return.items():
                        assert isinstance(append_entries, AppendEntries)
                        message = (
                            b"append_entries "
                            + append_entries.model_dump_json().encode()
                        )
                        self.server.send_to_single_node(server, message)
                    return
                elif isinstance(tick_return, RequestVote):
                    encoded = tick_return.model_dump_json().encode()
                    self.server.send_to_all_nodes(b"request_vote " + encoded)
                    return

                raise ValueError(f"Unexpected message type: {type(tick_return)}")
            case ActionKind.MESSAGE:
                if not self.machine.is_leader:
                    # TODO: Send a response containing the leader id
                    # This should be stored on the state machine
                    # When server recieves a message, it should also be able to send a response back
                    print("Can only send messages to the leader node")
                    return

                self.machine.pending_entries.put(action.data)
            case _:
                raise ValueError("action not detected")

    def handle_queue(self) -> None:
        while True:
            action = self.queue.get()
            self._handle_item(action)


if __name__ == "__main__":
    host, port = ("127.0.0.1", 20000)
    controller = Controller(
        server=SocketServer(host, port, {0: (host, port)}, 0),
        machine=RaftMachine(1, 1),
    )
    controller.run()
