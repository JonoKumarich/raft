import json
import queue
import threading
import time
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any

from pyraft.log import Log
from pyraft.message import AppendEntries, RequestVote, RequestVoteResponse
from pyraft.server import Server
from pyraft.state import MachineState, RaftMachine


class ActionKind(Enum):
    APPEND_ENTRIES = auto()
    REQUEST_VOTE = auto()
    REQUEST_VOTE_RESPONSE = auto()
    TICK = auto()


@dataclass
class Action:
    kind: ActionKind
    data: Any


class Controller:
    def __init__(self, server: Server, machine: RaftMachine) -> None:
        self.server = server
        self.machine = machine
        self.log = Log()  # TODO: Should this be in the controller or the state machine?
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

            command, data = message.split(maxsplit=1)
            command = command.decode()

            try:
                data = json.loads(data)
            except SyntaxError:
                print("Recieved value can't be deserliazlised")
                continue

            match command:
                case "append_entries":
                    append_entry = AppendEntries.model_validate(data)
                    self.queue.put(Action(ActionKind.APPEND_ENTRIES, append_entry))
                case "request_vote":
                    request_vote = RequestVote.model_validate(data)
                    self.queue.put(Action(ActionKind.REQUEST_VOTE, request_vote))
                case "request_vote_response":
                    request_vote_response = RequestVoteResponse.model_validate(data)
                    self.queue.put(
                        Action(ActionKind.REQUEST_VOTE_RESPONSE, request_vote_response)
                    )
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
                    self.handle_request_vote(action.data)
                case ActionKind.REQUEST_VOTE_RESPONSE:
                    self.handle_request_vote_reponse(action.data)
                case ActionKind.TICK:
                    self.handle_tick()
                case _:
                    raise ValueError("action not detected")

    def handle_append_entries(self, append_entry: AppendEntries) -> None:
        raise NotImplementedError

    def handle_request_vote(self, request_vote: RequestVote) -> None:

        term_is_greater = request_vote.last_log_term > self.log.last_term
        index_is_greater = (
            request_vote.last_log_term == self.log.last_term
            and request_vote.last_log_index >= self.log.last_index
        )
        already_voted = self.machine.voted_for == request_vote.candidate_id
        not_voted = self.machine.voted_for is None

        if request_vote.term < self.machine.current_term:
            response = RequestVoteResponse(
                term=self.machine.current_term, vote_granted=False
            )
        elif (not_voted or already_voted) and (term_is_greater or index_is_greater):
            response = RequestVoteResponse(
                term=self.machine.current_term, vote_granted=True
            )
        else:
            response = RequestVoteResponse(
                term=self.machine.current_term, vote_granted=False
            )

        message = b"request_vote_response " + response.model_dump_json().encode()
        self.server.send_to_single_node(request_vote.candidate_id, message)

        self.machine.reset_clock()

    def handle_request_vote_reponse(
        self, request_vote_response: RequestVoteResponse
    ) -> None:

        if request_vote_response.vote_granted:
            self.machine.add_vote()

    def handle_tick(self) -> None:
        self.machine.increment_clock()

        is_election_start = (
            self.machine.clock == 0 and self.machine.state == MachineState.CANDIDATE
        )
        if is_election_start:
            print("Creating vote request")

            data = RequestVote(
                term=self.machine.current_term,
                candidate_id=self.server.server_id,
                last_log_index=self.log.last_index,
                last_log_term=self.log.last_term,
            )

            self.server.send_to_all_nodes(
                b"request_vote " + data.model_dump_json().encode()
            )


if __name__ == "__main__":
    host, port = ("127.0.0.1", 20000)
    controller = Controller(
        server=Server(host, port, {0: (host, port)}, 0), machine=RaftMachine(1, 1)
    )
    controller.run()
