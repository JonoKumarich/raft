import json
import queue
import threading
import time
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Optional

from pyraft.log import Log
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


@dataclass
class Action:
    kind: ActionKind
    data: Any


class Controller:
    def __init__(
        self, server: Server, machine: RaftMachine, networked: bool = True
    ) -> None:
        self.server = server
        self.machine = machine
        self.log = Log()  # TODO: Should this be in the controller or the state machine?
        self.queue: queue.Queue[Action] = queue.Queue()
        self.time_dilation = 0.5
        self.heartbeat_frequency = 5
        self.active = True

        # This handles the actions via a queue instead of instantly to avoid race conditions
        # Can be turned off to help with testing
        self.networked = networked

    def run(self) -> None:
        threading.Thread(target=self.clock, daemon=True).start()
        threading.Thread(target=self.handle_queue, daemon=True).start()
        threading.Thread(target=self.handle_messages, daemon=True).start()

        while True:
            continue

    def toggle_active_status(self) -> bool:
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

        if self.networked:
            self.queue.put(action)
        else:
            self._handle_item(action)

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
        if self.networked:
            self.queue.put(action)
        else:
            self._handle_item(action)

    def _handle_item(self, action: Action) -> None:
        if not self.active:
            return

        match action.kind:
            case ActionKind.APPEND_ENTRIES:
                self.handle_append_entries(action.data)
            case ActionKind.APPEND_ENTRIES_RESPONSE:
                self.handle_append_entries_response(action.data)
            case ActionKind.REQUEST_VOTE:
                self.handle_request_vote(action.data)
            case ActionKind.REQUEST_VOTE_RESPONSE:
                self.handle_request_vote_reponse(action.data)
            case ActionKind.TICK:
                self.handle_tick()
            case _:
                raise ValueError("action not detected")

    def handle_queue(self) -> None:
        while True:
            action = self.queue.get()
            self._handle_item(action)

    def _append_entry_success(self, append_entry: AppendEntries) -> bool:
        if append_entry.term < self.machine.current_term:
            return False

        no_logs = append_entry.prev_log_index == 0 and self.log.last_index == 0
        if no_logs:
            return True

        last_log = self.log.get(append_entry.prev_log_index)
        if last_log is None:
            return False

        if last_log.term != append_entry.term:
            return False

        return True

    def handle_append_entries(self, append_entry: AppendEntries) -> None:
        # TODO: Steps 3-5 of reciever implementation

        if self.machine.is_candidate and self.machine.current_term <= append_entry.term:
            self.machine.demote_to_follower()

        if self.machine.is_leader and self.machine.current_term < append_entry.term:
            self.machine.demote_to_follower()

        success = self._append_entry_success(append_entry)

        if success:
            self.machine.update_term(append_entry.term)

        response = AppendEntriesResponse(
            term=self.machine.current_term,
            success=success,
        )

        message = b"append_entries_response " + response.model_dump_json().encode()
        self.server.send_to_single_node(append_entry.leader_id, message)

        if not success:
            return

        self.machine.reset_clock()
        self.machine.update_commits()

    def handle_append_entries_response(
        self, append_entry_response: AppendEntriesResponse
    ) -> None:
        # TODO: Empty implementation for now
        pass

    def _request_vote_success(self, request_vote: RequestVote) -> bool:
        term_is_greater = request_vote.last_log_term > self.log.last_term
        index_is_greater = (
            request_vote.last_log_term == self.log.last_term
            and request_vote.last_log_index >= self.log.last_index
        )
        already_voted_for = self.machine.voted_for == request_vote.candidate_id
        no_votes = self.machine.voted_for is None

        if request_vote.term < self.machine.current_term:
            return False

        if (no_votes or already_voted_for) and (term_is_greater or index_is_greater):
            return True

        return False

    def handle_request_vote(self, request_vote: RequestVote) -> None:

        if request_vote.term > self.machine.current_term:
            self.machine.update_term(request_vote.term)

        success = self._request_vote_success(request_vote)

        if success:
            self.machine.voted_for = request_vote.candidate_id
            self.machine.reset_clock()
            # NOTE: said that the clock doesn't need to reset here in excalidraw, but it looks like it does in the simulation

        response = RequestVoteResponse(
            server_id=self.server.server_id,
            term=self.machine.current_term,
            vote_granted=success,
        )

        message = b"request_vote_response " + response.model_dump_json().encode()
        self.server.send_to_single_node(request_vote.candidate_id, message)

    def handle_request_vote_reponse(
        self, request_vote_response: RequestVoteResponse
    ) -> None:

        # Skip remaining votes responses after obtaining a majority
        if self.machine.is_leader:
            return

        if request_vote_response.vote_granted:
            self.machine.add_vote(request_vote_response.server_id)

        if self.machine.is_leader:
            self.send_heartbeat()

    def handle_tick(self) -> None:
        self.machine.increment_clock()

        is_election_start = self.machine.clock == 0 and self.machine.is_candidate
        if is_election_start:
            print("starting election")

            data = RequestVote(
                term=self.machine.current_term,
                candidate_id=self.server.server_id,
                last_log_index=self.log.last_index,
                last_log_term=self.log.last_term,
            )

            self.server.send_to_all_nodes(
                b"request_vote " + data.model_dump_json().encode()
            )
            return

        is_heartbeat = self.machine.clock % self.heartbeat_frequency == 0
        if is_heartbeat and self.machine.is_leader:
            self.send_heartbeat()

        # TODO:
        # If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
        #     • If successful: update nextIndex and matchIndex for follower (§5.3)
        #     • If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)

    def send_heartbeat(self) -> None:
        data = AppendEntries(
            term=self.machine.current_term,
            leader_id=self.machine.server_id,
            prev_log_term=self.log.last_term,
            prev_log_index=self.log.last_index,
            entries=[],
            leader_commit=self.log.latest_commit,
        )

        self.server.send_to_all_nodes(
            b"append_entries " + data.model_dump_json().encode()
        )


if __name__ == "__main__":
    host, port = ("127.0.0.1", 20000)
    controller = Controller(
        server=SocketServer(host, port, {0: (host, port)}, 0), machine=RaftMachine(1, 1)
    )
    controller.run()
