import queue
import random
import uuid
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional

from pyraft.log import LogEntry, RaftLog
from pyraft.message import (
    AppendEntries,
    AppendEntriesResponse,
    RequestVote,
    RequestVoteResponse,
)
from pyraft.storage import DataStore, LocalDataStore


class MachineState(Enum):
    LEADER = auto()
    CANDIDATE = auto()
    FOLLOWER = auto()


def create_timeout() -> int:
    return random.randint(15, 25)


class RaftMachine:
    def __init__(
        self, server_id: int, num_servers: int, datastore: DataStore = LocalDataStore()
    ) -> None:
        self.server_id = server_id
        self.num_servers = num_servers
        self.current_term = 0
        self.voted_for: Optional[int] = None
        self.clock = 0
        self.election_timeout = create_timeout()
        self.state = MachineState.FOLLOWER
        self.commit_index = 0
        self.last_applied = 0
        self.votes = {id: False for id in range(num_servers)}
        self.log = RaftLog()
        self.datastore = datastore
        self.heartbeat_freq = 5
        self.pending_entries: queue.Queue[bytes] = queue.Queue()

        # Leader Volatile state
        self.next_index: dict[int, int] = defaultdict(lambda: self.log.last_index + 1)
        self.match_index: dict[int, int] = defaultdict(lambda: 0)

        assert num_servers % 2 != 0, "Only supporting an odd number of servers for now"

    def __repr__(self) -> str:
        return f"Server={self.server_id} Clock={self.clock} Term={self.current_term} {self.state} "

    def update_persistent_storage(self) -> None:
        self.datastore.store_term(self.current_term)
        self.datastore.store_vote(self.voted_for)
        self.datastore.store_log(self.log)

    def handle_tick(self) -> Optional[RequestVote | dict[int, AppendEntriesResponse]]:
        self.increment_clock()

        if self.is_election_start:
            return RequestVote(
                term=self.current_term,
                candidate_id=self.server_id,
                last_log_index=self.log.last_index,
                last_log_term=self.log.last_term,
            )

        if not (self.is_leader and self.is_hearbeat_tick):
            return None

        logs_to_process: list[LogEntry] = []
        while not self.pending_entries.empty():
            entry = self.pending_entries.get()
            logs_to_process.append(LogEntry.from_bytes(entry, self.current_term))

        self.log.append_entry(self.log.last_index, self.log.last_term, logs_to_process)

        entries_to_send = {}
        for server_id in range(self.num_servers):
            if server_id == self.server_id:
                continue

            if self.match_index[server_id] != self.commit_index:
                self.match_index[server_id] -= 1
                assert self.match_index[server_id] >= 0
                logs_to_process.insert(0, self.log.last_item)

            entries_to_send[server_id] = AppendEntries(
                uuid=None if len(logs_to_process) == 0 else str(uuid.uuid4()),
                term=self.current_term,
                leader_id=self.server_id,
                prev_log_index=self.log.last_index,
                prev_log_term=self.log.last_term,
                entries=logs_to_process,
                leader_commit=self.commit_index,
            )

        return entries_to_send

    def _request_vote_valid(self, request_vote: RequestVote) -> bool:
        if request_vote.term != self.current_term:
            return request_vote.term > self.current_term

        voted_for_other_candidate = (
            self.voted_for is not None and self.voted_for != request_vote.candidate_id
        )
        if voted_for_other_candidate:
            return False

        candidate_log_out_of_date = (
            request_vote.last_log_term < self.log.last_term
            or request_vote.last_log_index < self.log.last_index
        )
        if candidate_log_out_of_date:
            return False

        return True

    def handle_request_vote(self, request_vote: RequestVote) -> RequestVoteResponse:
        if not self._request_vote_valid(request_vote):
            return RequestVoteResponse(
                server_id=self.server_id, term=self.current_term, vote_granted=False
            )

        self.update_term(request_vote.term)

        self.voted_for = request_vote.candidate_id
        self.reset_clock()

        self.update_persistent_storage()
        return RequestVoteResponse(
            server_id=self.server_id,
            term=self.current_term,
            vote_granted=True,
        )

    def handle_request_vote_response(
        self, request_vote_response: RequestVoteResponse
    ) -> Optional[AppendEntries]:
        # Skip remaining votes responses after obtaining a majority
        if self.is_leader:
            return

        if request_vote_response.vote_granted:
            self.add_vote(request_vote_response.server_id)

        if self.is_leader:
            return AppendEntries(
                uuid=None,
                term=self.current_term,
                leader_id=self.server_id,
                prev_log_index=self.log.last_index,
                prev_log_term=self.log.last_term,
                entries=[],
                leader_commit=self.commit_index,
            )

        return None

    def _append_entries_valid(self, append_entries: AppendEntries) -> bool:
        # 1) Reply false if term < currentTerm (§5.1)
        if append_entries.term < self.current_term:
            return False

        # 2) Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
        if append_entries.prev_log_index == 0:
            assert (
                append_entries.prev_log_term == 0
            ), "Non-zero term with zero log index"
            return True

        last_entry = self.log.get(append_entries.prev_log_index)
        if last_entry is None:
            return False

        return last_entry.term == append_entries.prev_log_term

    def handle_append_entries(
        self, append_entries: AppendEntries
    ) -> AppendEntriesResponse:
        if not self._append_entries_valid(append_entries):
            return AppendEntriesResponse(
                server_id=self.server_id,
                uuid=append_entries.uuid,
                term=self.current_term,
                success=False,
            )

        self.reset_clock()

        if self.is_candidate:
            self.demote_to_follower()

        if self.is_leader and append_entries.term > self.current_term:
            self.demote_to_follower()

        self.log.append_entry(
            prev_log_index=append_entries.prev_log_index,
            prev_log_term=append_entries.prev_log_term,
            entries=append_entries.entries,
        )

        if len(append_entries.entries) > 0:
            print(
                f"Server {self.server_id} commit_index={self.commit_index} log={self.log._items}"
            )

        self.update_commit_index(append_entries.leader_commit, self.log.last_index)

        return AppendEntriesResponse(
            server_id=self.server_id,
            uuid=append_entries.uuid,
            term=self.current_term,
            success=True,
        )

    def handle_append_entries_response(
        self, append_entries_response: AppendEntriesResponse
    ) -> None:

        if not self.is_leader:
            return

        if append_entries_response.uuid is None:
            return

        if not append_entries_response.success:
            # TODO: What do do here? I think we decrement the next_index
            return

        self.next_index[append_entries_response.server_id] += 1
        self.match_index[append_entries_response.server_id] += 1

    def increment_clock(self) -> None:
        assert (
            self.clock < self.election_timeout or self.is_leader
        ), "Error, Clock is already past the election timeout"

        self.clock += 1

        if self.state == MachineState.LEADER:
            return

        if self.clock >= self.election_timeout:
            print(f"Server {self.server_id} timed out, sending vote")
            self.attempt_candidacy()

    def attempt_candidacy(self) -> None:
        assert self.state != MachineState.LEADER, "Leader cannot become a candidate"

        self.votes = {id: False for id in range(self.num_servers)}
        self.current_term += 1
        self.add_vote(self.server_id)
        self.voted_for = self.server_id
        self.state = MachineState.CANDIDATE
        self.reset_clock()
        self.election_timeout = create_timeout()

    def convert_to_leader(self) -> None:
        self.state = MachineState.LEADER

        self.next_index = {
            id: self.last_applied
            for id in range(self.num_servers)
            if id != self.server_id
        }
        self.match_index = defaultdict(lambda: 0)
        self.reset_clock()

        print(f"Server {self.server_id} became LEADER")

    @property
    def num_votes_received(self) -> int:
        return len([k for k, v in self.votes.items() if v is True])

    @property
    def has_majority(self) -> bool:
        return self.num_votes_received > (self.num_servers / 2)

    @property
    def is_leader(self) -> bool:
        return self.state == MachineState.LEADER

    @property
    def is_follower(self) -> bool:
        return self.state == MachineState.FOLLOWER

    @property
    def is_candidate(self) -> bool:
        return self.state == MachineState.CANDIDATE

    @property
    def is_hearbeat_tick(self) -> bool:
        return self.clock % self.heartbeat_freq == 0

    @property
    def is_election_start(self) -> bool:
        return self.clock == 0 and self.is_candidate

    def add_vote(self, server_id: int) -> None:
        self.votes[server_id] = True

        if not self.has_majority:
            return

        self.convert_to_leader()

    def reset_clock(self) -> None:
        self.clock = 0

    def update_term(self, term: int) -> None:
        assert term >= self.current_term, "Can't lower the value of a term"
        if self.current_term == term:
            return

        self.current_term = term
        self.voted_for = None

        if self.is_leader:
            self.demote_to_follower()

    def demote_to_follower(self) -> None:
        assert self.state != MachineState.FOLLOWER, "Can't demote a follower"

        self.state = MachineState.FOLLOWER
        self.reset_clock()

    def update_commit_index(self, leader_commit: int, new_log_length: int) -> None:
        if self.commit_index <= leader_commit:
            return

        self.commit_index = min(leader_commit, new_log_length)
