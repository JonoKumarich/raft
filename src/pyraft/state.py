import random
from enum import Enum, auto
from typing import Optional


class MachineState(Enum):
    LEADER = auto()
    CANDIDATE = auto()
    FOLLOWER = auto()


class RaftMachine:
    def __init__(
        self, server_id: int, num_servers: int, timeout: Optional[int] = None
    ) -> None:
        self.server_id = server_id
        self.num_servers = num_servers
        self.current_term = 1
        self.voted_for: Optional[int] = None
        self.clock = 0
        # TODO: Should this be reinitialized every timeout?
        self.election_timeout = random.randint(5, 10) if timeout is None else timeout
        self.state = MachineState.FOLLOWER
        self.commit_index = 0
        self.last_applied = 0
        self.votes = {id: False for id in range(num_servers)}

        # Leader Volatile state
        self.next_index: dict[int, int] = {}
        self.match_index: dict[int, int] = {}

        assert num_servers % 2 != 0, "Only supporting an odd number of servers for now"

    def __repr__(self) -> str:
        return f"Server={self.server_id} {self.state} Clock={self.clock}"

    def increment_clock(self) -> None:
        assert (
            self.clock < self.election_timeout
        ), "Error, Clock is already past the election timeout"

        print(self)

        if self.state == MachineState.LEADER:
            return

        self.clock += 1

        if self.clock >= self.election_timeout:
            self.attempt_candidacy()

    def attempt_candidacy(self) -> None:
        assert self.state != MachineState.LEADER, "Leader cannot become a candidate"

        self.votes = {id: False for id in range(self.num_servers)}
        self.current_term += 1
        self.add_vote(self.server_id)
        self.voted_for = self.server_id
        self.state = MachineState.CANDIDATE
        self.reset_clock()

    def convert_to_leader(self) -> None:
        self.state = MachineState.LEADER

        self.next_index = {
            id: self.last_applied
            for id in range(self.num_servers)
            if id != self.server_id
        }
        self.match_index = {
            id: 0 for id in range(self.num_servers) if id != self.server_id
        }

        self.reset_clock()

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

    def update_commits(self) -> None:
        if self.commit_index <= self.last_applied:
            return

        self.last_applied += 1
        # TODO: Apply log[last applied] to state machine
