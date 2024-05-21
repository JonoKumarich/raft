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
        self.voted_for = None
        self.clock = 0
        # TODO: Should this be reinitialized every timeout?
        self.election_timeout = random.randint(5, 10) if timeout is None else timeout
        self.state = MachineState.FOLLOWER
        self.commit_index = 0
        self.last_applied = 0
        self.num_votes_recieved = 0

    def __repr__(self) -> str:
        return f"Server={self.server_id} {self.state} Clock={self.clock}"

    def increment_clock(self) -> None:
        assert (
            self.clock < self.election_timeout
        ), "Error, Clock is already past the election timeout"

        self.clock += 1
        print(self)

        if self.clock >= self.election_timeout:
            self.attempt_candidacy()

    def attempt_candidacy(self) -> None:
        assert self.state != MachineState.LEADER, "Leader cannot become a candidate"
        # ISSUE: servers are attempting candidacy twice

        print(f"{self.server_id} ATTEMPTYIUNg candidacy")
        self.current_term += 1
        self.voted_for = self.server_id
        self.state = MachineState.CANDIDATE
        self.clock = 0

        # TODO: Instead of voting for itself, it just starts with a single vote. Need to check that this is okay
        self.num_votes_recieved = 1

    def convert_to_leader(self) -> None:
        raise NotImplementedError

    @property
    def has_majority(self) -> bool:
        return self.num_votes_recieved < self.num_servers / 2

    def add_vote(self) -> None:
        # TODO: Need to make sure we don't duplicate votes
        self.num_votes_recieved += 1

        if not self.has_majority:
            return

        self.convert_to_leader()

    def reset_clock(self) -> None:
        self.clock = 0
        # TODO: Reset the election timeout to another random initialization
