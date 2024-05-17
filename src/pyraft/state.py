import random
from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional


class MachineState(Enum):
    LEADER = auto()
    CANDIDATE = auto()
    FOLLOWER = auto()


class RaftMachine:
    def __init__(self, election_timeout: int = random.randint(3, 6)) -> None:
        self.current_term = 1
        self.voted_for = None
        self.clock = 0
        self.election_timeout = (
            election_timeout  # TODO: Should this be reinitialized every timeout?
        )
        self.state = MachineState.FOLLOWER
        self.commit_index = 0
        self.last_applied = 0

    def increment_clock(self) -> None:
        self.clock += 1

        if self.clock >= self.election_timeout:
            self.current_term += 1
            self.clock = 0
