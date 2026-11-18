from enum import Enum
class Role(str,Enum):
    admin='admin'; analyst='analyst'; viewer='viewer'
class RunState(str,Enum):
    queued='queued'; running='running'; completed='completed'; failed='failed'
class ReviewState(str,Enum):
    draft='draft'; approved='approved'
class RoundState(str,Enum):
    collecting='collecting'; published='published'
class ContributionState(str,Enum):
    accepted='accepted'; superseded='superseded'
class MergeState(str,Enum):
    queued='queued'; running='running'; computed='computed'; failed='failed'
