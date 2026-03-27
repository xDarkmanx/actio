# actio/ref/types.py
# -*- coding: utf-8 -*-
"""Actor reference and definition types."""

from typing import Dict
from typing import Any
from typing import Optional
from typing import Union

from dataclasses import dataclass
from dataclasses import field

@dataclass(repr=False, eq=False, frozen=False)
class ActorRef:
    """Immutable reference to an actor.

    Used for message routing and actor identification.
    """
    actor_id: str
    path: str
    name: str

    def __str__(self) -> str:
        return self.path

    def __hash__(self) -> int:
        return hash(self.actor_id)

    def __repr__(self) -> str:
        return f'ActorRef(path={self.path!r})'

    def __eq__(self, other: object) -> bool:
        return isinstance(other, ActorRef) and self.actor_id == other.actor_id

@dataclass(repr=False, eq=False, frozen=True)
class ActorDefinition:
    """Definition of an actor class for registration.

    Used by @actio decorator and registry for orchestration.
    """
    name: str
    cls: type
    parent: Optional[str]
    replicas: Union[int, str] = 1
    minimal: int = 1
    weight: float = 0.01
    dynamic: bool = False
    config: Dict[str, Any] = field(default_factory=dict)
