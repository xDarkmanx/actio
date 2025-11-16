# actio/ref.py
# -*- coding: utf-8 -*-

from typing import Dict
from typing import Any
from typing import Optional
from typing import Union

from dataclasses import dataclass
from dataclasses import field


@dataclass(repr=False, eq=False, frozen=True)
class ActorRef:
    actor_id: str
    path: str
    name: str

    def __str__(self):
        return self.path

    def __hash__(self):
        return hash(self.actor_id)

    def __repr__(self):
        return self.path

    def __eq__(self, other):
        return isinstance(other, ActorRef) and self.actor_id == other.actor_id


@dataclass
class ActorDefinition:
    name: str
    cls: type
    parent: Optional[str]
    replicas: Union[int, str] = 1
    minimal: int = 1
    dynamic: bool = False
    config: Dict[str, Any] = field(default_factory=dict)
