# actio/messages/types.py
# -*- coding: utf-8 -*-
"""System messages for actor communication."""

from typing import Any
from dataclasses import dataclass

from ..ref import ActorRef

@dataclass(frozen=True)
class PoisonPill:
    """Sentinel message to stop an actor."""
    pass

@dataclass(frozen=True)
class DeadLetter:
    """Message that could not be delivered."""
    actor: ActorRef
    message: Any

@dataclass(frozen=True)
class Terminated:
    """Notification that an actor has stopped."""
    actor: ActorRef
