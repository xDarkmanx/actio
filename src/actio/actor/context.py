# actio/actor/context.py
# -*- coding: utf-8 -*-
"""Actor execution context."""
from __future__ import annotations

import asyncio

from typing import Set
from typing import List
from typing import Optional
from typing import TYPE_CHECKING

from ..ref import ActorRef
from .mailbox import Mailbox

if TYPE_CHECKING:
    from ..system.base import ActorSystem


class ActorContext:
    """Actor execution context.

    Holds all state needed for actor execution.
    Does not hold reference to actor instance (avoids circular deps).
    """

    def __init__(
        self,
        system: ActorSystem,
        actor_ref: ActorRef,
        parent: Optional[ActorRef]
    ) -> None:
        self.system: ActorSystem = system
        self.actor_ref: ActorRef = actor_ref
        self.parent: Optional[ActorRef] = parent
        self.mailbox: Mailbox = Mailbox()
        self.lifecycle: Optional[asyncio.Task] = None
        self.watching: Set[ActorRef] = set()
        self.watched_by: Set[ActorRef] = set()
        self.children: List[ActorRef] = []
        self.is_stopped: asyncio.Event = asyncio.Event()
        self.receiving_messages: bool = False
        self.actor_instance: Optional[object] = None

    @property
    def is_running(
        self
    ) -> bool:
        """Check if actor is running."""
        return self.receiving_messages and not self.is_stopped.is_set()
