# actio/system/base.py
# -*- coding: utf-8 -*-
"""ActorSystem - orchestrator for actor lifecycle."""
from __future__ import annotations

import asyncio
import logging

from typing import Dict
from typing import List
from typing import Set
from typing import Any
from typing import Optional
from typing import Union
from typing import TYPE_CHECKING

from uuid import uuid4

from ..ref import ActorRef
from ..messages import PoisonPill
from ..messages import DeadLetter
from ..protocols import RegistryProtocol
from ..registry import LocalRegistry

from .lifecycle import actor_lifecycle_loop

if TYPE_CHECKING:
    from ..actor.base import Actor

log = logging.getLogger("actio.system")


class ActorSystem:
    """Actor system orchestrator.

    Manages actor lifecycle, message delivery, and supervision.
    Requires explicit registry injection.
    """

    def __init__(self, registry: Optional[RegistryProtocol] = None) -> None:
        """Initialize ActorSystem.

        Args:
            registry: Registry implementation. Defaults to LocalRegistry() for standalone.
        """
        self.registry: RegistryProtocol = registry or LocalRegistry()
        self._actors: Dict[ActorRef, "_ActorContext"] = {}
        self._is_stopped: asyncio.Event = asyncio.Event()
        self._children: List[ActorRef] = []
        self._actor_names: Dict[str, ActorRef] = {}

    def create(
        self,
        actor: Actor,
        name: Optional[str] = None
    ) -> ActorRef:
        """Create a root actor.

        Args:
            actor: Actor instance to create
            name: Optional name for the actor

        Returns:
            ActorRef to the created actor
        """
        return self._create(actor=actor, parent=None, name=name)

    def tell(
        self,
        actor: Union[Actor, ActorRef],
        message: Any
    ) -> None:
        """Send a message to an actor.

        Args:
            actor: Target actor or ActorRef
            message: Message to send
        """
        self._tell(actor=actor, message=message, sender=None)

    def stop(
        self,
        actor: Union[Actor, ActorRef]
    ) -> None:
        """Stop an actor.

        Args:
            actor: Actor or ActorRef to stop
        """
        self._tell(actor=actor, message=PoisonPill(), sender=None)

    async def shutdown(
        self,
        timeout: Optional[Union[int, float]] = None
    ) -> None:
        """Shutdown the entire actor system.

        Args:
            timeout: Optional timeout for graceful shutdown
        """
        await self._shutdown(timeout=timeout)

    def stopped(self) -> asyncio.Future:
        """Wait for system shutdown.

        Returns:
            Future that completes when system is stopped
        """
        return self._is_stopped.wait()

    # --- Internal methods ---

    def _create(
        self,
        actor: Actor,
        *,
        parent: Union[None, Actor, ActorRef],
        name: Optional[str] = None
    ) -> ActorRef:
        """Internal method to create an actor.

        Args:
            actor: Actor instance
            parent: Parent actor or None for root
            name: Optional name

        Returns:
            ActorRef to the created actor
        """
        from ..actor.base import Actor as ActorClass

        if not isinstance(actor, ActorClass):
            raise ValueError(f"Not an actor: {actor}")

        parent_ctx: Optional["_ActorContext"] = None
        if parent:
            parent = self._validate_actor_ref(parent)
            parent_ctx = self._actors[parent]
            child_idx = len(parent_ctx.children) + 1
        else:
            child_idx = len(self._children) + 1

        if not name:
            name = f"{type(actor).__name__}-{child_idx}"

        if parent:
            path = f"{parent.path}/{name}"
        else:
            path = name

        actor_id = str(uuid4().hex)
        actor_ref = ActorRef(actor_id=actor_id, path=path, name=name)

        # Create context and store actor instance
        actor_ctx = _ActorContext(self, actor_ref, parent)
        actor_ctx.actor_instance = actor

        actor_ctx.lifecycle = asyncio.get_event_loop().create_task(
            actor_lifecycle_loop(self, actor, actor_ref, actor_ctx)
        )

        actor._context = actor_ctx
        self._actors[actor_ref] = actor_ctx

        if name in self._actor_names:
            log.warning(f"ActorSystem: Name '{name}' already exists, overwriting: {self._actor_names[name]} with {actor_ref}")
        self._actor_names[name] = actor_ref

        if parent and parent_ctx:
            parent_ctx.children.append(actor_ref)
        else:
            self._children.append(actor_ref)

        # Register dynamic instance if needed
        if hasattr(actor, "_definition") and getattr(actor._definition, "dynamic", False):
            try:
                from ..registry import get_default_registry
                registry = get_default_registry()
                asyncio.create_task(
                    registry.register_instance(actor._definition.name, actor_ref)
                )
            except ImportError as e:
                log.error(f"Failed to register dynamic instance: {e}")

        return actor_ref

    def _tell(
        self,
        actor: Union[Actor, ActorRef],
        message: Any,
        *,
        sender: Union[None, Actor, ActorRef]
    ) -> None:
        """Internal method to send a message."""
        actor = self._validate_actor_ref(actor)

        if sender:
            sender = self._validate_actor_ref(sender)
            if sender not in self._actors:
                raise ValueError(f"Sender does not exist: {sender}")

        if actor in self._actors:
            actor_ctx = self._actors[actor]
            # ✅ ИСПРАВЛЕНО: передаём sender и message раздельно
            actor_ctx.mailbox.put_nowait(sender, message)
        elif sender:
            deadletter = DeadLetter(actor=actor, message=message)
            self._tell(sender, deadletter, sender=None)
        else:
            log.warning(f"Failed to deliver message {message} to {actor}")

    def _watch(
        self,
        actor: Union[Actor, ActorRef],
        other: Union[Actor, ActorRef]
    ) -> None:
        """Internal method to watch an actor.

        Args:
            actor: Watcher actor
            other: Actor to watch
        """
        actor = self._validate_actor_ref(actor)
        if actor not in self._actors:
            raise ValueError(f"Actor does not exist: {actor}")

        other = self._validate_actor_ref(other)
        if other not in self._actors:
            raise ValueError(f"Actor does not exist: {other}")

        if actor == other:
            raise ValueError(f"Actor cannot watch themselves: {actor}")

        actor_ctx = self._actors[actor]
        other_ctx = self._actors[other]
        actor_ctx.watching.add(other)
        other_ctx.watched_by.add(actor)

    def _unwatch(
        self,
        actor: Union[Actor, ActorRef],
        other: Union[Actor, ActorRef]
    ) -> None:
        """Internal method to unwatch an actor.

        Args:
            actor: Watcher actor
            other: Actor to unwatch
        """
        actor = self._validate_actor_ref(actor)
        if actor not in self._actors:
            raise ValueError(f"Actor does not exist: {actor}")

        if actor == other:
            raise ValueError(f"Actor cannot unwatch themselves: {actor}")

        actor_ctx = self._actors[actor]
        if other in actor_ctx.watching:
            actor_ctx.watching.remove(other)

        if other in self._actors:
            other_ctx = self._actors[other]
            if actor in other_ctx.watched_by:
                other_ctx.watched_by.remove(actor)

    async def _shutdown(
        self,
        timeout: Optional[Union[int, float]] = None
    ) -> None:
        """Internal method to shutdown the system.

        Args:
            timeout: Optional timeout for graceful shutdown
        """
        if self._actors:
            for actor_ref in self._children:
                self.stop(actor_ref)

            lifecycle_tasks = [
                task for task
                in (actor_ctx.lifecycle for actor_ctx in self._actors.values())
                if task is not None
            ]

            done, pending = await asyncio.wait(lifecycle_tasks, timeout=timeout)
            for lifecycle_task in pending:
                lifecycle_task.cancel()

        self._is_stopped.set()
        self._actor_names.clear()

    @staticmethod
    def _validate_actor_ref(
        actor: Union[Actor, ActorRef]
    ) -> ActorRef:
        """Validate and return ActorRef.

        Args:
            actor: Actor or ActorRef

        Returns:
            ActorRef
        """
        from ..actor.base import Actor as ActorClass

        if isinstance(actor, ActorClass):
            actor = actor.actor_ref

        if not isinstance(actor, ActorRef):
            raise ValueError(f"Not an actor: {actor}")

        return actor

    def get_actor_ref_by_name(
        self,
        name: str
    ) -> Optional[ActorRef]:
        """Get ActorRef by name.

        Args:
            name: Actor name

        Returns:
            ActorRef or None
        """
        return self._actor_names.get(name)

    def get_actor_instance(
        self,
        actor_ref: ActorRef
    ) -> Optional[Actor]:  # ✅ Аннотация работает через TYPE_CHECKING
        """Get actor instance by ActorRef (local actors only)."""
        actor_ctx = self._actors.get(actor_ref)
        if actor_ctx and actor_ctx.actor_instance is not None:
            return actor_ctx.actor_instance  # type: ignore

        log.debug(f"Actor instance not found (likely remote): {actor_ref}")
        return None

    def get_actor_instance_by_path(
        self,
        path: str
    ) -> Optional[Actor]:
        """Get actor instance by path (local actors only)."""
        for actor_ref, actor_ctx in self._actors.items():
            if actor_ref.path == path and actor_ctx.actor_instance is not None:
                return actor_ctx.actor_instance  # type: ignore

        log.debug(f"Actor instance by path not found (likely remote): {path}")
        return None

class _ActorContext:
    """Actor execution context.

    Holds all state needed for actor execution.
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

        from ..actor.mailbox import Mailbox
        self.mailbox: Mailbox = Mailbox()

        self.lifecycle: Optional[asyncio.Task] = None
        self.watching: Set[ActorRef] = set()
        self.watched_by: Set[ActorRef] = set()
        self.children: List[ActorRef] = []
        self.is_stopped: asyncio.Event = asyncio.Event()
        self.receiving_messages: bool = False
        self.actor_instance: Optional[Actor] = None

    @property
    def is_running(self) -> bool:
        """Check if actor is running."""
        return self.receiving_messages and not self.is_stopped.is_set()
