# actio/actor/base.py
# -*- coding: utf-8 -*-
"""Base Actor class with lifecycle hooks."""

from __future__ import annotations

import logging

from typing import Any
from typing import Optional
from typing import Union
from typing import List
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..system.base import ActorSystem

from ..ref import ActorRef

from .context import ActorContext

log = logging.getLogger("actio.actor")


class Actor:
    """Base actor class with lifecycle hooks."""

    _context: Optional[ActorContext] = None
    _definition: Optional[object] = None

    async def started(self) -> None:
        """Called when actor is started."""
        pass

    async def receive(
        self,
        sender: ActorRef,
        message: Any
    ) -> None:
        """Called when actor receives a message."""
        raise NotImplementedError

    async def restarted(
        self,
        sender: ActorRef,
        message: Any,
        error: Exception
    ) -> None:
        """Called when an error occurs in receive()."""
        log.exception(
            "%s failed to receive message %s from %s",
            self.actor_ref, message, sender, exc_info=error
        )

    async def stopped(self) -> None:
        """Called when actor is stopped."""
        pass

    async def create(  # ✅ СДЕЛАТЬ ASYNC
        self,
        actor: "Actor",
        name: Optional[str] = None
    ) -> ActorRef:
        """Create a child actor."""
        await self._set_actor_definition(actor)  # ✅ await
        child_ref = self.system._create(actor=actor, parent=self.actor_ref, name=name)
        self.watch(child_ref)
        return child_ref

    async def _set_actor_definition(  # ✅ СДЕЛАТЬ ASYNC
        self,
        actor: "Actor"
    ) -> None:
        """Set actor definition from registry."""
        try:
            from ..registry import get_default_registry
            actor_class = type(actor)

            registry = get_default_registry()

            # ✅ await async методы
            definitions = await registry.get_definitions()
            for defn in definitions:
                if defn.cls == actor_class:
                    actor._definition = defn
                    return

            dynamic_definitions = await registry.get_dynamic_definitions()
            for defn in dynamic_definitions:
                if defn.cls == actor_class:
                    actor._definition = defn
                    return

            log.debug(f"No definition found for actor class: {actor_class.__name__}")
        except ImportError:
            log.debug("Registry not available for setting actor definition")

    def tell(  # ✅ ОСТАВИТЬ СИНХРОННЫМ
        self,
        actor: Union["Actor", ActorRef],
        message: Any
    ) -> None:
        """Send a message to another actor."""
        # ✅ Проверка на None sender
        sender = self.actor_ref if hasattr(self, 'actor_ref') else None
        self.system._tell(actor=actor, message=message, sender=sender)

    def watch(
        self,
        actor: ActorRef
    ) -> None:
        """Watch another actor for termination."""
        self.system._watch(actor=self.actor_ref, other=actor)

    def unwatch(
        self,
        actor: ActorRef
    ) -> None:
        """Stop watching another actor."""
        self.system._unwatch(actor=self.actor_ref, other=actor)

    def stop(self) -> None:
        """Stop this actor."""
        self.system.stop(actor=self.actor_ref)

    @property
    def actor_ref(self) -> ActorRef:
        """Get this actor's reference."""
        assert self._context is not None, "Actor context not initialized"
        return self._context.actor_ref

    @property
    def system(self) -> ActorSystem:
        """Get the actor system."""
        assert self._context is not None, "Actor context not initialized"
        return self._context.system

    @property
    def parent(self) -> Optional[ActorRef]:
        """Get this actor's parent reference."""
        assert self._context is not None, "Actor context not initialized"
        return self._context.parent

    @property
    def name(self) -> str:
        """Get this actor's name."""
        return self.actor_ref.name

    @property
    def path(self) -> str:
        """Get this actor's path."""
        return self.actor_ref.path

    @property
    def children(self) -> List[ActorRef]:
        """Get this actor's children references."""
        assert self._context is not None, "Actor context not initialized"
        return self._context.children

    def __str__(self) -> str:
        return self.path

    def __repr__(self) -> str:
        return self.path

    async def _route_message_logic(
        self,
        sender: ActorRef,
        message: dict
    ) -> bool:
        """Built-in routing logic for route_message actions."""
        action = message.get("action")
        if action != "route_message":
            return False

        destination: str = message.get("destination", "")
        data = message.get("data")

        # 1. Empty destination - message for this actor
        if not destination:
            final_message = data if isinstance(data, dict) else {"data": data}
            final_message["source"] = message.get("source")
            await self.receive(sender, final_message)
            return True

        # 2. Parse path
        path_parts = [p for p in destination.split("/") if p]
        if not path_parts:
            final_message = data if isinstance(data, dict) else {"data": data}
            await self.receive(sender, final_message)
            return True

        first_part = path_parts[0]
        remaining_path = "/".join(path_parts[1:]) if len(path_parts) > 1 else None

        # 3. Find child
        target_child_ref: Optional[ActorRef] = None
        child_actors_dict = getattr(self, "actors", None)
        if child_actors_dict and isinstance(child_actors_dict, dict):
            target_child_ref = child_actors_dict.get(first_part)
        else:
            assert self._context is not None, "Actor context not initialized"
            for child_ref in self._context.children:
                if child_ref.name == first_part:
                    target_child_ref = child_ref
                    break

        if target_child_ref:
            # Forward to child
            forwarded_message = message.copy()
            forwarded_message["destination"] = remaining_path
            current_source = forwarded_message.get("source", "")
            log.debug(
                f"[{self.actor_ref.path}] Routing to child '{first_part}' "
                f"with new destination '{remaining_path}'"
            )
            self.tell(target_child_ref, forwarded_message)
        else:
            # Forward to parent
            if self.parent:
                forwarded_message = message.copy()
                current_source = forwarded_message.get("source", "")
                forwarded_message["source"] = f"{self.actor_ref.name}/{current_source}".strip("/")
                log.debug(f"[{self.actor_ref.path}] Forwarding to parent")
                self.tell(self.parent, forwarded_message)
            else:
                log.warning(
                    f"[{self.actor_ref.path}] Cannot route message, "
                    f"no parent and '{first_part}' not found among children"
                )

        return True
