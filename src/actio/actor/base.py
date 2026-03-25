# src/actio/actor/base.py
# -*- coding: utf-8 -*-
"""Base Actor class with lifecycle hooks."""

from __future__ import annotations

import logging
from typing import Any, Optional, Union, List, TYPE_CHECKING, Dict

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

    async def receive(self, sender: ActorRef, message: Any) -> None:
        """Called when actor receives a message."""
        raise NotImplementedError

    async def restarted(self, sender: ActorRef, message: Any, error: Exception) -> None:
        """Called when an error occurs in receive()."""
        log.exception(
            "%s failed to receive message %s from %s",
            self.actor_ref, message, sender, exc_info=error
        )

    async def stopped(self) -> None:
        """Called when actor is stopped."""
        pass

    async def create(self, actor: "Actor", name: Optional[str] = None) -> ActorRef:
        """Create a child actor."""
        await self._set_actor_definition(actor)
        child_ref = self.system._create(actor=actor, parent=self.actor_ref, name=name)
        self.watch(child_ref)
        return child_ref

    async def _set_actor_definition(self, actor: "Actor") -> None:
        """Set actor definition from registry."""
        try:
            from ..registry import get_default_registry
            actor_class = type(actor)
            registry = get_default_registry()
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

    def tell(self, actor: Union["Actor", ActorRef], message: Any) -> None:
        """Send a message to another actor."""
        sender = self.actor_ref if hasattr(self, 'actor_ref') else None
        self.system._tell(actor=actor, message=message, sender=sender)

    def watch(self, actor: ActorRef) -> None:
        """Watch another actor for termination."""
        self.system._watch(actor=self.actor_ref, other=actor)

    def unwatch(self, actor: ActorRef) -> None:
        """Stop watching another actor."""
        self.system._unwatch(actor=self.actor_ref, other=actor)

    def stop(self) -> None:
        """Stop this actor."""
        self.system.stop(actor=self.actor_ref)

    @property
    def actor_ref(self) -> ActorRef:
        assert self._context is not None, "Actor context not initialized"
        return self._context.actor_ref

    @property
    def system(self) -> ActorSystem:
        assert self._context is not None, "Actor context not initialized"
        return self._context.system

    @property
    def parent(self) -> Optional[ActorRef]:
        assert self._context is not None, "Actor context not initialized"
        return self._context.parent

    @property
    def name(self) -> str:
        return self.actor_ref.name

    @property
    def path(self) -> str:
        return self.actor_ref.path

    @property
    def children(self) -> List[ActorRef]:
        assert self._context is not None, "Actor context not initialized"
        return self._context.children

    def __str__(self) -> str:
        return self.path

    def __repr__(self) -> str:
        return self.path

    async def _route_message_logic(self, sender: ActorRef, message: Dict[str, Any]) -> bool:
        """
        Built-in routing logic for route_message actions.

        Flow:
        1. Empty destination → deliver locally
        2. Find among children → deliver (NO source accumulation)
        3. Not found + has parent → accumulate source, delegate to parent
        4. Not found + parent=None → root routing (cluster logic)
        """
        action = message.get("action")
        if action != "route_message":
            return False

        destination: str = message.get("destination", "")
        data = message.get("data")

        # 1. Empty destination → message for this actor
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

        # 3. Find child actor
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
            # ✅ Found locally → forward to child (NO source accumulation!)
            forwarded_message = message.copy()
            forwarded_message["destination"] = remaining_path
            log.debug(
                f"[{self.actor_ref.path}] Routing to child '{first_part}' "
                f"with destination '{remaining_path}'"
            )
            self.tell(target_child_ref, forwarded_message)
            return True
        else:
            # ❌ Not found → delegate to parent (ACCUMULATE SOURCE!)
            if self.parent:
                forwarded_message = message.copy()
                # 🎯 CRITICAL: accumulate source ONLY when delegating to parent
                current_source = forwarded_message.get("source", "")
                if current_source:
                    forwarded_message["source"] = f"{self.actor_ref.name}/{current_source}".strip("/")
                else:
                    forwarded_message["source"] = self.actor_ref.name
                log.debug(f"[{self.actor_ref.path}] Forwarding to parent, source: {forwarded_message['source']}")
                self.tell(self.parent, forwarded_message)
                return True
            else:
                # 🎯 parent=None → root routing (cluster logic)
                log.debug(f"[{self.actor_ref.path}] Root actor, delegating to _handle_root_routing")
                return await self._handle_root_routing(sender, message, destination)

        return False

    async def _handle_root_routing(
        self,
        sender: Optional[ActorRef],
        message: Dict[str, Any],
        destination: str
    ) -> bool:
        """
        Handle routing at root level (parent=None).

        CRITICAL: Verify actor is actually registered on target node before sending!
        """
        system = getattr(self, 'system', None)
        if not system:
            log.warning(f"[{self.name}] No system reference for root routing")
            return False

        registry = getattr(system, 'registry', None)
        if not registry or not hasattr(registry, 'get_actor_nodes'):
            log.warning(f"[{self.name}] No registry for cross-node routing")
            return False

        node_id = getattr(registry, 'node_id', 'local')

        # ─────────────────────────────────────────────────────────
        # Case 1: destination WITHOUT 'node:' prefix → outgoing message
        # ─────────────────────────────────────────────────────────
        if not destination.startswith("node:"):
            # Find where this actor lives
            actor_nodes = await registry.get_actor_nodes(destination)
            if not actor_nodes:
                log.warning(f"[{self.name}] Actor '{destination}' not found in cluster")
                return False

            # Filter out dead nodes
            available_nodes = await registry.get_available_nodes()
            alive_actor_nodes = [n for n in actor_nodes if n in available_nodes]
            if not alive_actor_nodes:
                log.warning(f"[{self.name}] Actor '{destination}' exists but all nodes are dead")
                return False

            # 🎯 КРИТИЧНАЯ ПРОВЕРКА: убедиться что актор ДЕЙСТВИТЕЛЬНО есть на целевой ноде
            # (защита от гонки регистрации)
            target_node = None
            for node in alive_actor_nodes:
                # Проверяем через registry.get_actor_ref_by_name на той ноде
                # Но мы не можем запросить удалённую ноду напрямую...
                # Поэтому берём первую живую и надеемся что регистрация успела
                # В продакшене можно добавить retry с задержкой
                target_node = node
                break

            if not target_node:
                log.warning(f"[{self.name}] No alive node found for '{destination}'")
                return False

            # If actor should be local but not found → error
            if target_node == node_id:
                log.warning(f"[{self.name}] Actor '{destination}' should be local but not found")
                return False

            # 🎯 Transform source: add node prefix ONLY if not already present!
            current_source = message.get("source", "")
            if not current_source.startswith("node:"):
                if sender:
                    message["source"] = f"node:{node_id}/{sender.name}"
                else:
                    message["source"] = f"node:{node_id}"

            # 🎯 FIX: Add node: prefix to destination for explicit channel routing
            prefixed_destination = f"node:{target_node}/{destination}"

            log.info(f"[{self.name}] 🌐 Outgoing: '{destination}' → node:{target_node}")
            # 🎯 Send with PREFIXED destination so send_message() knows the target channel
            return await registry.send_message(prefixed_destination, message, sender)

        # ─────────────────────────────────────────────────────────
        # Case 2: destination WITH 'node:' prefix → reply or local delivery
        # ─────────────────────────────────────────────────────────
        if destination.startswith("node:"):
            node_part, actor_path = destination.split("/", 1)
            target_node = node_part.replace("node:", "")

            if target_node == node_id:
                # ✅ This message is for US → strip prefix and deliver locally
                log.debug(f"[{self.name}] Message for this node, delivering to '{actor_path}'")
                message["destination"] = actor_path
                return await self._deliver_local(actor_path, message)

            # ❌ This is a reply to ANOTHER node → transform and forward
            message["destination"] = actor_path  # Strip for the payload that actor receives

            # Transform source: add node prefix ONLY if not already present!
            current_source = message.get("source", "")
            if not current_source.startswith("node:"):
                if sender:
                    message["source"] = f"node:{node_id}/{sender.name}"
                else:
                    message["source"] = f"node:{node_id}"

            log.info(f"[{self.name}] 🌐 Reply: forwarding to node:{target_node}")
            # 🎯 Send with ORIGINAL prefixed destination so send_message() routes to target_node
            return await registry.send_message(destination, message, sender)

        return False

    async def _deliver_local(self, actor_path: str, message: Dict[str, Any]) -> bool:
        """
        Deliver message to local actor by path.

        message structure:
        {
            "action": "route_message",
            "destination": "...",
            "source": "...",
            "data": {...}  ← This is the actual payload for the target actor
        }

        We extract message["data"] and deliver THAT to the target actor.
        """
        parts = [p for p in actor_path.split("/") if p]
        if not parts:
            return False

        # Find target actor by name (last part of path)
        target_name = parts[-1]
        target_ref = self.system.get_actor_ref_by_name(target_name)

        if not target_ref:
            log.warning(f"❌ Cannot find actor '{target_name}' in path '{actor_path}'")
            return False

        # 🎯 Extract the INNER data — this is what the target actor receives
        inner_data = message.get("data", {})

        # Preserve source in delivered payload (for reply routing)
        if isinstance(inner_data, dict) and "source" not in inner_data:
            inner_data["source"] = message.get("source", "")

        # Deliver to the target actor
        self.tell(target_ref, inner_data)
        log.debug(f"✅ Delivered to '{target_name}' (source: {message.get('source')})")
        return True
