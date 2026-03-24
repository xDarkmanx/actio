# actio/protocols/registry.py
# -*- coding: utf-8 -*-
"""Registry protocol interface for actor discovery and orchestration."""

from typing import Protocol
from typing import Dict
from typing import List
from typing import Optional
from typing import Any

from typing import runtime_checkable

from ..ref import ActorRef
from ..ref import ActorDefinition

@runtime_checkable
class RegistryProtocol(Protocol):
    """Async interface for actor registry.

    All implementations must be asyncio-safe (no blocking calls).
    Implement this for Redis, RabbitMQ, or other distributed backends.
    """

    async def register(
        self,
        actor_name: str,
        actor_ref: ActorRef,
        node_id: str
    ) -> None:
        """Register an actor replica on a node."""
        ...

    async def unregister(
        self,
        actor_name: str,
        node_id: str
    ) -> None:
        """Unregister an actor replica from a node."""
        ...

    async def get_replicas(
        self,
        actor_name: str
    ) -> Dict[str, ActorRef]:
        """Get all replicas: {node_id: ActorRef}."""
        ...

    async def get_any_replica(
        self,
        actor_name: str
    ) -> Optional[ActorRef]:
        """Get any available replica (prefer local in standalone)."""
        ...

    async def register_definition(
        self,
        definition: ActorDefinition
    ) -> None:
        """Register an actor class definition."""
        ...

    async def get_definitions(
        self
    ) -> List[ActorDefinition]:
        """Get all registered definitions."""
        ...

    async def get_dynamic_definitions(
        self
    ) -> List[ActorDefinition]:
        """Get dynamic actor definitions."""
        ...

    async def register_instance(
        self,
        template_name: str,
        actor_ref: ActorRef
    ) -> None:
        """Register a dynamic actor instance."""
        ...

    async def get_dynamic_instances(
        self
    ) -> Dict[str, List[str]]:
        """Get dynamic actor instances: {template_name: [instance_names]}."""
        ...

    async def build_actor_tree(
        self,
        system: Any,
        timeout: float = 5.0
    ) -> Dict[str, List[ActorRef]]:
        """Build actor tree for orchestration (root actors first)."""
        ...

    async def close(
        self
    ) -> None:
        """Cleanup resources (connections, tasks, etc.)."""
        ...

    # Optional helpers (can be no-op in simple impls)
    def get_actors_for_orchestration(
        self
    ) -> List[ActorDefinition]:
        """Return actors that need orchestration (parent != None)."""
        ...

    def get_topologically_sorted_actors(
        self
    ) -> List[ActorDefinition]:
        """Topological sort: parents before children."""
        ...

    def print_actor_tree(
        self
    ) -> None:
        """Print actor tree for debugging."""
        ...
