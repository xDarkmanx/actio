# actio/registry/local.py
# -*- coding: utf-8 -*-
"""In-memory registry for standalone mode.

Zero dependencies, works without network.
Thread-safe via asyncio.Lock.
"""

import asyncio
import logging

from typing import Dict
from typing import List
from typing import Optional
from typing import Any

from ..protocols import RegistryProtocol
from ..ref import ActorRef
from ..ref import ActorDefinition

log = logging.getLogger("actio.registry.local")


class LocalRegistry(RegistryProtocol):
    """In-memory registry for standalone mode."""

    def __init__(self) -> None:
        self._definitions: Dict[str, ActorDefinition] = {}
        self._dynamic_definitions: Dict[str, ActorDefinition] = {}
        self._actor_replicas: Dict[str, Dict[str, ActorRef]] = {}
        self._actor_instances: Dict[str, List[ActorRef]] = {}
        self._lock = asyncio.Lock()
        self._closed = False

    async def register(
        self,
        actor_name: str,
        actor_ref: ActorRef,
        node_id: str
    ) -> None:
        """Register an actor replica on a node."""
        if self._closed:
            return

        async with self._lock:
            self._actor_replicas.setdefault(actor_name, {})

            if node_id != "local" and "local" in self._actor_replicas[actor_name]:
                del self._actor_replicas[actor_name]["local"]

            self._actor_replicas[actor_name][node_id] = actor_ref

        log.debug(f"Registered replica {actor_name} on node {node_id}")

    async def unregister(
        self,
        actor_name: str,
        node_id: str
    ) -> None:
        """Unregister an actor replica from a node."""
        if self._closed:
            return

        async with self._lock:
            self._actor_replicas.get(actor_name, {}).pop(node_id, None)

        log.debug(f"Unregistered replica {actor_name} from node {node_id}")

    async def get_replicas(
        self,
        actor_name: str
    ) -> Dict[str, ActorRef]:
        """Get all replicas: {node_id: ActorRef}."""
        async with self._lock:
            return dict(self._actor_replicas.get(actor_name, {}))

    async def get_any_replica(
        self,
        actor_name: str
    ) -> Optional[ActorRef]:
        """Get any available replica (prefer local in standalone)."""
        replicas = await self.get_replicas(actor_name)

        if not replicas:
            return None

        if "local" in replicas:
            return replicas["local"]

        return next(iter(replicas.values()), None)

    async def register_definition(
        self,
        definition: ActorDefinition
    ) -> None:
        """Register an actor class definition."""
        if self._closed:
            return

        async with self._lock:
            target = self._dynamic_definitions if definition.dynamic else self._definitions
            target[definition.name] = definition

        log.debug(f"Registered definition: {definition.name}")

    async def get_definitions(
        self
    ) -> List[ActorDefinition]:
        """Get all registered definitions."""
        async with self._lock:
            return list(self._definitions.values()) + list(self._dynamic_definitions.values())

    async def get_dynamic_definitions(
        self
    ) -> List[ActorDefinition]:
        """Get dynamic actor definitions."""
        async with self._lock:
            return list(self._dynamic_definitions.values())

    async def register_instance(
        self,
        template_name: str,
        actor_ref: ActorRef
    ) -> None:
        """Register a dynamic actor instance."""
        if self._closed:
            return

        async with self._lock:
            self._actor_instances.setdefault(template_name, []).append(actor_ref)

    async def get_dynamic_instances(
        self
    ) -> Dict[str, List[str]]:
        """Get dynamic actor instances: {template_name: [instance_names]}."""
        async with self._lock:
            return {
                template: [ref.name for ref in refs]
                for template, refs in self._actor_instances.items()
            }

    async def build_actor_tree(
        self,
        system: Any,  # ActorSystem
        timeout: float = 10.0
    ) -> None:
        """Build actor tree recursively.

        Creates static actors (dynamic=False) in parent-first order.
        Dynamic actors (dynamic=True) are NOT created here - use self.create() at runtime.

        Args:
            system: ActorSystem instance to create actors with
            timeout: Timeout for actor startup
        """
        log.info("Building actor tree...")

        # 1. Найти все корневые статические определения
        root_definitions = [
            defn for defn in self._definitions.values()
            if defn.parent is None and not defn.dynamic
        ]

        # 2. Создать корни
        for defn in root_definitions:
            await self._create_actor_recursive(system, defn)

        # 3. Ждём пока все акторы запустятся
        started_actors = [
            ctx for ctx in system._actors.values()
            if ctx.actor_instance is not None
        ]
        if started_actors:
            try:
                await asyncio.wait_for(
                    asyncio.gather(
                        *(ctx.is_stopped.wait() for ctx in started_actors),
                        return_exceptions=True
                    ),
                    timeout=0.1  # Просто даём время на started()
                )
            except asyncio.TimeoutError:
                pass  # Нормально - акторы продолжают работать

        log.info("Root actors started")

    async def _create_actor_recursive(
        self,
        system: Any,
        parent_defn: ActorDefinition
    ) -> Optional[ActorRef]:
        """Recursively create an actor and its static children."""
        # Создаём экземпляр актора
        try:
            actor_instance = parent_defn.cls()
        except Exception as e:
            log.error(f"Failed to instantiate {parent_defn.name}: {e}")
            return None

        # Создаём в системе
        try:
            actor_ref = system.create(actor_instance, name=parent_defn.name)
            log.info(f"Created actor: {parent_defn.name} on node local")
        except Exception as e:
            log.error(f"Failed to create actor {parent_defn.name}: {e}")
            return None

        # ✅ ВАЖНО: Регистрируем в _actor_replicas (передаём node_id!)
        try:
            await self.register(parent_defn.name, actor_ref, node_id="local")  # ← Добавили node_id
        except Exception as e:
            log.warning(f"Failed to register {parent_defn.name} in replicas: {e}")

        # Находим статических детей этого актора
        child_definitions = [
            defn for defn in self._definitions.values()
            if defn.parent == parent_defn.name and not defn.dynamic
        ]

        # Рекурсивно создаём детей
        for child_defn in child_definitions:
            await self._create_actor_recursive(system, child_defn)

        return actor_ref

    async def close(
        self
    ) -> None:
        """Cleanup resources."""
        async with self._lock:
            self._closed = True
            self._definitions.clear()
            self._actor_replicas.clear()
            self._actor_instances.clear()

        log.info("LocalRegistry closed")

    def get_actors_for_orchestration(
        self
    ) -> List[ActorDefinition]:
        """Return static actors that need orchestration (parent != None)."""
        all_actors = self.get_topologically_sorted_actors()
        return [
            d for d in all_actors
            if not d.dynamic and d.parent is not None
        ]

    def get_topologically_sorted_actors(
        self
    ) -> List[ActorDefinition]:
        """Topological sort: parents before children."""
        graph: Dict[Optional[str], List[str]] = {}

        for defn in self._definitions.values():
            graph.setdefault(defn.parent, []).append(defn.name)

        for defn in self._dynamic_definitions.values():
            graph.setdefault(defn.parent, []).append(defn.name)

        in_degree: Dict[Optional[str], int] = {}
        for parent, children in graph.items():
            in_degree.setdefault(parent, 0)
            for child in children:
                in_degree[child] = in_degree.get(child, 0) + 1

        queue = [n for n, deg in in_degree.items() if deg == 0]
        result: List[ActorDefinition] = []

        while queue:
            node = queue.pop(0)
            if node is not None:
                defn = self._definitions.get(node) or self._dynamic_definitions.get(node)
                if defn:
                    result.append(defn)

            for child in graph.get(node, []):
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)

        return result

    def print_actor_tree(
        self
    ) -> None:
        """Print actor tree for debugging."""
        graph: Dict[Optional[str], List[str]] = {}

        # Учитываем ОБА словаря: статические + динамические
        for defn in list(self._definitions.values()) + list(self._dynamic_definitions.values()):
            graph.setdefault(defn.parent, []).append(defn.name)

        def print_node(parent: Optional[str], level: int = 0) -> None:
            indent = "│    " * level
            for child in graph.get(parent, []):
                # Ищем в обоих словарях
                defn = self._definitions.get(child) or self._dynamic_definitions.get(child)
                if defn:
                    marker = " 🎯" if defn.dynamic else " ♻️"

                    # Считаем реплики из ОБОИХ словарей
                    static_replicas = len(self._actor_replicas.get(child, {}))
                    dynamic_replicas = len(self._actor_instances.get(child, []))
                    replica_count = static_replicas + dynamic_replicas

                    replica_info = f" [{replica_count}/{defn.replicas}]"
                    log.warning(f"{indent}├── {child}{marker}{replica_info}")
                    print_node(child, level + 1)

        log.warning("Actor System Tree:")
        print_node(None)

# --- Backward compatibility ---
_default_registry: Optional[LocalRegistry] = None


def get_default_registry() -> LocalRegistry:
    """Get or create default registry instance (for @actio decorator)."""
    global _default_registry
    if _default_registry is None:
        _default_registry = LocalRegistry()
    return _default_registry
