# actio/registry.py
# -*- coding: utf-8 -*-

import logging
import asyncio

from typing import Dict
from typing import Any
from typing import Optional
from typing import List

from . import ActorRef
from . import ActorDefinition
# from . import ActorSystem

log = logging.getLogger('actio.registry')

class ActorRegistry:
    def __init__(self):
        self._definitions: Dict[str, ActorDefinition] = {}
        self._dynamic_definitions: Dict[str, ActorDefinition] = {}
        self._actor_instances: Dict[str, List[ActorRef]] = {}

    def actio(
        self,
        name: Optional[str] = None,
        parent: Optional[str] = None,
        replicas: int = 1,
        minimal: int = 1,
        dynamic: bool = False,
        config: Optional[Dict[str, Any]] = None
    ):

        def decorator(cls):
            actor_name = name or cls.__name__
            definition = ActorDefinition(
                name=actor_name,
                cls=cls,
                parent=parent,
                replicas=replicas,
                minimal=minimal,
                dynamic=dynamic,
                config=config or {}
            )

            if dynamic:
                self._dynamic_definitions[actor_name] = definition
            else:
                self._definitions[actor_name] = definition

            return cls
        return decorator

    async def build_actor_tree(
        self,
        system: 'ActorSystem',
        timeout: float = 5.0
    ) -> Dict[str, ActorRef]:
        refs = {}
        actor_instances = {}

        # Создаем корневые акторы через систему
        for defn in self._definitions.values():
            if defn.parent is None:
                actor_instance = defn.cls()
                refs[defn.name] = system.create(actor_instance, name=defn.name)
                actor_instances[defn.name] = actor_instance

        # Создаем дочерние акторы через родительские ЭКЗЕМПЛЯРЫ акторов
        created = set(refs.keys())
        while len(created) < len(self._definitions):
            for defn in self._definitions.values():
                if defn.name not in created and defn.parent in created:
                    parent_instance = actor_instances[defn.parent]  # Экземпляр актора-родителя
                    actor_instance = defn.cls()

                    # создаем через parent_instance.create(), а не parent_ref.create()
                    child_ref = parent_instance.create(actor_instance, name=defn.name)
                    refs[defn.name] = child_ref
                    actor_instances[defn.name] = actor_instance

                    # Сохраняем ссылку в родителе
                    if hasattr(parent_instance, 'actors') and isinstance(parent_instance.actors, dict):
                        parent_instance.actors[defn.name] = child_ref

                    created.add(defn.name)

        start_time = asyncio.get_event_loop().time()
        while asyncio.get_event_loop().time() - start_time < timeout:
            all_started = True
            for actor_instance in actor_instances.values():
                if (
                    hasattr(actor_instance, '_context')
                    and actor_instance._context
                    and not actor_instance._context.receiving_messages
                ):
                    all_started = False
                    break

            if all_started:
                break
            await asyncio.sleep(0.1)

        return refs

    def register_instance(self, template_name: str, actor_ref: ActorRef):
        """Регистрируем созданный экземпляр динамического актора"""
        if template_name not in self._actor_instances:
            self._actor_instances[template_name] = []
        self._actor_instances[template_name].append(actor_ref)

    def get_actor_graph(self) -> Dict[Optional[str], List[str]]:
        graph = {}

        # Статические акторы
        for defn in self._definitions.values():
            if defn.parent not in graph:
                graph[defn.parent] = []
            graph[defn.parent].append(defn.name)

        # Динамические шаблоны
        for defn in self._dynamic_definitions.values():
            if defn.parent not in graph:
                graph[defn.parent] = []
            graph[defn.parent].append(defn.name)

        return graph

    def print_actor_tree(self):
        """Печатает дерево акторов в консоль"""
        graph = self.get_actor_graph()
        instances = self.get_dynamic_instances()

        def print_node(parent: Optional[str], level: int = 0):
            indent = "│   " * level
            if parent in graph:
                for child in graph[parent]:
                    defn = self._definitions.get(child) or self._dynamic_definitions.get(child)
                    if defn:
                        marker = " 🎯" if defn.dynamic else " ♻️"
                        # Печатаем шаблон
                        log.warning(f"{indent}├── {child}{marker}")

                        # Если это динамический шаблон - печатаем экземпляры
                        if defn.dynamic and child in instances:
                            for instance in instances[child]:
                                log.warning(f"{indent}│   ├── {instance} 🌀")

                        print_node(child, level + 1)

        log.warning("Actor System Tree:")
        print_node(None)

    def get_dynamic_instances(self) -> Dict[str, List[str]]:
        """Возвращает template_name -> list(instance_names)"""
        instances = {}
        for template_name, actor_refs in self._actor_instances.items():
            instances[template_name] = [ref.name for ref in actor_refs]
        return instances

registry = ActorRegistry()
actio = registry.actio
