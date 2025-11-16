# actio/registry.py
# -*- coding: utf-8 -*-

import logging
import asyncio

from typing import Dict
from typing import Any
from typing import Optional
from typing import Union
from typing import List
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from . import ActorSystem

from . import ActorRef
from . import ActorDefinition

log = logging.getLogger('actio.registry')


class ActorRegistry:
    def __init__(self):
        self._definitions: Dict[str, ActorDefinition] = {}
        self._dynamic_definitions: Dict[str, ActorDefinition] = {}
        self._actor_instances: Dict[str, List[ActorRef]] = {}
        self._actor_replicas: Dict[str, Dict[str, ActorRef]] = {}  # actor_name -> {node_id: ActorRef}

    def actio(
        self,
        name: Optional[str] = None,
        parent: Optional[str] = None,
        replicas: Union[int, str] = 1,
        minimal: int = 1,
        dynamic: bool = False,
        config: Optional[Dict[str, Any]] = None
    ):

        def decorator(cls):
            actor_name = name or cls.__name__
            if isinstance(replicas, str) and replicas != 'all':
                raise ValueError(f"Invalid replicas value: {replicas}. Must be integer or 'all'")

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
    ) -> Dict[str, List[ActorRef]]:
        """Создает ТОЛЬКО корневые акторы (parent=None) с сохранением оригинальных имен"""
        refs = {}
        actor_instances = {}

        # ШАГ 1: Создаем ТОЛЬКО корневые акторы (parent=None)
        for defn in self._definitions.values():
            if defn.parent is None:  # Только корневые акторы
                refs[defn.name] = []

                # Создаем актор с оригинальным именем
                actor_instance = defn.cls()

                # Получаем node_id для регистрации
                node_id = "local"
                if hasattr(actor_instance, 'config') and actor_instance.config:
                    node_id = actor_instance.config.node_id

                # Создаем с оригинальным именем!
                ref = system.create(actor_instance, name=defn.name)
                refs[defn.name].append(ref)
                actor_instances[defn.name] = actor_instance

                # Регистрируем под оригинальным именем
                self._register_replica(defn.name, node_id, ref)
                log.info(f"🏁 Created root actor: {defn.name} on node {node_id}")

        # ШАГ 2: Ждем инициализации корневых акторов
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

        log.info("✅ Root actors started. Waiting for leader orchestration...")
        return refs

    def get_actors_for_orchestration(self) -> List[ActorDefinition]:
        """Возвращает акторы для оркестрации CrushMapper"""
        actors = []
        for defn in self._definitions.values():
            # Оркестрируем: dynamic=False И parent != None
            if defn.dynamic is False and defn.parent is not None:
                actors.append(defn)
        return actors

    def _register_replica(self, actor_name: str, node_id: str, actor_ref: ActorRef):
        """Регистрирует реплику актора"""
        if actor_name not in self._actor_replicas:
            self._actor_replicas[actor_name] = {}
        self._actor_replicas[actor_name][node_id] = actor_ref
        log.info(f"✅ Registered replica {actor_name} on node {node_id}: {actor_ref}")

    def register_instance(self, template_name: str, actor_ref: ActorRef):
        """Регистрируем созданный экземпляр динамического актора"""
        if template_name not in self._actor_instances:
            self._actor_instances[template_name] = []
        self._actor_instances[template_name].append(actor_ref)

    def get_actor_replicas(self, actor_name: str) -> Dict[str, ActorRef]:
        """Возвращает все реплики актора {node_id: ActorRef}"""
        log.debug(f"🔍 Registry lookup for {actor_name}: available keys {list(self._actor_replicas.keys())}")
        return self._actor_replicas.get(actor_name, {})

    def get_any_replica(self, actor_name: str) -> Optional[ActorRef]:
        """Возвращает любую работающую реплику актора"""
        replicas = self.get_actor_replicas(actor_name)
        if not replicas:
            return None

        # В standalone режиме возвращаем первую реплику
        if "local" in replicas:
            return replicas["local"]

        # В кластерном режиме возвращаем первую доступную
        return next(iter(replicas.values()))

    def get_actor_replica_count(self, actor_name: str) -> int:
        """Возвращает количество зарегистрированных реплик"""
        return len(self.get_actor_replicas(actor_name))

    def find_replica_by_node(self, actor_name: str, node_id: str) -> Optional[ActorRef]:
        """Находит реплику актора на конкретной ноде"""
        replicas = self.get_actor_replicas(actor_name)
        return replicas.get(node_id)

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
                        replica_count = self.get_actor_replica_count(child)

                        # Показываем информацию о репликах только если их >1
                        replica_info = f" [{replica_count}/{defn.replicas}]" if defn.replicas > 1 else ""

                        log.warning(f"{indent}├── {child}{marker}{replica_info}")

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
