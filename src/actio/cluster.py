# actio/cluster.py
# -*- coding: utf-8 -*

import logging
import asyncio
import json
import time
import random
import socket

from typing import Any
from typing import Dict
from typing import Set
from typing import Optional
from typing import List

from .crush import Crush
from . import Actor
from . import ActorRef
from . import ActorDefinition
from . import ActioConfig
from . import registry
from . import Terminated

log = logging.getLogger("actio.cluster")


class ClusterActor(Actor):
    def __init__(self):
        super().__init__()

        self.config: Optional[ActioConfig] = None
        self.crush_mapper = Crush()
        self.server = None
        self.conn: Dict[str, asyncio.StreamWriter] = {}
        self.members: Dict[str, Dict] = {}
        self.goss_tgt: Set[str] = set()
        self._cluster_initialized = False
        self._is_leader = False
        self._election_task = None
        self._orchestration_task = None
        self._orchestration_done = False

    async def started(self) -> None:
        """Переопределяем started для правильной инициализации"""
        await super().started()
        await self.cluster_started()

    def set_config(self, config: ActioConfig):
        """Устанавливаем конфигурацию извне"""
        self.config = config

        self.crush_mapper.update_nodes({
            config.node_id: {
                "status": "alive",
                "last_seen": time.time(),
                "weight": config.node_weight,
                "resources": config.resources,
                "actor_count": 1
            }
        })

        log.info(f"ClusterActor configured for node: {self.config.node_id}")

    async def cluster_started(self) -> None:
        """Вызывается после инициализации контекста"""
        if not self.config:
            log.error("ClusterActor started without configuration!")
            return

        log.info(f"ClusterActor started for node: {self.config.node_id}")
        registry._register_replica(self.actor_ref.name, self.config.node_id, self.actor_ref)
        await self._broadcast_replica_update(self.actor_ref.name, "add", self.actor_ref)

        # Запускаем кластер
        if not self._cluster_initialized:
            await self.cluster_init()

    async def cluster_init(self) -> None:
        """Инициализация кластера"""
        if self._cluster_initialized:
            return

        self.server = await asyncio.start_server(
            self._conn_hdl, self.config.node_ip, self.config.cluster_port
        )

        self.members[self.config.node_id] = {
            "status": "alive",
            "last_seen": time.time(),
            "incarnation": 0,
            "address": f"{self.config.node_id}:{self.config.cluster_port}",
            "resources": self.config.resources,
            "actor_count": 1
        }

        # Подключаемся к другим нодам
        await self._nodes_conn()

        # Запускаем фоновые задачи
        asyncio.create_task(self._goss_loop())
        asyncio.create_task(self._failure_detect())
        asyncio.create_task(self._heartbeat())
        asyncio.create_task(self._background_connector())

        # Запускаем выборы лидера
        self._election_task = asyncio.create_task(self._leader_election_loop())

        self._cluster_initialized = True
        log.info(f"✅ Cluster node {self.config.node_id} fully initialized")

    async def _route_message_logic(self, sender: ActorRef, message: Dict[str, Any]) -> bool:
        action = message.get('action')
        if action != 'route_message':
            return False

        destination = message.get('destination', '')
        log.info(f"🔍 ClusterActor routing: destination='{destination}' from {sender}")

        # 🔥 1.5. УМНЫЙ RESOLVE ЛОГИЧЕСКИХ ПУТЕЙ (НОВАЯ ЛОГИКА)
        if '/' in destination and not destination.startswith('node:'):
            resolved_destination = await self._resolve_logical_path(destination)
            if resolved_destination:
                log.info(f"🎯 Resolved logical path: {destination} → {resolved_destination}")
                forward_message = message.copy()
                forward_message['destination'] = resolved_destination

                return await self._cluster_route(resolved_destination[5:], forward_message, sender)
                # return await self._try_cluster_routing(forward_message, sender)

        # 1. Существующая логика (node: префикс)
        if destination.startswith('node:'):
            log.info(f"🎯 Routing to specific node: {destination}")
            return await self._cluster_route(destination[5:], message, sender)

        # 2. Существующая логика (пустой destination)
        if not destination:
            data = message.get('data')
            final_message = data if isinstance(data, dict) else {'data': data}
            final_message['source'] = message.get('source')
            log.info("📨 Processing message locally (no destination)")
            await self.receive(sender, final_message)
            return True

        # 3. Существующая логика (кластерная маршрутизация)
        if self._cluster_initialized:
            log.info(f"🌐 Attempting cluster routing for: {destination}")
            cluster_handled = await self._try_cluster_routing(message, sender)
            if cluster_handled:
                log.info("✅ Message routed via cluster")
                return True

        # 4. Существующая логика (fallback к локальной)
        log.info(f"🔄 Falling back to local routing for: {destination}")
        handled_locally = await super()._route_message_logic(sender, message)
        if handled_locally:
            log.info("✅ Message handled locally")
            return True

        log.warning(f"🚫 Message could not be routed to: {destination}")
        return False

    async def _try_cluster_routing(self, message: Dict[str, Any], sender: ActorRef) -> bool:
        """Пытается найти актор в кластере и перенаправить сообщение"""
        if not self._cluster_initialized:
            log.debug("Cluster not initialized, skipping cluster routing")
            return False

        destination = message.get('destination', '')
        if not destination:
            return False

        log.info(f"🔍 Searching for actor '{destination}' in cluster registry...")

        # Ищем актор в кластере через registry
        target_ref = registry.get_any_replica(destination)
        if not target_ref:
            log.info(f"🔍 Actor '{destination}' not found in cluster registry")
            return False

        # Нашли актор в кластере - определяем ноду
        log.info(f"📍 Found actor '{destination}' in cluster: {target_ref}")

        # Ищем на какой ноде находится этот актор
        target_node_id = None
        replicas = registry.get_actor_replicas(destination)
        for node_id, ref in replicas.items():
            if ref == target_ref:
                target_node_id = node_id
                break

        if not target_node_id:
            log.warning(f"🚫 Could not determine target node for {destination}")
            return False

        if (
            target_node_id in self.members
            and self.members[target_node_id].get("status") != "alive"
        ):
            log.warning(
                f"🚫 Target node {target_node_id} is not alive "
                f"(status: {self.members[target_node_id].get('status')}). "
                f"Skipping cluster routing."
            )
            return False

        if target_node_id == self.config.node_id:
            log.info(f"🎯 Target is local, delivering to {destination}")

            # Это гарантирует что сообщение пройдет всю цепочку маршрутизации правильно!
            handled = await super()._route_message_logic(sender, message)
            if handled:
                log.info(f"✅ Local message delivered to {destination}")
            else:
                log.warning(f"🚫 Local message could not be delivered to {destination}")
            return handled

        log.info(f"🎯 Routing to remote node {target_node_id}")

        # Формируем новое сообщение для пересылки
        forward_message = message.copy()
        current_source = message.get('source', '')

        # Обновляем source для отслеживания пути
        if current_source:
            forward_message['source'] = f"node:{self.config.node_id}/{current_source}"
        else:
            forward_message['source'] = f"node:{self.config.node_id}"

        # Пересылаем на целевую ноду
        success = await self._forward_to_cluster_node(target_node_id, forward_message, sender)
        if success:
            log.info(f"✅ Successfully routed to node {target_node_id}")
        else:
            log.error(f"❌ Failed to route to node {target_node_id}")

        return success

    async def _cluster_route(self, node_and_path: str, message: Dict[str, Any], sender: ActorRef) -> bool:
        """Маршрутизация на конкретную ноду в формата node:node_id/path"""
        try:
            parts = node_and_path.split('/', 1)
            target_node = parts[0]
            remaining_path = parts[1] if len(parts) > 1 else ''

            log.info(f"🎯 Cluster routing to node {target_node}, path: {remaining_path}")

            # Формируем сообщение для пересылки
            forward_message = message.copy()
            forward_message['destination'] = remaining_path

            current_source = message.get('source', '')
            if current_source:
                forward_message['source'] = f"node:{self.config.node_id}/{current_source}"
            else:
                forward_message['source'] = f"node:{self.config.node_id}"

            await self._forward_to_cluster_node(target_node, forward_message, sender)
            return True

        except Exception as e:
            log.error(f"❌ Cluster routing error for {node_and_path}: {e}")
            return False

    async def _forward_to_cluster_node(self, node_id: str, message: Dict[str, Any], sender: ActorRef) -> bool:
        """Пересылает сообщение на указанную ноду кластера"""
        if not self._cluster_initialized:
            return False

        if node_id == self.config.node_id:
            self._context.letterbox.put_nowait((sender, message))
            return True

        connection_id = self._find_connection_for_node(node_id)
        if connection_id:
            try:
                await self._send_msg(connection_id, message)
                return True
            except Exception as e:
                log.error(f"❌ Failed to send to {node_id}: {e}")
        return False

    def _find_connection_for_node(self, node_id: str) -> Optional[str]:
        """Находит соединение для указанной ноды"""
        if node_id in self.conn:
            return node_id

        for conn_id in self.conn.keys():
            if node_id in conn_id or conn_id in node_id:
                return conn_id

        if node_id in self.members:
            member_address = self.members[node_id].get('address', '')
            if member_address:
                for conn_id in self.conn.keys():
                    if member_address == conn_id or conn_id in member_address:
                        return conn_id

        return None

    async def _leader_election_loop(self):
        """Цикл выборов лидера"""
        while True:
            try:
                await self._run_leader_election()
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"❌ Error in leader election loop: {e}")
                await asyncio.sleep(30)

    async def _run_leader_election(self):
        """Выборы лидера - самая маленькая нода становится лидером"""
        if not self.members:
            return

        if not self._has_quorum():
            if self._is_leader:
                self._is_leader = False
                log.warning(f"⚠️ Lost leadership due to no quorum: {self.config.node_id}")
                if self._orchestration_task:
                    self._orchestration_task.cancel()
                    self._orchestration_task = None
            return

        alive_nodes = [
            node_id for node_id, member in self.members.items()
            if member.get("status") == "alive"
        ]

        if not alive_nodes:
            return

        alive_nodes.sort()
        new_leader = alive_nodes[0]

        was_leader = self._is_leader
        self._is_leader = (new_leader == self.config.node_id)

        if self._is_leader and not was_leader:
            log.info(f"🎯 This node is now the cluster leader: {self.config.node_id}")
            if self._has_quorum():
                await self._announce_leadership()
                if self._orchestration_task:
                    self._orchestration_task.cancel()
                self._orchestration_task = asyncio.create_task(self._orchestrate_all_actors())
            else:
                log.warning(
                    f"⚠️ Elected as leader but NO quorum "
                    f"({self._get_alive_count()}/{len(self.config.cluster_nodes)}), "
                    f"waiting for majority"
                )
        elif was_leader and not self._is_leader:
            log.info(f"❌ This node is no longer the leader: {self.config.node_id}")
            if self._orchestration_task:
                self._orchestration_task.cancel()
                self._orchestration_task = None

    async def _orchestrate_all_actors(self):
        """Оркестрирует ВСЕ static акторы волнами с ожиданием"""
        if not self._is_leader:
            return

        await asyncio.sleep(5)
        log.info("🔄 Leader starting orchestration of all static actors...")

        await self._sync_all_replicas()
        await self._cleanup_dead_replicas()

        # Получаем акторы для оркестрации
        actors_to_orchestrate = registry.get_actors_for_orchestration()
        if not actors_to_orchestrate:
            log.info("✅ No actors to orchestrate")
            self._orchestration_done = True
            return

        log.info(f"🎯 Actors for orchestration: {[a.name for a in actors_to_orchestrate]}")

        self.crush_mapper.update_nodes(self.members)

        # Разделение на волны
        generations = self._build_generation_waves(actors_to_orchestrate)
        if not generations:
            return

        log.info(f"🌊 Generations for orchestration: {[[a.name for a in gen] for gen in generations]}")

        # Оркестрация по волнам
        commands_sent = 0
        for gen_idx, gen_actors in enumerate(generations):
            log.info(f"🔄 Orchestration wave {gen_idx}: {[a.name for a in gen_actors]}")

            wave_commands = await self._orchestrate_wave(gen_idx, gen_actors)
            commands_sent += wave_commands

            # Ждем завершения волны перед следующей
            await self._wait_for_wave_completion(gen_actors, timeout=30.0)

        log.info(f"✅ Leader sent {commands_sent} create commands in {len(generations)} waves")
        self._orchestration_done = True

        # Финальная проверка
        log.info("📊 Final replica distribution:")
        for defn in registry._definitions.values():
            if defn.parent is not None:  # Только дочерние акторы
                replicas = registry.get_actor_replicas(defn.name)
                log.info(f"   {defn.name}: {list(replicas.keys())}")

    async def _sync_all_replicas(self):
        """Синхронизирует реплики со всеми нодами"""
        log.info("🔄 Synchronizing replicas across all nodes...")
        for actor_name in list(registry._actor_replicas.keys()):
            for node_id, actor_ref in registry._actor_replicas[actor_name].items():
                if node_id == self.config.node_id:
                    await self._broadcast_replica_update(actor_name, "add", actor_ref)
        log.info("✅ Replica synchronization completed")

    def _build_generation_waves(self, actors_to_orchestrate: List[ActorDefinition]) -> List[List[ActorDefinition]]:
        """Строит волны поколений акторов"""
        cluster_actor_parents = set()
        for defn in registry._definitions.values():
            if issubclass(defn.cls, ClusterActor):
                cluster_actor_parents.add(defn.name)

        generations = []
        remaining_actors = set(actors_to_orchestrate)

        while remaining_actors:
            current_gen = []
            for defn in list(remaining_actors):
                parent_in_prev_gens = any(defn.parent == prev_defn.name for gen_list in generations for prev_defn in gen_list)
                if defn.parent in cluster_actor_parents or parent_in_prev_gens:
                    current_gen.append(defn)

            if not current_gen:
                log.error(f"❌ Could not place any actors in next generation. Remaining: {[a.name for a in remaining_actors]}")
                break

            generations.append(current_gen)
            for defn in current_gen:
                remaining_actors.remove(defn)

        return generations

    async def _orchestrate_wave(self, gen_idx: int, wave_actors: List[ActorDefinition]) -> int:
        """Оркестрирует одну волну акторов с диагностикой"""
        commands_sent = 0

        for actor_def in wave_actors:
            target_nodes = self._get_target_nodes_for_actor(actor_def)

            # 🔥 ДИАГНОСТИКА
            log.info(f"🎯 Orchestrating {actor_def.name}: target_nodes={target_nodes}")

            if not target_nodes:
                log.warning(f"❌ No target nodes for {actor_def.name}")
                continue

            for replica_index, node_id in enumerate(target_nodes):
                current_replicas = registry.get_actor_replicas(actor_def.name)

                # Проверяем есть ли уже реплика на этой ноде
                if node_id in current_replicas:
                    log.info(f"⏭️  Skipping {actor_def.name} on {node_id} - already exists")
                    continue

                # 🔥 ПРОВЕРЯЕМ ЕСТЬ ЛИ РОДИТЕЛЬ НА ЭТОЙ НОДЕ
                parent_ref = self._find_parent_for_creation(node_id, actor_def)
                if not parent_ref:
                    log.error(f"❌ Cannot create {actor_def.name} on {node_id} - parent not found")
                    continue

                success = await self._send_create_command(node_id, actor_def, replica_index)
                if success:
                    commands_sent += 1
                    log.info(f"✅ Wave {gen_idx}: Sent create command for {actor_def.name} to {node_id}")
                else:
                    log.error(f"❌ Failed to send create command for {actor_def.name} to {node_id}")

        return commands_sent

    def _get_target_nodes_for_actor(self, actor_def: ActorDefinition) -> List[str]:
        """Возвращает целевые ноды для актора с учетом родительских реплик"""
        if actor_def.parent:
            parent_replicas = registry.get_actor_replicas(actor_def.parent)

            if not parent_replicas:
                # Проверяем, является ли родитель ClusterActor
                parent_is_cluster_actor = any(
                    defn.name == actor_def.parent and issubclass(defn.cls, ClusterActor)
                    for defn in registry._definitions.values()
                )

                if parent_is_cluster_actor:
                    log.info(f"🎯 Parent {actor_def.parent} is ClusterActor, using all nodes for {actor_def.name}")
                    return self.crush_mapper.map_actor(actor_def.name, actor_def.replicas)
                else:
                    log.warning(f"⚠️ Parent {actor_def.parent} not found for {actor_def.name}")
                    return []

            # Родитель найден - распределяем по нодам где есть родитель
            available_parent_nodes = [
                node_id for node_id in parent_replicas.keys()
                if node_id in self.crush_mapper.nodes
            ]

            if not available_parent_nodes:
                return []

            if len(available_parent_nodes) == len(self.crush_mapper.nodes):
                return self.crush_mapper.map_actor(actor_def.name, actor_def.replicas)
            else:
                # Ограничиваем распределение нодами где есть родитель
                original_nodes = self.crush_mapper.nodes
                try:
                    self.crush_mapper.nodes = {
                        node_id: weight
                        for node_id, weight in self.crush_mapper.nodes.items()
                        if node_id in available_parent_nodes
                    }
                    target_nodes = self.crush_mapper.map_actor(actor_def.name, actor_def.replicas)
                    return target_nodes
                finally:
                    self.crush_mapper.nodes = original_nodes

        # Корневой актор - распределяем по всем нодам
        return self.crush_mapper.map_actor(actor_def.name, actor_def.replicas)

    async def _send_create_command(self, node_id: str, actor_def: ActorDefinition, replica_index: int) -> bool:
        """Отправляет команду создания актора"""
        try:
            # Находим правильного родителя для создания
            parent_ref = self._find_parent_for_creation(node_id, actor_def)
            if not parent_ref:
                return False

            if node_id == self.config.node_id:
                # Локальное создание
                return await self._create_locally(actor_def, replica_index, parent_ref)
            else:
                # Удаленное создание
                return await self._send_remote_create_command(node_id, actor_def, replica_index, parent_ref)

        except Exception as e:
            log.error(f"❌ Error sending create command for {actor_def.name}: {e}")
            return False

    def _find_parent_for_creation(self, node_id: str, actor_def: ActorDefinition) -> Optional[ActorRef]:
        """Находит правильного родителя для создания актора"""
        if not actor_def.parent:
            # 🔥 КОРНЕВОЙ АКТОР - ищем СТАТИЧЕСКИЕ акторы с parent=None на указанной ноде
            for defn in registry._definitions.values():
                if defn.parent is None:  # Это корневой актор
                    replicas = registry.get_actor_replicas(defn.name)
                    parent_ref = replicas.get(node_id)
                    if parent_ref and parent_ref.name == defn.name:
                        return parent_ref

            log.warning(f"❌ No root actor found on node {node_id} for creating {actor_def.name}")
            log.warning(f"   Available root actors: {[d.name for d in registry._definitions.values() if d.parent is None]}")
            log.warning(f"   Registry replicas: {dict((k, list(v.keys())) for k, v in registry._actor_replicas.items())}")
            return None
        else:
            # Дочерний актор - создается через своего родителя
            parent_replicas = registry.get_actor_replicas(actor_def.parent)

            # 🔥 ИЩЕМ РОДИТЕЛЯ ПО ИМЕНИ (без replica_index)
            for ref_node_id, parent_ref in parent_replicas.items():
                if ref_node_id == node_id and parent_ref.name == actor_def.parent:
                    return parent_ref

            log.warning(f"❌ Parent {actor_def.parent} not found on node {node_id}")
            log.warning(
                f"   Available on node: {[
                    ref.name for ref in parent_replicas.values() if
                    ref.name.startswith(actor_def.parent)
                ]}")
            return None

    async def _create_locally(self, actor_def: ActorDefinition, replica_index: int, parent_ref: ActorRef) -> bool:
        """Создает актор локально через правильного родителя"""
        try:
            if parent_ref == self.actor_ref:
                return await self._create_directly(actor_def, replica_index)
            else:
                parent_actor = self.system.get_actor_instance(parent_ref)
                if parent_actor:
                    log.info(f"🎯 Creating {actor_def.name} via parent {actor_def.parent}.create()")
                    actor_instance = actor_def.cls()
                    ref = parent_actor.create(actor_instance, name=actor_def.name)

                    if ref:
                        await self._broadcast_replica_update(actor_def.name, "add", ref)
                        self._update_node_metrics()
                        log.info(f"✅ Successfully created {actor_def.name} via parent {actor_def.parent}")
                        return True
                return False
        except Exception as e:
            log.error(f"❌ Failed to create {actor_def.name} locally: {e}")
            return False

    async def _create_directly(self, actor_def: ActorDefinition, replica_index: int) -> bool:
        """Создает актор напрямую"""
        try:
            actor_instance = actor_def.cls()
            ref = self.create(actor_instance, name=actor_def.name)

            if ref:
                await self._broadcast_replica_update(actor_def.name, "add", ref)
                self._update_node_metrics()
                log.info(f"✅ Successfully created {actor_def.name} on {self.config.node_id}")
                return True

            return False
        except Exception as e:
            log.error(f"❌ Failed to create {actor_def.name} directly: {e}")
            return False

    async def _send_remote_create_command(
        self,
        node_id: str,
        actor_def: ActorDefinition,
        replica_index: int,
        parent_ref: ActorRef
    ) -> bool:
        """Отправляет команду создания на удаленную ноду"""
        conn_id = self._find_connection_for_node(node_id)
        if not conn_id:
            return False

        try:
            await self._send_msg(
                conn_id,
                {
                    "action": "replica_command",
                    "actor_name": actor_def.name,
                    "command": "create",
                    "replica_index": replica_index,
                    "parent_ref": {
                        "actor_id": parent_ref.actor_id,
                        "path": parent_ref.path,
                        "name": parent_ref.name
                    },
                    "from_leader": self.config.node_id
                }
            )
            return True
        except Exception as e:
            log.error(f"❌ Failed to send remote create command to {node_id}: {e}")
            return False

    async def _wait_for_wave_completion(self, wave_actors, timeout: float = 60.0):
        """Ждет пока все акторы волны будут созданы И зарегистрированы в реестре"""
        start_time = asyncio.get_event_loop().time()
        remaining_actors = {actor.name for actor in wave_actors}

        log.info(f"⏳ Waiting for wave completion: {list(remaining_actors)}")

        for node_id in self.conn:
            if node_id != self.config.node_id:
                replica_sync_msg = {
                    'action': 'replica_sync_request',
                    'from_leader': self.config.node_id,
                    'wave_actors': [a.name for a in wave_actors]
                }
                await self._send_msg(node_id, replica_sync_msg)

        await asyncio.sleep(1.0)

        iteration = 0
        while remaining_actors and (asyncio.get_event_loop().time() - start_time) < timeout:
            iteration += 1
            completed_actors = set()

            for actor_name in list(remaining_actors):
                replicas = registry.get_actor_replicas(actor_name)
                actor_def = next((a for a in wave_actors if a.name == actor_name), None)

                if not actor_def:
                    continue

                expected_count = self._get_expected_replica_count(actor_def)
                target_nodes = self._get_target_nodes_for_actor(actor_def)
                received_from_nodes = set(replicas.keys())

                if iteration % 5 == 1:
                    missing_nodes = [n for n in target_nodes if n not in received_from_nodes]
                    if missing_nodes:
                        log.debug(f"⏳ {actor_name}: waiting updates from nodes {missing_nodes}")

                # Проверяем что получили от всех целевых нод
                received_from_all_targets = all(node_id in received_from_nodes for node_id in target_nodes)

                if not received_from_all_targets:
                    continue

                all_running = True
                for node_id in target_nodes:
                    if node_id == self.config.node_id:
                        actor_ref = replicas.get(node_id)
                        if actor_ref:
                            actor_instance = self.system.get_actor_instance(actor_ref)
                            if not (
                                actor_instance and
                                hasattr(actor_instance, '_context') and
                                actor_instance._context and
                                actor_instance._context.receiving_messages
                            ):
                                all_running = False
                                log.debug(f'⏳ {actor_name}: local actor on {node_id} not started')
                                break

                if all_running and len(replicas) >= expected_count:
                    completed_actors.add(actor_name)
                    log.info(f'✅ Actor {actor_name} completed: {len(replicas)}/{expected_count} replicas')
                    log.info(f'   Replicas on nodes: {list(replicas.keys())}')

            remaining_actors -= completed_actors

            if remaining_actors:
                if iteration % 3 == 0:
                    for actor_name in remaining_actors:
                        actor_def = next((a for a in wave_actors if a.name == actor_name), None)
                        if actor_def:
                            target_nodes = self._get_target_nodes_for_actor(actor_def)
                            current_replicas = registry.get_actor_replicas(actor_name)

                            for node_id in target_nodes:
                                if (
                                    node_id != self.config.node_id
                                    and node_id not in current_replicas
                                ):
                                    conn_id = self._find_connection_for_node(node_id)
                                    if conn_id:
                                        log.debug(f'📨 Requesting sync for {actor_name} from {node_id}')

                                        replica_sync_msg = {
                                            'action': 'replica_sync_request',
                                            'actor_name': actor_name,
                                            'from_leader': self.config.node_id,
                                            'request_id': f'sync_{actor_name}_{node_id}_{iteration}'
                                        }

                                        await self._send_msg(conn_id, replica_sync_msg)

                log.debug(f'⏳ Still waiting for: {list(remaining_actors)}')
                await asyncio.sleep(2.0)
            else:
                await asyncio.sleep(1.0)
                log.info('🎯 Wave completed: all actors created and registered')
                return

        if remaining_actors:
            log.warning(f'⚠️ Timeout waiting for actors: {list(remaining_actors)}')
            # Детальная диагностика
            for actor_name in remaining_actors:
                replicas = registry.get_actor_replicas(actor_name)
                actor_def = next((a for a in wave_actors if a.name == actor_name), None)
                if actor_def:
                    expected_count = self._get_expected_replica_count(actor_def)
                    target_nodes = self._get_target_nodes_for_actor(actor_def)
                    missing_nodes = [n for n in target_nodes if n not in replicas]
                    log.warning(f"   {actor_name}:")
                    log.warning(f"     Expected on nodes: {target_nodes}")
                    log.warning(f"     Registered on: {list(replicas.keys())}")
                    log.warning(f"     Missing from: {missing_nodes}")
                    log.warning(f"     Current replicas: {len(replicas)}/{expected_count}")

    def _get_expected_replica_count(self, actor_def: ActorDefinition) -> int:
        """Возвращает ожидаемое количество реплик"""
        if actor_def.replicas == 'all':
            return len(self.crush_mapper.nodes)
        else:
            return int(actor_def.replicas)

    async def _cleanup_dead_replicas(self):
        """Очищает реплики на мертвых нодах"""
        cleanup_count = 0
        for actor_name in list(registry._actor_replicas.keys()):
            for node_id in list(registry._actor_replicas[actor_name].keys()):
                if (
                    node_id in self.members
                    and self.members[node_id].get("status") == "dead"
                ):
                    del registry._actor_replicas[actor_name][node_id]
                    cleanup_count += 1

        if cleanup_count > 0:
            if self._is_leader:
                for node_id in list(self.members.keys()):
                    if (
                        node_id != self.config.node_id
                        and self.members[node_id]['status'] == 'alive'
                    ):
                        await self._send_msg(node_id, {
                            'action': 'cleanup_dead_replicas'
                        })

            log.info(f"🧹 Cleaned up {cleanup_count} dead replicas")

    def _update_node_metrics(self):
        """Обновляет метрики ноды"""
        if self.config.node_id in self.members:
            self.members[self.config.node_id]["actor_count"] = \
                self.members[self.config.node_id].get("actor_count", 0) + 1
            self.crush_mapper.update_nodes(self.members)

    def _has_quorum(self) -> bool:
        """Проверяет есть ли кворум для принятия решений"""
        if not self.config:
            return False

        total_configured = len(self.config.cluster_nodes)

        if total_configured < 2:
            log.error("Cluster must have at least 2 nodes")
            return False

        # Считаем живых (включая себя)
        known_alive = 1  # Я всегда жив

        for member in self.config.cluster_nodes:
            node_id = member.split(':')[0]
            if node_id == self.config.node_id:
                continue

            node = self.members.get(node_id)
            if node and node.get("status") == "alive":
                known_alive += 1

        # Для 2 нод: нужны ОБЕ живы
        if total_configured == 2:
            return known_alive == 2

        # Для 3+ нод: strict majority
        return known_alive > total_configured / 2

    def _get_alive_count(self) -> int:
        """Возвращает количество живых нод (включая себя)"""
        alive = 1  # Я
        for member in self.config.cluster_nodes:
            node_id = member.split(':')[0]
            if node_id == self.config.node_id:
                continue

            node = self.members.get(node_id)
            if node and node.get("status") == "alive":
                alive += 1
        return alive

    async def receive(self, sender: ActorRef, message: Any) -> None:
        """Обрабатывает входящие сообщения"""
        log.info(f"🔍 ClusterActor.receive: {type(message).__name__} from {sender}")

        if isinstance(message, Terminated):
            log.info(f"{message.actor.name} has stopped")

        action = message.get('action', None)
        match action:
            case 'create_child':
                child_actor_name = message["child_actor_name"]
                replica_index = message.get("replica_index", 0)

                log.info(f"🎯 Received create_child command for {child_actor_name}")
                await self._create_directly_by_name(child_actor_name, replica_index)
                return
            case 'replica_command':
                await self._process_replica_command(message)
                return
            case 'actio_broadcast':
                for node_id in self.conn:
                    await self._send_msg(node_id, message["payload"])
                return

    async def _process_replica_command(self, message: Dict[str, Any]):
        """Обрабатывает команды реплик"""
        actor_name = message["actor_name"]
        command = message["command"]
        replica_index = message.get("replica_index", 0)
        parent_ref_data = message.get("parent_ref")

        log.info(f"🔄 Processing replica command: {command} for {actor_name}")

        match command:
            case 'create':
                if parent_ref_data:
                    # Восстанавливаем ActorRef родителя
                    parent_ref = ActorRef(
                        actor_id=parent_ref_data["actor_id"],
                        path=parent_ref_data["path"],
                        name=parent_ref_data["name"]
                    )

                    # Проверяем, являемся ли мы целевым родителем
                    if parent_ref == self.actor_ref:
                        await self._create_directly_by_name(actor_name, replica_index)
                    else:
                        log.info(f"🔄 Getting parent instance: {parent_ref}")
                        parent_actor = self.system.get_actor_instance(parent_ref)

                        if parent_actor:
                            log.info(f"✅ Found parent instance, creating {actor_name} via parent.create()")
                            await self._create_via_parent(parent_actor, actor_name, replica_index)
                        else:
                            log.error(f"❌ Parent instance not found: {parent_ref}")
                else:
                    # Обратная совместимость
                    await self._create_directly_by_name(actor_name, replica_index)
                return
            case 'stop':
                log.info(f"🛑 Received stop command for {actor_name}")
                # Получаем actor_ref из сообщения
                actor_ref_data = message.get("actor_ref")
                if not actor_ref_data:
                    log.error(f"❌ No actor_ref in stop command for {actor_name}")
                    return

                actor_ref = ActorRef(
                    actor_id=actor_ref_data["actor_id"],
                    path=actor_ref_data["path"],
                    name=actor_ref_data["name"]
                )

                # Находим актор и останавливаем
                actor_instance = self.system.get_actor_instance_by_path(actor_ref.path)
                if actor_instance:
                    log.info(f"🛑 Stopping actor {actor_ref.path}")
                    actor_instance.stop()
                else:
                    log.warning(f"⚠️ Actor instance not found for stop: {actor_ref.path}")

                return
            case _:
                log.warning(f"⚠️ Unknown replica command: {command} for {actor_name}")
                return

    async def _create_via_parent(self, parent_actor: Actor, actor_name: str, replica_index: int):
        """Создает актор через экземпляр родителя"""
        try:
            # Находим определение актора
            actor_def = None
            for defn in registry._definitions.values():
                if defn.name == actor_name:
                    actor_def = defn
                    break

            if not actor_def:
                log.error(f"❌ Actor definition not found: {actor_name}")
                return

            # Создаем экземпляр актора
            actor_instance = actor_def.cls()
            ref = parent_actor.create(actor_instance, name=actor_name)

            if ref:
                await self._broadcast_replica_update(actor_name, "add", ref)
                log.info(f"✅ Successfully created {actor_name} via parent {parent_actor.actor_ref.name}")
            else:
                log.error(f"❌ Failed to create {actor_name} via parent")

        except Exception as e:
            log.error(f"❌ Error creating {actor_name} via parent: {e}")

    async def _create_directly_by_name(self, actor_name: str, replica_index: int):
        """Создает актор по имени"""
        try:
            defn = None
            for d in registry._definitions.values():
                if d.name == actor_name:
                    defn = d
                    break

            if defn:
                await self._create_directly(defn, replica_index)
            else:
                log.error(f"❌ Actor definition not found: {actor_name}")
        except Exception as e:
            log.error(f"❌ Failed to create {actor_name}: {e}")

    async def _broadcast_replica_update(self, actor_name: str, command: str, actor_ref: ActorRef = None):
        """Рассылает обновление о реплике и регистрирует локально"""
        if not self._cluster_initialized:
            return

        if command == "add" and actor_ref:
            current_replicas = registry.get_actor_replicas(actor_name)
            if self.config.node_id not in current_replicas:
                registry._register_replica(actor_name, self.config.node_id, actor_ref)
                log.info(f'📝 Registered replica {actor_name} on node {self.config.node_id}')
            else:
                log.debug(f'🔍 Replica {actor_name} already registered on node {self.config.node_id}')

        message = {
            'action': 'replica_update',
            'actor_name': actor_name,
            'node_id': self.config.node_id,
            'command': command,
            'timestamp': time.time()
        }

        if command == 'add' and actor_ref:
            message['actor_ref'] = {
                'actor_id': actor_ref.actor_id,
                'path': actor_ref.path,
                'name': actor_ref.name
            }

        # Рассылаем другим нодам
        for node_id in self.conn:
            if node_id != self.config.node_id:
                try:
                    await self._send_msg(node_id, message)
                    log.debug(f'📤 Broadcasted replica update for {actor_name} to {node_id}')
                except Exception as e:
                    log.error(f'❌ Failed to broadcast to {node_id}: {e}')

    async def _goss_loop(self):
        while True:
            if self.goss_tgt:
                tgt = random.choice(list(self.goss_tgt))
                if tgt in self.conn:
                    await self._send_msg(
                        tgt,
                        {
                            "action": "gossip",
                            "node_id": self.config.node_id,
                            "members": self.members,
                            "incarnation": self.members[self.config.node_id]["incarnation"],
                        },
                    )
            await asyncio.sleep(1)

    async def _failure_detect(self):
        """Failure detection с кворумом"""
        while True:
            now = time.time()
            dead_nodes = []
            resurrected_nodes = []

            for node_id, member in list(self.members.items()):
                if node_id == self.config.node_id:
                    continue

                old_status = member.get("status", "alive")

                if now - member["last_seen"] > self.config.failure_timeout:
                    new_status = "dead"
                elif now - member["last_seen"] > self.config.failure_timeout / 2:
                    new_status = "suspect"
                elif member["status"] != "alive":
                    new_status = "alive"
                else:
                    new_status = "alive"

                if new_status != old_status:
                    member["status"] = new_status

                    if new_status == "dead" and old_status != "dead":
                        dead_nodes.append(node_id)
                    elif old_status in ["dead", "suspect"] and new_status == "alive":
                        resurrected_nodes.append(node_id)

            if dead_nodes or resurrected_nodes:
                self.crush_mapper.update_nodes(self.members)

            if hasattr(self, '_pending_resurrected_nodes') and self._pending_resurrected_nodes:
                for node_id in list(self._pending_resurrected_nodes):
                    # Проверяем что нода все еще alive
                    if node_id in self.members and self.members[node_id].get("status") == "alive":
                        resurrected_nodes.append(node_id)
                        log.info(f"🔍 Processing resurrected node from pending list: {node_id}")

                # Очищаем обработанные
                self._pending_resurrected_nodes.clear()

            if (dead_nodes or resurrected_nodes) and self._is_leader:
                if self._has_quorum():
                    log.info(
                        f"✅ Quorum present ({self._get_alive_count()}/{len(self.config.cluster_nodes)}), "
                        f"re-orchestrating after deaths: {dead_nodes}"
                    )
                    self._orchestration_done = False
                    if self._orchestration_task:
                        self._orchestration_task.cancel()
                    self._orchestration_task = asyncio.create_task(self._orchestrate_all_actors())
                else:
                    log.warning(
                        f"⚠️ NO quorum ({self._get_alive_count()}/{len(self.config.cluster_nodes)}), "
                        f"waiting for majority after deaths: {dead_nodes}"
                    )

            # ==================== ВОССТАНОВЛЕНИЕ НОД ====================
            if resurrected_nodes and self._is_leader:
                log.info(f"🔍 Detected resurrected nodes: {resurrected_nodes}")
                asyncio.create_task(self._handle_resurrected_nodes_with_delay(resurrected_nodes))

            await asyncio.sleep(2)

    async def _handle_resurrected_nodes_with_delay(self, resurrected_nodes):
        """Обрабатывает воскрешение нод с задержкой"""
        log.info(f"⏳ Handle resurrected nodes delay started for: {resurrected_nodes}")
        await asyncio.sleep(15.0)

        if not self._is_leader:
            log.debug("⏭️ Skipping restoration - not leader")
            return

        if not self._has_quorum():
            log.warning(f"⏭️ Skipping restoration - no quorum ({self._get_alive_count()}/{len(self.config.cluster_nodes)})")
            return

        # Проверяем стабильность
        stable_nodes = []
        for node_id in resurrected_nodes:
            member = self.members.get(node_id)
            if member and member.get("status") == "alive":
                # Проверяем что нода стабильно жива последние 5 секунд
                if time.time() - member.get("last_seen", 0) < 5.0:
                    stable_nodes.append(node_id)

        if stable_nodes:
            log.info(f"🎯 Stable resurrected nodes: {stable_nodes}, restoring actors...")
            await self._restore_resurrected_nodes(stable_nodes)
        else:
            log.info("⏭️ No stable nodes for restoration")

    async def _restore_resurrected_nodes(self, node_ids):
        """Восстанавливаем акторы на воскресших нодах: Остановить все → Очистить → Переоркестрировать"""
        if not self._is_leader or not self._has_quorum():
            log.debug('⏭️ Skipping restore_resurrected_nodes - not leader or no quorum')
            return

        for node_id in node_ids:
            log.info(f"🔄 Leader initiating FULL restore for node {node_id}")

            # 1. Остановить все акторы на ноде
            await self._stop_all_replicas_on_node(node_id)
            log.info(f"✅ Node {node_id} prepared for re-orchestration")

        # 3. Обновить CrushMapper (ноды снова живы)
        self.crush_mapper.update_nodes(self.members)

        # 4. Запустить полную оркестрацию
        log.info("🎯 Starting full orchestration after node restoration")
        self._orchestration_done = False
        if self._orchestration_task:
            self._orchestration_task.cancel()
        self._orchestration_task = asyncio.create_task(self._orchestrate_all_actors())

    async def _stop_all_replicas_on_node(self, node_id: str):
        """Останавливает ВСЕ акторы на указанной ноде (кроме ClusterActor)"""
        if node_id == self.config.node_id:
            # Локальная нода
            log.info("🛑 Stopping ALL local actors (except ClusterActor)")

            # Собираем всех акторов кроме себя
            actors_to_stop = []
            for actor_ref in list(self.system._actors.keys()):
                if actor_ref != self.actor_ref:  # Не останавливаем ClusterActor
                    actors_to_stop.append(actor_ref)

            # Останавливаем всех
            for actor_ref in actors_to_stop:
                log.info(f"🛑 Stopping {actor_ref}")
                self.system.stop(actor_ref)

            log.info(f"✅ Sent stop commands to {len(actors_to_stop)} local actors")
        else:
            # Удаленная нода
            conn_id = self._find_connection_for_node(node_id)
            if conn_id:
                log.info(f"🛑 Sending stop_all command to {node_id}")
                await self._send_msg(conn_id, {
                    "action": "stop_all",
                    "from_leader": self.config.node_id,
                    "timestamp": time.time()
                })
            else:
                log.warning(f"🚫 No connection to node {node_id} for stop_all")

    async def _nodes_conn(self):
        for node_name in self.config.cluster_nodes:
            if node_name != self.config.node_id:
                await self._node_conn(host=node_name, port=int(self.config.cluster_port))

    def _find_node_for_actor_ref(self, actor_ref: ActorRef) -> Optional[str]:
        """Находит ноду для ActorRef"""
        for actor_name, replicas in registry._actor_replicas.items():
            for node_id, ref in replicas.items():
                if ref == actor_ref:
                    return node_id
        return None

    async def _resolve_logical_path(self, destination: str) -> Optional[str]:
        """Преобразует логический путь в физический с нодой"""
        if not destination or destination.startswith('node:'):
            return None

        path_parts = [p for p in destination.split('/') if p]
        if not path_parts:
            return None

        target_actor = path_parts[-1]
        target_ref = registry.get_any_replica(target_actor)

        if not target_ref:
            return None

        target_node = self._find_node_for_actor_ref(target_ref)
        if not target_node or target_node == self.config.node_id:
            return None  # Актор локальный или не найден

        return f"node:{target_node}/{destination}"

    async def _node_conn(self, host: str, port: int, max_retries: int = 3) -> bool:
        for attempt in range(max_retries):
            try:
                reader, writer = await asyncio.open_connection(host, port)
                # node_id = f"{host}:{port}"
                self.conn[host] = writer

                asyncio.create_task(self._node_lstn(reader=reader, node_id=host))

                join_msg = {
                    "action": "node_join",
                    "node_id": self.config.node_id,
                    "port": self.config.cluster_port,
                }

                await self._send_msg(host, join_msg)
                # 🔥 ЗАПРАШИВАЕМ СУЩЕСТВУЮЩИЕ РЕПЛИКИ У НОВОЙ НОДЫ
                replica_sync_msg = {
                    'action': 'replica_sync_request',
                    'node_id': self.config.node_id
                }
                await self._send_msg(host, replica_sync_msg)

                log.info(f'✅ Connected to {host}')
                return True

            except (ConnectionRefusedError, socket.gaierror):
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                else:
                    break
            except Exception as e:
                log.debug(f'Connection error: {e}')
                break

        return False

    async def _node_lstn(self, reader: asyncio.StreamReader, node_id: str):
        try:
            while True:
                hdr = await reader.readexactly(4)
                length = int.from_bytes(hdr, "big")
                data = await reader.readexactly(length)
                msg = json.loads(data.decode("utf-8"))
                await self._process_cluster_msg(node_id, msg)

        except (asyncio.IncompleteReadError, ConnectionError):
            log.info(f"Lost connection to: {node_id}")
            writer = self.conn.pop(node_id, None)
            if writer:
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass
        except Exception as e:
            log.error(f"Error listening to {node_id}: {e}")
            writer = self.conn.pop(node_id, None)
            if writer:
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass

    async def _conn_hdl(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        peer = writer.get_extra_info("peername")
        node_id = f"{peer[0]}:{peer[1]}"

        log.info(f"New cluster connection from: {node_id}")
        self.conn[node_id] = writer

        try:
            while True:
                hdr = await reader.readexactly(4)
                length = int.from_bytes(hdr, "big")
                data = await reader.readexactly(length)
                msg = json.loads(data.decode("utf-8"))
                await self._process_cluster_msg(node_id, msg)

        except (asyncio.IncompleteReadError, ConnectionError):
            log.info(f"Lost connection to: {node_id}")
            writer = self.conn.pop(node_id, None)
            if writer:
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass
        except Exception as e:
            log.error(f"Error listening to {node_id}: {e}")
            writer = self.conn.pop(node_id, None)
            if writer:
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass

    async def _send_msg(self, node_id: str, message: Dict[str, Any]):
        if node_id in self.conn:
            writer = self.conn[node_id]
            try:
                data = json.dumps(message).encode("utf-8")
                hdr = len(data).to_bytes(4, "big")
                writer.write(hdr + data)
                await writer.drain()
            except Exception as e:
                log.error(f"Error send to {node_id}: {e}")
                self.conn.pop(node_id, None)

    async def _process_cluster_msg(self, sender_node: str, message: Dict[str, Any]):
        action = message.get("action", None)

        match action:
            case 'route_message':
                self._context.letterbox.put_nowait((self.actor_ref, message))
                log.debug(f"Injected route_message into letterbox for {self.actor_ref.path}")
                return
            case 'node_join':
                await self._handle_node_join(message)
                return
            case 'leader_announcement':
                await self._process_leader_announcement(message)
                return
            case 'gossip':
                await self._merge_member(message["members"], message["incarnation"])
                self.crush_mapper.update_nodes(self.members)
                return
            case 'replica_sync_request':
                await self._send_all_replicas_to_node(sender_node)
                return
            case 'replica_update':
                await self._process_replica_update(message)
                return
            case 'replica_command':
                await self._process_replica_command(message)
                return
            case 'heartbeat':
                if message["node_id"] in self.members:
                    self.members[message["node_id"]]["last_seen"] = time.time()
                return
            case 'cleanup_dead_replicas':
                await self._cleanup_dead_replicas()
                return
            case 'stop_all':
                await self._handle_stop_all()
                return
            case _:
                return

    async def _handle_stop_all(self, message: Dict[str, Any]):
        """Обрабатывает команду остановки всех акторов на ноде"""
        log.info(f"🛑 Received stop_all command from leader {message.get('from_leader')}")

        stopped_count = 0
        for actor_ref in list(self.system._actors.keys()):
            if actor_ref != self.actor_ref:  # Не останавливаем себя (ClusterActor)
                log.info(f"🛑 Stopping {actor_ref}")
                self.system.stop(actor_ref)
                stopped_count += 1

        log.info(f"✅ Stopped {stopped_count} local actors")

    async def _handle_node_join(self, message: Dict[str, Any]):
        """Обрабатывает подключение новой ноды или восстановление старой"""
        node_id = message['node_id']
        self.goss_tgt.add(node_id)

        # Сохраняем старый статус перед изменением
        old_status = None
        old_incarnation = 0
        if node_id in self.members:
            old_status = self.members[node_id].get("status")
            old_incarnation = self.members[node_id].get("incarnation", 0)

        # Увеличиваем incarnation, не сбрасываем
        new_incarnation = old_incarnation + 1 if node_id in self.members else 0

        # Сохраняем что нода была dead (если была)
        was_dead_before = (old_status == "dead")

        # Обновляем запись
        self.members[node_id] = {
            'status': 'alive',
            'last_seen': time.time(),
            'incarnation': new_incarnation,
            'address': f"{node_id}:{message['port']}",
            'resources': {},
            'actor_count': 1
        }

        log.info(f'Node {node_id} joined the cluster (incarnation: {new_incarnation}, was: {old_status})')
        if node_id != self.config.node_id and was_dead_before:
            if not self._find_connection_for_node(node_id):
                log.info(f"🔌 Establishing connection TO {node_id}")
                await self._node_conn(node_id, self.config.cluster_port, max_retries=2)

        self.crush_mapper.update_nodes(self.members)
        await self._send_all_replicas_to_node(node_id)

        # Если нода была dead и мы лидер - запускаем восстановление
        if was_dead_before and self._is_leader:
            log.info(f"🎯 Node {node_id} resurrected from dead, scheduling restoration...")
            if not hasattr(self, '_pending_resurrected_nodes'):
                self._pending_resurrected_nodes = []
            self._pending_resurrected_nodes.append(node_id)

    async def _send_all_replicas_to_node(self, target_node: str):
        """Отправляет все наши реплики указанной ноде"""
        for actor_name, replicas in registry._actor_replicas.items():
            for node_id, actor_ref in replicas.items():
                if node_id == self.config.node_id:  # Только наши реплики
                    await self._send_msg(
                        target_node,
                        {
                            "action": "replica_update",
                            "actor_name": actor_name,
                            "node_id": node_id,
                            "command": "add",
                            "actor_ref": {
                                "actor_id": actor_ref.actor_id,
                                "path": actor_ref.path,
                                "name": actor_ref.name
                            },
                            "timestamp": time.time()
                        }
                    )
        log.debug(f"📤 Sent all replicas to {target_node}")

    async def _process_replica_update(self, message: Dict[str, Any]):
        """Обрабатывает обновления реплик от других нод"""
        actor_name = message["actor_name"]
        node_id = message["node_id"]
        command = message["command"]

        if node_id == self.config.node_id:
            log.debug(f"🔍 Skipping local replica update from ourselves: {actor_name}")
            return

        if command == "add":
            actor_ref_data = message["actor_ref"]
            actor_ref = ActorRef(
                actor_id=actor_ref_data["actor_id"],
                path=actor_ref_data["path"],
                name=actor_ref_data["name"]
            )

            current_replicas = registry.get_actor_replicas(actor_name)
            if node_id not in current_replicas:
                registry._register_replica(actor_name, node_id, actor_ref)
                log.info(f"✅ Registered REMOTE replica {actor_name} from node {node_id}")
            else:
                log.debug(f"🔍 Replica {actor_name} from {node_id} already registered")

        elif command == "remove":
            if actor_name in registry._actor_replicas and node_id in registry._actor_replicas[actor_name]:
                del registry._actor_replicas[actor_name][node_id]
                log.info(f"🗑️ Removed remote replica {actor_name} from node {node_id}")

        self.crush_mapper.update_nodes(self.members)

    async def _process_leader_announcement(self, message: Dict[str, Any]):
        """Обрабатывает анонс лидерства"""
        announced_leader = message["leader_id"]

        alive_nodes = [
            node_id for node_id, member in self.members.items()
            if member.get("status") == "alive"
        ]

        if not alive_nodes:
            return

        alive_nodes.sort()
        true_leader = alive_nodes[0]

        if announced_leader != true_leader:
            return

        if announced_leader != self.config.node_id and self._is_leader:
            self._is_leader = False
            if self._orchestration_task:
                self._orchestration_task.cancel()
                self._orchestration_task = None

    async def _heartbeat(self):
        while True:
            for node_id in list(self.conn.keys()):
                await self._send_msg(
                    node_id,
                    {
                        "action": "heartbeat",
                        "node_id": self.config.node_id,
                        "timestamp": time.time(),
                    },
                )
            await asyncio.sleep(3)

    async def _announce_leadership(self):
        """Анонсирует свое лидерство"""
        for node_id in self.conn:
            await self._send_msg(
                node_id,
                {
                    "action": "leader_announcement",
                    "leader_id": self.config.node_id,
                    "timestamp": time.time()
                }
            )
        log.info(f"🎯 Leader {self.config.node_id} announced leadership")

    async def _merge_member(self, remote_members: Dict[str, Dict], remote_incarnation: int):
        for node_id, remote_info in remote_members.items():
            if node_id == self.config.node_id:
                if remote_incarnation > self.members[node_id]["incarnation"]:
                    self.members[node_id] = remote_info
                    self.members[node_id]["incarnation"] = remote_incarnation
                continue

            if node_id not in self.members:
                self.members[node_id] = remote_info
                self.goss_tgt.add(node_id)
                log.info(f"Discovered new node: {node_id}")
            else:
                local_info = self.members[node_id]
                if remote_info["incarnation"] > local_info["incarnation"]:
                    self.members[node_id] = remote_info
                elif (
                    remote_info["incarnation"] == local_info["incarnation"]
                    and remote_info["last_seen"] > local_info["last_seen"]
                ):
                    self.members[node_id]["last_seen"] = remote_info["last_seen"]
                    self.members[node_id]["status"] = remote_info["status"]

    async def _background_connector(self):
        """Фоновое подключение к нодам"""
        connection_attempts = {}

        while True:
            current_time = time.time()

            for node in self.config.cluster_nodes:
                try:
                    node_host, node_port = node.split(":")
                    node_id = f"{node_host}:{node_port}"

                    if node_host == self.config.node_id:
                        continue

                    if node_id in self.conn:
                        continue

                    if (
                        node_id in self.members
                        and self.members[node_id].get("status") == "dead"
                    ):
                        continue

                    last_attempt = connection_attempts.get(node_id, 0)
                    if (
                        node_id in self.members
                        and self.members[node_id].get("status") in ["suspect", "unreachable"]
                        and current_time - last_attempt < 45
                    ):
                        continue

                    success = await self._node_conn(node_host, int(node_port))
                    connection_attempts[node_id] = current_time

                    if not success:
                        if node_id not in self.members:
                            self.members[node_id] = {
                                "status": "unreachable",
                                "last_seen": current_time,
                                "incarnation": 0
                            }
                        elif self.members[node_id].get("status") == "alive":
                            self.members[node_id]["status"] = "unreachable"
                            self.members[node_id]["last_seen"] = current_time

                except Exception as e:
                    log.debug(f"Background connector error: {e}")

            await asyncio.sleep(15)

    async def stopped(self) -> None:
        """Остановка кластера"""
        if self.server:
            self.server.close()
            await self.server.wait_closed()

        for writer in self.conn.values():
            writer.close()
            await writer.wait_closed()

        if self._election_task:
            self._election_task.cancel()
        if self._orchestration_task:
            self._orchestration_task.cancel()
