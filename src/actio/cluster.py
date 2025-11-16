# actio/cluster.py
# -*- coding: utf-8 -*

import logging
import asyncio
import json
import time
import random
import hashlib
import socket

from typing import Any
from typing import Dict
from typing import Set
from typing import Optional
from typing import List
from typing import Union

from actio import Terminated

from . import Actor
from . import ActorRef
from . import ActorDefinition
from . import ActioConfig
from . import registry

log = logging.getLogger("actio.cluster")


class CrushMapper:
    def __init__(self) -> None:
        self.nodes = {}
        self.virtual_nodes = 100

    def update_nodes(self, cluster_members):
        """Обновляет информацию о нодах кластера"""
        self.nodes = {
            node_id: self._calculate_weight(member_data)
            for node_id, member_data in cluster_members.items()
            if member_data.get("status") == "alive"
        }
        log.debug(f"CrushMapper updated nodes: {list(self.nodes.keys())}")

    def _calculate_weight(self, member_data):
        """Рассчитывает вес ноды на основе ресурсов и загрузки"""
        cpu_cores = member_data.get('resources', {}).get('cpu_cores', 4)
        memory_gb = member_data.get('resources', {}).get('memory_gb', 8)
        current_load = member_data.get('actor_count', 0)

        base_weight = (cpu_cores * 0.6 + memory_gb * 0.4)
        current_weight = base_weight / (current_load + 1)

        return max(current_weight, 0.1)

    def map_actors_to_nodes(self, actor_definitions: List[ActorDefinition]) -> Dict[str, List[tuple]]:
        """Распределяет акторы по нодам: {node_id: [(actor_name, replica_index)]}"""
        if not self.nodes:
            return {}

        placement = {}

        for defn in actor_definitions:
            # Получаем целевые ноды для этого актора
            target_nodes = self.map_actor(defn.name, defn.replicas)

            for replica_index, node_id in enumerate(target_nodes):
                if node_id not in placement:
                    placement[node_id] = []
                placement[node_id].append((defn.name, replica_index))

        return placement

    def map_actor(self, actor_name: str, replicas: Union[int, str] = 1) -> List[str]:
        """Распределяет реплики актора по нодам с учетом текущего состояния"""
        if not self.nodes:
            return []

        # 🔥 Обработка replicas='all'
        if replicas == 'all':
            target_nodes = list(self.nodes.keys())
            log.info(f"🎯 CrushMapper mapped {actor_name} to ALL nodes: {target_nodes}")
            return target_nodes

        # Преобразуем в int для обратной совместимости
        replica_count = int(replicas) if isinstance(replicas, str) else replicas

        current_replicas = registry.get_actor_replicas(actor_name)
        nodes_with_replicas = set(current_replicas.keys())
        available_nodes = list(self.nodes.keys())

        if not available_nodes:
            return []

        # 🔥 Single-replica с улучшенной балансировкой
        if replica_count == 1:
            return self._map_single_replica(actor_name, nodes_with_replicas, available_nodes)

        # 🔥 Multi-replica логика
        return self._map_multi_replica(actor_name, replica_count, nodes_with_replicas, available_nodes)

    def _map_single_replica(self, actor_name: str, nodes_with_replicas: set, available_nodes: List[str]) -> List[str]:
        """Распределение single-replica акторов с балансировкой"""
        # Стратегия 1: Если есть текущая реплика и нода жива - оставляем на ней
        if nodes_with_replicas:
            current_node = next(iter(nodes_with_replicas))
            if current_node in available_nodes:
                log.info(f"🎯 CrushMapper keeping {actor_name} on current node: {current_node}")
                return [current_node]

        # Стратегия 2: Round-robin распределение с учетом загрузки
        available_nodes.sort()  # Для детерминированности
        actor_hash = hash(actor_name) % len(available_nodes)
        selected_node = available_nodes[actor_hash]

        # Стратегия 3: Если выбранная нода перегружена - найти менее загруженную
        selected_weight = self.nodes.get(selected_node, 1.0)
        if selected_weight < 0.5:  # Нода перегружена
            best_node = max(available_nodes, key=lambda n: self.nodes.get(n, 1.0))
            log.info(f"🔄 CrushMapper rebalanced {actor_name} from {selected_node} to {best_node} (load balancing)")
            return [best_node]

        log.info(f"🎯 CrushMapper round-robin mapped {actor_name} to node: {selected_node}")
        return [selected_node]

    def _map_multi_replica(
        self,
        actor_name: str,
        replica_count: int,
        nodes_with_replicas: set,
        available_nodes: List[str]
    ) -> List[str]:
        """Распределение multi-replica акторов"""
        actor_hash = int(hashlib.md5(actor_name.encode()).hexdigest()[:8], 16)
        placement = []

        # Сначала добавляем ноды которые уже имеют реплики (если нужно сохранить их)
        for node in list(nodes_with_replicas):
            if len(placement) < replica_count and node in available_nodes:
                placement.append(node)
                available_nodes.remove(node)
                log.debug(f"🔁 CrushMapper keeping existing replica {actor_name} on node: {node}")

        # Добавляем новые ноды если нужно больше реплик
        while len(placement) < replica_count and available_nodes:
            selected_node = self._weighted_selection(available_nodes, actor_hash + len(placement))
            if selected_node:
                placement.append(selected_node)
                available_nodes.remove(selected_node)
                log.debug(f"➕ CrushMapper adding new replica {actor_name} on node: {selected_node}")
            else:
                break

        # Если все еще не хватает реплик - пытаемся использовать уже занятые ноды
        if len(placement) < replica_count:
            all_occupied_nodes = list(nodes_with_replicas) + placement
            unique_occupied_nodes = list(set(all_occupied_nodes))

            for node in unique_occupied_nodes:
                if len(placement) < replica_count and node not in placement:
                    placement.append(node)
                    log.debug(f"🔄 CrushMapper reusing node {node} for {actor_name}")

        log.info(
            f"🎯 CrushMapper mapped {actor_name} to nodes: {placement} "
            f"(requested: {replica_count}, available: {list(self.nodes.keys())})"
        )
        return placement

    def _weighted_selection(self, available_nodes: List[str], seed: int) -> Optional[str]:
        """Взвешенный выбор ноды на основе весов"""
        if not available_nodes:
            return None

        # Создаем взвешенный список
        weighted_nodes = []
        for node in available_nodes:
            weight = self.nodes.get(node, 1.0)
            # Добавляем ноду несколько раз в зависимости от веса
            count = max(1, int(weight * 10))
            weighted_nodes.extend([node] * count)

        if not weighted_nodes:
            return None

        # Детерминированный выбор на основе seed
        random.seed(seed)
        selected = random.choice(weighted_nodes)
        random.seed()  # Сбрасываем seed

        log.debug(
            f"🎲 Weighted selection: {selected} from {available_nodes} "
            f"(weights: {[self.nodes.get(n, 1.0) for n in available_nodes]})"
        )
        return selected

    def get_optimal_node_for_actor(self, actor_name: str) -> Optional[str]:
        """Возвращает оптимальную ноду для нового актора"""
        if not self.nodes:
            return None

        available_nodes = list(self.nodes.keys())
        if not available_nodes:
            return None

        # Выбираем ноду с максимальным весом (наименее загруженную)
        best_node = max(available_nodes, key=lambda n: self.nodes.get(n, 1.0))
        log.debug(
            f"🏆 Optimal node for {actor_name}: {best_node} "
            f"(weight: {self.nodes.get(best_node, 1.0)})"
        )
        return best_node

    def get_node_load(self, node_id: str) -> float:
        """Возвращает текущую нагрузку ноды (обратный вес)"""
        weight = self.nodes.get(node_id, 1.0)
        return 1.0 / weight if weight > 0 else float('inf')

    def print_node_weights(self):
        """Логирует текущие веса нод (для отладки)"""
        if not self.nodes:
            log.info("📊 No nodes available in CrushMapper")
            return

        log.info("📊 CrushMapper node weights:")
        for node_id, weight in sorted(self.nodes.items(), key=lambda x: x[1], reverse=True):
            load = 1.0 / weight if weight > 0 else float('inf')
            log.info(f"   {node_id}: weight={weight:.2f}, load={load:.2f}")


class ClusterActor(Actor):
    def __init__(self):
        super().__init__()

        self.config: Optional[ActioConfig] = None
        self.crush_mapper = CrushMapper()
        self.tasks = 0
        self.server = None
        self.conn: Dict[str, asyncio.StreamWriter] = {}
        self.members: Dict[Optional[str], Dict] = {}
        self.goss_tgt: Set[str] = set()
        self._cluster_initialized = False
        self._is_leader = False
        self._election_task = None
        self._orchestration_task = None
        self._last_leader_announcement = 0
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
                "actor_count": 1  # Этот актор
            }
        })

        log.info(f"ClusterActor configured for node: {self.config.node_id}")

    async def cluster_started(self) -> None:
        """Вызывается после инициализации контекста"""
        if not self.config:
            log.error("ClusterActor started without configuration!")
            return

        log.info(f"ClusterActor started for node: {self.config.node_id}")

        # Регистрируем себя в реестре
        actor_name = self.actor_ref.name.split('-')[0]
        registry._register_replica(actor_name, self.config.node_id, self.actor_ref)
        log.debug(f"Registered ClusterActor in registry: {self.actor_ref}")

        # Запускаем кластер
        if not self._cluster_initialized:
            await self.cluster_init()

    async def cluster_init(self) -> None:
        """Инициализация кластера"""
        if self._cluster_initialized:
            return

        log.warning(f"Starting cluster node: {self.config.node_id}")

        self.server = await asyncio.start_server(
            self._conn_hdl, self.config.node_ip, self.config.cluster_port
        )

        log.warning(f"Cluster server started on port {self.config.cluster_port}")
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
                return await self._try_cluster_routing(forward_message, sender)

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
        """Маршрутизация на конкретную ноду в формате node:node_id/path"""
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
            log.warning(f"Cluster not ready, cannot forward to {node_id}")
            return False

        log.info(f"📡 Forwarding to node {node_id}: {message.get('destination', 'no destination')}")

        if node_id == self.config.node_id:
            log.info("🎯 Message is for local node, processing locally")
            # Доставляем сообщение локально
            self._context.letterbox.put_nowait((sender, message))
            return True

        connection_id = self._find_connection_for_node(node_id)
        if connection_id:
            try:
                await self._send_msg(connection_id, message)
                log.info(f"✅ Successfully forwarded to {node_id}")
                return True
            except Exception as e:
                log.error(f"❌ Failed to send to {node_id}: {e}")
                return False
        else:
            log.warning(f"🚫 No connection to node {node_id}")
            for conn_id in self.conn.keys():
                if node_id in conn_id or conn_id in node_id:
                    try:
                        await self._send_msg(conn_id, message)
                        log.info(f"✅ Successfully forwarded via alternative connection {conn_id}")
                        return True
                    except Exception as e:
                        log.error(f"❌ Failed to send via {conn_id}: {e}")

            log.error(f"💥 No available connections to node {node_id}")
            return False

    def _find_connection_for_node(self, node_id: str) -> Optional[str]:
        """Находит соединение для указанной ноды"""
        if node_id in self.conn:
            return node_id

        for conn_id in self.conn.keys():
            # Если node_id это "api2", а conn_id это "api2:7946" - считаем что подходит
            if node_id in conn_id or conn_id in node_id:
                return conn_id

        if node_id in self.members:
            member_address = self.members[node_id].get('address', '')
            if member_address:
                # member_address в формате "api2:7946"
                for conn_id in self.conn.keys():
                    if member_address == conn_id or conn_id in member_address:
                        return conn_id

        log.debug(f"🔍 No connection found for node {node_id}, available: {list(self.conn.keys())}")
        return None

    async def _leader_election_loop(self):
        """Цикл выборов лидера с мониторингом изменений в кластере"""
        last_member_state = None

        while True:
            try:
                await self._run_leader_election()
                current_member_state = {
                    node_id: member.get("status", "alive")
                    for node_id, member in self.members.items()
                }

                # Если лидер и состояние кластера изменилось
                if (
                    self._is_leader
                    and current_member_state != last_member_state
                    and self._orchestration_done
                ):
                    # Фильтруем только значимые изменения (появление/исчезновение живых нод)
                    alive_nodes_now = {k for k, v in current_member_state.items() if v == "alive"}
                    alive_nodes_before = {k for k, v in (last_member_state or {}).items() if v == "alive"}

                    if alive_nodes_now != alive_nodes_before:
                        log.info(
                            f"🔄 Cluster membership changed! "
                            f"Alive nodes: {len(alive_nodes_now)} (was {len(alive_nodes_before)}). "
                            f"Re-orchestrating..."
                        )

                        self._orchestration_done = False
                        if self._orchestration_task and not self._orchestration_task.done():
                            self._orchestration_task.cancel()

                        self._orchestration_task = asyncio.create_task(self._orchestrate_all_actors())

                last_member_state = current_member_state
                await asyncio.sleep(10)  # Проверяем каждые 10 секунд

            except asyncio.CancelledError:
                log.info("Leader election loop cancelled")
                break
            except Exception as e:
                log.error(f"❌ Error in leader election loop: {e}")
                await asyncio.sleep(30)  # Подождем подольше при ошибках

    async def _run_leader_election(self):
        """Выборы лидера - самая маленькая нода становится лидером"""
        if not self.members:
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
            await self._announce_leadership()
            # Лидер запускает оркестрацию ВСЕХ static акторов
            if self._orchestration_task:
                self._orchestration_task.cancel()
            self._orchestration_task = asyncio.create_task(self._orchestrate_all_actors())
        elif was_leader and not self._is_leader:
            log.info(f"❌ This node is no longer the leader: {self.config.node_id}")
            if self._orchestration_task:
                self._orchestration_task.cancel()
                self._orchestration_task = None

    async def _orchestrate_all_actors(self):
        """Оркестрирует ВСЕ static акторы с учетом parent-child отношений"""
        if not self._is_leader:
            return

        await asyncio.sleep(5)
        log.info("🔄 Leader starting orchestration of all static actors...")

        # 🔥 ШАГ 1: Cleanup мертвых реплик (существующая логика)
        cleanup_count = 0
        for actor_name in list(registry._actor_replicas.keys()):
            for node_id in list(registry._actor_replicas[actor_name].keys()):
                if (
                    node_id in self.members
                    and self.members[node_id].get("status") == "dead"
                ):
                    del registry._actor_replicas[actor_name][node_id]
                    cleanup_count += 1
                    log.info(f"🧹 Cleaned up dead replica {actor_name} from node {node_id}")

        if cleanup_count > 0:
            log.info(f"✅ Cleaned up {cleanup_count} dead replicas")

        actors_to_orchestrate = registry.get_actors_for_orchestration()

        if not actors_to_orchestrate:
            log.info("✅ No actors to orchestrate")
            self._orchestration_done = True
            return

        log.info(f"🎯 Orchestrating {len(actors_to_orchestrate)} actors: {[a.name for a in actors_to_orchestrate]}")

        self.crush_mapper.update_nodes(self.members)

        # 🔥 ШАГ 2: УМНОЕ РАСПРЕДЕЛЕНИЕ С УЧЕТОМ PARENT
        placement = {}

        for defn in actors_to_orchestrate:
            if defn.replicas == 'all':
                target_nodes = list(self.crush_mapper.nodes.keys())
            else:
                target_nodes = self._get_target_nodes_for_actor(defn)

            for replica_index, node_id in enumerate(target_nodes):
                if node_id not in placement:
                    placement[node_id] = []
                placement[node_id].append((defn.name, replica_index))

        # 🔥 ШАГ 3: Рассылаем команды создания
        commands_sent = 0
        for node_id, actor_assignments in placement.items():
            if (
                node_id in self.members
                and self.members[node_id].get("status") == "alive"
            ):
                for actor_name, replica_index in actor_assignments:
                    current_replicas = registry.get_actor_replicas(actor_name)

                    if node_id not in current_replicas:
                        success = await self._send_create_command(node_id, actor_name, replica_index)
                        if success:
                            commands_sent += 1
                    else:
                        log.debug(f"✅ Replica {actor_name} already exists on alive node {node_id}")

        # 🔥 ШАГ 4: Финальная очистка (существующая логика)
        final_cleanup_count = 0
        for actor_name in [a.name for a in actors_to_orchestrate]:
            current_replicas = registry.get_actor_replicas(actor_name)
            expected_nodes = set()

            for node_id, assignments in placement.items():
                if node_id in self.members and self.members[node_id].get("status") == "alive":
                    for assigned_actor, _ in assignments:
                        if assigned_actor == actor_name:
                            expected_nodes.add(node_id)

            for node_id in list(current_replicas.keys()):
                if node_id not in expected_nodes:
                    del registry._actor_replicas[actor_name][node_id]
                    final_cleanup_count += 1
                    log.info(f"🧹 Removed orphaned replica {actor_name} from node {node_id}")

        if final_cleanup_count > 0:
            log.info(f"✅ Final cleanup: removed {final_cleanup_count} orphaned replicas")

        log.info(f"✅ Leader sent {commands_sent} create commands")
        self._orchestration_done = True

        log.info("📊 Final replica distribution:")
        for actor_name in [a.name for a in actors_to_orchestrate]:
            replicas = registry.get_actor_replicas(actor_name)
            log.info(f"   {actor_name}: {list(replicas.keys())}")

    def _get_target_nodes_for_actor(self, actor_def: ActorDefinition) -> List[str]:
        if actor_def.parent:
            parent_replicas = registry.get_actor_replicas(actor_def.parent)
            if not parent_replicas:
                log.warning(f"⚠️ Parent {actor_def.parent} not found for {actor_def.name}")
                return []

            # 🔥 ПРОВЕРЯЕМ КЛАСС РОДИТЕЛЯ ЧЕРЕЗ ОПРЕДЕЛЕНИЕ
            parent_is_cluster_actor = False

            for defn in registry._definitions.values():
                if defn.name == actor_def.parent:
                    if issubclass(defn.cls, ClusterActor):
                        parent_is_cluster_actor = True
                        log.info(f"🎯 Parent {actor_def.parent} is ClusterActor (class: {defn.cls.__name__})")
                    break

            # 🔥 ЕСЛИ РОДИТЕЛЬ ClusterActor - УЧИТЫВАЕМ replicas!
            if parent_is_cluster_actor:
                if actor_def.replicas == 'all':
                    target_nodes = list(self.crush_mapper.nodes.keys())
                    log.info(
                        f"🎯 Distributing {actor_def.name} to ALL nodes "
                        f"(parent is ClusterActor, replicas='all')"
                    )
                else:
                    # Используем CrushMapper но с учетом что родитель на всех нодах
                    target_nodes = self.crush_mapper.map_actor(actor_def.name, actor_def.replicas)
                    log.info(
                        f"🎯 Distributing {actor_def.name} to {target_nodes} "
                        f"(parent is ClusterActor, replicas={actor_def.replicas})"
                    )
                return target_nodes

            available_parent_nodes = [
                node_id for node_id in parent_replicas.keys()
                if node_id in self.crush_mapper.nodes
            ]

            if not available_parent_nodes:
                log.warning(f"⚠️ No alive parent nodes for {actor_def.name}")
                return []

            # Если родитель на всех нодах - распределяем как корневой
            if len(available_parent_nodes) == len(self.crush_mapper.nodes):
                return self.crush_mapper.map_actor(actor_def.name, actor_def.replicas)

            # Родитель на части нод - распределяем ТОЛЬКО по этим нодам
            else:
                original_nodes = self.crush_mapper.nodes
                try:
                    self.crush_mapper.nodes = {
                        node_id: weight
                        for node_id, weight in self.crush_mapper.nodes.items()
                        if node_id in available_parent_nodes
                    }

                    target_nodes = self.crush_mapper.map_actor(actor_def.name, actor_def.replicas)

                    log.info(
                        f"🎯 Hierarchical mapping: {actor_def.name} -> {target_nodes} "
                        f"(parent {actor_def.parent} on {available_parent_nodes})"
                    )
                    return target_nodes

                finally:
                    self.crush_mapper.nodes = original_nodes

        # Корневой актор - распределяем по всем нодам
        return self.crush_mapper.map_actor(actor_def.name, actor_def.replicas)

    def _find_node_for_actor_ref(self, actor_ref: ActorRef) -> Optional[str]:
        """Находит ноду для заданного ActorRef"""
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

        # 🔥 Ищем конечный актор в пути
        target_actor = path_parts[-1]
        target_ref = registry.get_any_replica(target_actor)

        if not target_ref:
            return None

        target_node = self._find_node_for_actor_ref(target_ref)
        if not target_node or target_node == self.config.node_id:
            return None  # Актор локальный или не найден

        # 🔥 Возвращаем полный путь с указанием ноды
        return f"node:{target_node}/{destination}"

    async def _send_create_command(self, node_id: str, actor_name: str, replica_index: int):
        """Отправляет команду создания актора на ноду"""
        log.info(f"🔍 Sending create command for {actor_name} to {node_id}")

        if node_id == self.config.node_id:
            # Создаем локально
            await self._create_local_actor(actor_name, replica_index)
            return True
        else:
            # Отправляем на удаленную ноду
            connection_id = self._find_connection_for_node(node_id)
            if connection_id:
                await self._send_msg(
                    connection_id,
                    {
                        "type": "replica_command",
                        "actor_name": actor_name,
                        "action": "create",
                        "replica_index": replica_index,
                        "from_leader": self.config.node_id
                    }
                )
                return True
            else:
                log.warning(f"🚫 No connection to node {node_id}")
                return False

    async def _create_local_actor(self, actor_name: str, replica_index: int):
        """Создает актор локально по команде оркестрации"""
        try:
            # Находим определение актора
            defn = None
            for d in registry._definitions.values():
                if d.name == actor_name:
                    defn = d
                    break

            if not defn:
                log.error(f"❌ Actor definition not found: {actor_name}")
                return

            actor_instance = defn.cls()
            replica_name = defn.name

            # Создаем актор
            ref = None
            if defn.parent:
                # Создаем как дочерний актор через текущий ClusterActor
                ref = self.create(actor_instance, name=replica_name)
                log.info(f"✅ Created child actor {replica_name} via ClusterActor.create() on {self.config.node_id}")
            else:
                # Корневой актор - создаем напрямую
                ref = self.system.create(actor_instance, name=replica_name)
                log.info(f"✅ Created root actor: {replica_name} on {self.config.node_id}")

            # Регистрируем реплику
            registry._register_replica(defn.name, self.config.node_id, ref)

            # 🔥 Уведомляем кластер о новой реплике
            await self._broadcast_replica_update(defn.name, "add", ref)

            # Обновляем счетчик акторов
            if self.config.node_id in self.members:
                self.members[self.config.node_id]["actor_count"] = \
                    self.members[self.config.node_id].get("actor_count", 0) + 1
                self.crush_mapper.update_nodes(self.members)

        except Exception as e:
            log.error(f"❌ Failed to create local actor {actor_name}: {e}")
            import traceback
            log.error(traceback.format_exc())

    async def _broadcast_replica_update(self, actor_name: str, action: str, actor_ref: ActorRef = None):
        """Рассылает обновление о реплике всем нодам кластера"""
        if not self._cluster_initialized:
            return

        message = {
            "type": "replica_update",
            "actor_name": actor_name,
            "node_id": self.config.node_id,
            "action": action,
            "timestamp": time.time()
        }

        if action == "add" and actor_ref:
            message["actor_ref"] = {
                "actor_id": actor_ref.actor_id,
                "path": actor_ref.path,
                "name": actor_ref.name
            }

        # Рассылаем всем подключенным нодам
        for node_id in self.conn:
            if node_id != self.config.node_id:  # Не отправляем себе
                try:
                    await self._send_msg(node_id, message)
                    log.info(f"📢 Broadcasted replica update for {actor_name} to {node_id}")
                except Exception as e:
                    log.error(f"❌ Failed to broadcast replica update to {node_id}: {e}")

    async def _process_replica_command(self, message: Dict[str, Any]):
        """Обрабатывает команды реплик от лидера"""
        actor_name = message["actor_name"]
        action = message["action"]
        replica_index = message.get("replica_index", 0)

        log.info(f"Processing replica command: {action} for {actor_name}")

        if action == "create":
            log.info(f"Creating local replica for {actor_name}")
            await self._create_local_actor(actor_name, replica_index)
        elif action == "remove":
            log.info(f"Removing local replica for {actor_name}")
            await self._stop_local_replica(actor_name)

    async def _stop_local_replica(self, actor_name: str):
        """Удаляет локальную реплику и уведомляет кластер"""
        replicas = registry.get_actor_replicas(actor_name)
        if self.config.node_id in replicas:
            actor_ref = replicas[self.config.node_id]
            self.system.stop(actor_ref)

            if actor_name in registry._actor_replicas and self.config.node_id in registry._actor_replicas[actor_name]:
                del registry._actor_replicas[actor_name][self.config.node_id]

            await self._broadcast_replica_update(actor_name, "remove")

            log.info(f"🗑️ Removed local replica {actor_name} from node {self.config.node_id}")

    async def _process_replica_update(self, message: Dict[str, Any]):
        """Обрабатывает обновления реплик от других нод"""
        actor_name = message["actor_name"]
        node_id = message["node_id"]
        action = message["action"]

        log.info(f"🔄 Processing replica update: {action} for {actor_name} from {node_id}")

        if action == "add":
            actor_ref_data = message["actor_ref"]
            actor_ref = ActorRef(
                actor_id=actor_ref_data["actor_id"],
                path=actor_ref_data["path"],
                name=actor_ref_data["name"]
            )
            registry._register_replica(actor_name, node_id, actor_ref)
            log.info(f"✅ Registered remote replica {actor_name} from node {node_id}: {actor_ref}")

        elif action == "remove":
            if actor_name in registry._actor_replicas and node_id in registry._actor_replicas[actor_name]:
                del registry._actor_replicas[actor_name][node_id]
                log.info(f"🗑️ Removed remote replica {actor_name} from node {node_id}")

        # Обновляем маппер
        self.crush_mapper.update_nodes(self.members)

        # Логируем текущее состояние
        replicas = registry.get_actor_replicas(actor_name)
        log.info(f"📊 Current replicas for {actor_name}: {list(replicas.keys())}")

    # Остальные сетевые методы остаются без изменений
    async def _goss_loop(self):
        while True:
            if self.goss_tgt:
                tgt = random.choice(list(self.goss_tgt))
                if tgt in self.conn:
                    await self._send_msg(
                        tgt,
                        {
                            "type": "gossip",
                            "node_id": self.config.node_id,
                            "members": self.members,
                            "incarnation": self.members[self.config.node_id]["incarnation"],
                        },
                    )
            await asyncio.sleep(1)

    async def _failure_detect(self):
        """Обнаружение сбоев с очисткой ресурсов и автоматическим re-orchestration"""
        while True:
            now = time.time()
            cluster_changed = False
            dead_nodes_detected = []
            suspect_nodes_detected = []
            recovered_nodes_detected = []

            for node_id, member in list(self.members.items()):
                if node_id == self.config.node_id:
                    continue

                old_status = member.get("status", "alive")

                if now - member["last_seen"] > self.config.failure_timeout:
                    member["status"] = "dead"

                elif now - member["last_seen"] > self.config.failure_timeout / 2:
                    member["status"] = "suspect"

                elif member["status"] != "alive":
                    member["status"] = "alive"

                if member["status"] != old_status:
                    cluster_changed = True

                    if member["status"] == "dead":
                        dead_nodes_detected.append(node_id)
                        log.warning(f"🚨 Node {node_id} marked as DEAD (was {old_status})")

                        if node_id in self.conn:
                            writer = self.conn.pop(node_id)
                            try:
                                writer.close()
                                await writer.wait_closed()
                                log.debug(f"🧹 Closed connection to dead node: {node_id}")
                            except Exception as e:
                                log.debug(f"🔧 Error closing connection to {node_id}: {e}")

                    elif member["status"] == "suspect":
                        suspect_nodes_detected.append(node_id)
                        log.warning(f"⚠️  Node {node_id} is SUSPECT (was {old_status})")

                    elif member["status"] == "alive":
                        recovered_nodes_detected.append(node_id)
                        log.info(f"✅ Node {node_id} recovered to ALIVE (was {old_status})")

                    self.crush_mapper.update_nodes(self.members)

            if (
                self._is_leader
                and cluster_changed
                and dead_nodes_detected
                and self._orchestration_done
            ):
                log.info(
                    f"🔄 Cluster topology changed! "
                    f"Dead nodes: {dead_nodes_detected}, "
                    f"Suspect: {suspect_nodes_detected}, "
                    f"Recovered: {recovered_nodes_detected}. "
                    f"Re-orchestrating..."
                )

                self._orchestration_done = False

                # Отменяем предыдущую задачу оркестрации если есть
                if self._orchestration_task and not self._orchestration_task.done():
                    self._orchestration_task.cancel()

                # Запускаем новую оркестрацию
                self._orchestration_task = asyncio.create_task(self._orchestrate_all_actors())

            elif cluster_changed:
                log.debug(
                    f"📊 Cluster status changed - "
                    f"Dead: {dead_nodes_detected}, "
                    f"Suspect: {suspect_nodes_detected}, "
                    f"Recovered: {recovered_nodes_detected}"
                )

            await asyncio.sleep(2)  # Проверяем каждые 2 секунды

    async def _nodes_conn(self):
        for node in self.config.cluster_nodes:
            node_host, node_port = node.split(":")
            if node_host != self.config.node_id:
                await self._node_conn(host=node_host, port=int(node_port))

    async def _node_conn(self, host: str, port: int, max_retries: int = 3) -> bool:
        """Пытается подключиться к ноде с обработкой ошибок и exponential backoff"""
        for attempt in range(max_retries):
            try:
                reader, writer = await asyncio.open_connection(host, port)
                node_id = f"{host}:{port}"
                self.conn[node_id] = writer

                asyncio.create_task(self._node_lstn(reader=reader, node_id=node_id))

                await self._send_msg(
                    node_id,
                    {
                        "type": "node_join",
                        "node_id": self.config.node_id,
                        "port": self.config.cluster_port,
                    },
                )

                log.info(f"✅ Connected to {node_id}")
                return True

            except (ConnectionRefusedError, socket.gaierror) as e:
                error_type = "DNS resolution failed" if isinstance(e, socket.gaierror) else "Connection refused"

                if attempt < max_retries - 1:
                    backoff_delay = 2 ** attempt  # Exponential backoff: 1, 2, 4 seconds
                    log.debug(
                        f"🔄 Connection attempt {attempt + 1}/{max_retries} "
                        f"to {host}:{port} failed: {error_type}. "
                        f"Retrying in {backoff_delay}s")
                    await asyncio.sleep(backoff_delay)
                else:
                    log.debug(f"🚫 Node {host}:{port} not available after {max_retries} attempts: {error_type}")
                    return False

            except asyncio.CancelledError:
                log.debug(f"🔌 Connection attempt to {host}:{port} cancelled")
                return False

            except Exception as e:
                log.debug(f"⚠️ Unexpected error connecting to {host}:{port}: {e}")
                return False

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
            log.info(f"Lost connection to cluster node: {node_id}")
            self.conn.pop(node_id, None)
        except Exception as e:
            log.error(f"Error listening to {node_id}: {e}")
            self.conn.pop(node_id, None)

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
            log.info(f"Lost connection to cluster node: {node_id}")
            self.conn.pop(node_id, None)
        except Exception as e:
            log.error(f"Error listening to {node_id}: {e}")
            self.conn.pop(node_id, None)

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
        msg_type = message.get("type")

        if msg_type != "heartbeat":
            log.info(f"Received cluster message from {sender_node}: {msg_type}")

        if msg_type == "node_join":
            node_id = message["node_id"]
            self.goss_tgt.add(node_id)
            self.members[node_id] = {
                "status": "alive",
                "last_seen": time.time(),
                "incarnation": 0,
                "address": f"{node_id}:{message['port']}",
                "resources": {},
                "actor_count": 1
            }
            log.info(f"Node {message['node_id']} joined the cluster")
            self.crush_mapper.update_nodes(self.members)

            # 🔥 Синхронизируем наши реплики с новой нодой
            await self._sync_replicas_with_node(node_id)

        elif msg_type == "replica_update":
            await self._process_replica_update(message)

        elif msg_type == "replica_command":
            await self._process_replica_command(message)

        elif msg_type == "leader_announcement":
            await self._process_leader_announcement(message)

        elif msg_type == "gossip":
            await self._merge_member(message["members"], message["incarnation"])
            self.crush_mapper.update_nodes(self.members)

        elif msg_type == "heartbeat":
            if message["node_id"] in self.members:
                self.members[message["node_id"]]["last_seen"] = time.time()
        else:
            if message.get('action') == 'route_message':
                self._context.letterbox.put_nowait((self.actor_ref, message))
                log.debug(f"Injected route_message into letterbox for {self.actor_ref.path}")

    async def _sync_replicas_with_node(self, node_id: str):
        """Синхронизирует информацию о репликах с новой нодой"""
        log.info(f"🔄 Syncing replicas with new node {node_id}")

        for actor_name, replicas in registry._actor_replicas.items():
            for replica_node_id, actor_ref in replicas.items():
                if replica_node_id == self.config.node_id:  # Только наши реплики
                    await self._send_msg(node_id, {
                        "type": "replica_update",
                        "actor_name": actor_name,
                        "node_id": self.config.node_id,
                        "action": "add",
                        "actor_ref": {
                            "actor_id": actor_ref.actor_id,
                            "path": actor_ref.path,
                            "name": actor_ref.name
                        },
                        "timestamp": time.time()
                    })
                    log.debug(f"📤 Synced replica {actor_name} to {node_id}")

    async def _process_leader_announcement(self, message: Dict[str, Any]):
        """Обрабатывает анонс лидерства от другой ноды"""
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
            log.warning(f"Node {announced_leader} incorrectly announced leadership, true leader is {true_leader}")
            return

        if announced_leader != self.config.node_id and self._is_leader:
            log.info(f"Accepting {announced_leader} as true leader, stepping down")
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
                        "type": "heartbeat",
                        "node_id": self.config.node_id,
                        "timestamp": time.time(),
                    },
                )
            await asyncio.sleep(3)

    async def _announce_leadership(self):
        """Анонсирует свое лидерство кластеру"""
        self._last_leader_announcement = time.time()
        for node_id in self.conn:
            await self._send_msg(
                node_id,
                {
                    "type": "leader_announcement",
                    "leader_id": self.config.node_id,
                    "timestamp": time.time()
                }
            )
        log.info(f"🎯 Leader {self.config.node_id} announced leadership to cluster")

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
                log.info(f"Discovered new node via gossip: {node_id}")
            else:
                local_info = self.members[node_id]
                if remote_info["incarnation"] > local_info["incarnation"]:
                    self.members[node_id] = remote_info
                elif (
                    remote_info["incarnation"] == local_info["incarnation"] and
                    remote_info["last_seen"] > local_info["last_seen"]
                ):
                    self.members[node_id]["last_seen"] = remote_info["last_seen"]
                    self.members[node_id]["status"] = remote_info["status"]

    async def _background_connector(self):
        """Умный коннектор, который избегает подключений к мертвым и проблемным нодам"""
        connection_attempts = {}  # node_id -> last_attempt_time

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

                    log.debug(f"🔗 Attempting connection to {node_id}")
                    success = await self._node_conn(node_host, int(node_port))
                    connection_attempts[node_id] = current_time

                    if not success:
                        # Помечаем ноду как проблемную для временного игнорирования
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
                    log.debug(f"🔧 Background connector error for {node}: {e}")
                    # Не прерываем цикл из-за ошибки в одной ноде

            await asyncio.sleep(15)  # 1 минута вместо 30 секунд

    async def receive(self, sender: ActorRef, message: Any) -> None:
        log.info(f"🔍 ClusterActor.receive called: {type(message).__name__} from {sender}")

        if isinstance(message, dict) and message.get("action") == "actio_broadcast":
            for node_id in self.conn:
                await self._send_msg(node_id, message["payload"])

        if isinstance(message, Terminated):
            self.tasks -= 1
            log.info(f"{message.actor.name} has stopped task")
            if self.tasks == 0:
                self.system.shutdown()

    async def stopped(self) -> None:
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
