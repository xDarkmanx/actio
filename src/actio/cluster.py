# actio/cluster.py
# -*- coding: utf-8 -*

import logging
import asyncio
import json
import time
import random
import hashlib

from typing import Any
from typing import Dict
from typing import Set
from typing import Optional
from typing import List

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
        self.nodes = {
            node_id: self._calculate_weight(member_data)
            for node_id, member_data in cluster_members.items()
            if member_data.get("status") == "alive"
        }
        log.debug(f"CrushMapper updated nodes: {list(self.nodes.keys())}")

    def _calculate_weight(self, member_data):
        """Рассчитывает вес ноды на основе ресурсов"""
        cpu_cores = member_data.get('resources', {}).get('cpu_cores', 4)
        memory_gb = member_data.get('resources', {}).get('memory_gb', 8)
        current_load = member_data.get('actor_count', 0)

        base_weight = (cpu_cores * 0.6 + memory_gb * 0.4)
        current_weight = base_weight / (current_load + 1)

        return max(current_weight, 0.1)

    def map_actor(self, actor_name: str, replicas: int = 1) -> List[str]:
        """Распределяет реплики актора по нодам с учетом существующих реплик"""
        if not self.nodes:
            return []

        # Получаем текущие реплики
        current_replicas = registry.get_actor_replicas(actor_name)

        # Ноды которые уже имеют реплику
        nodes_with_replicas = set(current_replicas.keys())

        # Все доступные ноды
        available_nodes = list(self.nodes.keys())

        if not available_nodes:
            return []

        actor_hash = int(hashlib.md5(actor_name.encode()).hexdigest()[:8], 16)
        placement = []

        # Сначала добавляем ноды которые уже имеют реплики (если нужно сохранить их)
        for node in list(nodes_with_replicas):
            if len(placement) < replicas and node in available_nodes:
                placement.append(node)
                available_nodes.remove(node)

        # Добавляем новые ноды если нужно больше реплик
        while len(placement) < replicas and available_nodes:
            # Используем взвешенный выбор на основе весов нод
            selected_node = self._weighted_selection(available_nodes, actor_hash + len(placement))
            if selected_node:
                placement.append(selected_node)
                available_nodes.remove(selected_node)
            else:
                break

        log.info(f"CrushMapper mapped {actor_name} to nodes: {placement} (from available: {list(self.nodes.keys())})")
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

        return selected


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
        registry._register_replica(self.__class__.__name__, self.config.node_id, self.actor_ref)
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

    async def _leader_election_loop(self):
        """Цикл выборов лидера"""
        while True:
            try:
                await self._run_leader_election()
                await asyncio.sleep(10)  # Проверяем каждые 10 секунд
            except Exception as e:
                log.error(f"Error in leader election: {e}")
                await asyncio.sleep(30)

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

        # Сортируем ноды по ID (самый маленький становится лидером)
        alive_nodes.sort()
        new_leader = alive_nodes[0]

        was_leader = self._is_leader
        self._is_leader = (new_leader == self.config.node_id)

        if self._is_leader and not was_leader:
            log.info(f"🎯 This node is now the cluster leader: {self.config.node_id}")
            # Анонсируем лидерство
            await self._announce_leadership()
            # Лидер запускает оркестрацию реплик
            if self._orchestration_task:
                self._orchestration_task.cancel()
            self._orchestration_task = asyncio.create_task(self._orchestrate_replicas_loop())
        elif was_leader and not self._is_leader:
            log.info(f"❌ This node is no longer the leader: {self.config.node_id}")
            if self._orchestration_task:
                self._orchestration_task.cancel()
                self._orchestration_task = None

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

    async def _orchestrate_replicas_loop(self):
        """Цикл оркестрации реплик (только для лидера)"""
        # Ждем 5 секунд чтобы убедиться что лидерство стабильно
        await asyncio.sleep(5)

        while self._is_leader:
            try:
                await self._orchestrate_replicas()
                await asyncio.sleep(15)  # Оркестрируем каждые 15 секунд
            except Exception as e:
                log.error(f"Error in replica orchestration: {e}")
                await asyncio.sleep(30)

    async def _orchestrate_replicas(self):
        """Лидер оркестрирует распределение реплик"""
        if not self._is_leader:
            return

        log.debug("🔄 Cluster leader orchestrating replica distribution...")

        for defn in registry._definitions.values():
            if not defn.dynamic and defn.replicas > 1:
                await self._orchestrate_static_actor_replicas(defn)

    async def _orchestrate_static_actor_replicas(self, defn: ActorDefinition):
        """Лидер распределяет реплики статического актора"""
        alive_nodes = [
            node_id for node_id, member in self.members.items()
            if member.get("status") == "alive"
        ]

        if not alive_nodes:
            log.warning(f"No alive nodes for orchestrating {defn.name}")
            return

        # Обновляем веса нод перед распределением
        self.crush_mapper.update_nodes(self.members)

        # Получаем ТЕКУЩИЕ реплики перед распределением
        current_replicas = registry.get_actor_replicas(defn.name)

        # Используем CrushMapper для распределения (учитывает существующие реплики)
        target_nodes = self.crush_mapper.map_actor(defn.name, defn.replicas)

        log.info(f"🎯 Orchestrating {defn.name}: target nodes {target_nodes}, current replicas {list(current_replicas.keys())}")
        log.info(f"📡 Available connections: {list(self.conn.keys())}")

        # Отправляем команды создания реплик на целевые ноды
        commands_sent = 0
        for node_id in target_nodes:
            has_replica = node_id in current_replicas

            if not has_replica:
                # Команда создать реплику
                log.info(f"🔄 Sending CREATE command for {defn.name} to node {node_id}")
                success = await self._send_replica_command(node_id, defn.name, "create")
                if success:
                    commands_sent += 1
                    log.info(f"✅ CREATE command sent successfully to {node_id}")
                else:
                    log.error(f"❌ Failed to send CREATE command to {node_id}")

        if commands_sent > 0:
            log.info(f"Sent {commands_sent} create commands for {defn.name}")
        else:
            log.info(f"No create commands needed for {defn.name} - all target nodes have replicas")

    def _find_connection_for_node(self, node_id: str) -> Optional[str]:
        """Находит connection_id для node_id"""
        # Прямое совпадение
        if node_id in self.conn:
            return node_id

        # Ищем по имени ноды в соединениях
        for conn_id in self.conn.keys():
            if node_id in conn_id:
                return conn_id

        # Ищем по адресу в members
        if node_id in self.members:
            member_address = self.members[node_id].get('address', '')
            for conn_id in self.conn.keys():
                if member_address and conn_id in member_address:
                    return conn_id

        return None

    async def _send_replica_command(self, node_id: str, actor_name: str, action: str):
        """Отправляет команду реплики ноде"""
        log.info(f"🔍 Looking for connection to {node_id}")

        # Находим правильный connection_id
        connection_id = self._find_connection_for_node(node_id)

        if connection_id and connection_id != self.config.node_id:
            log.info(f"📤 Sending {action} command for {actor_name} to {node_id} via {connection_id}")
            await self._send_msg(
                connection_id,
                {
                    "type": "replica_command",
                    "actor_name": actor_name,
                    "action": action,
                    "from_leader": self.config.node_id
                }
            )
            return True
        elif node_id == self.config.node_id:
            # Обрабатываем команду для себя напрямую
            log.info(f"⚡ Processing {action} command locally for {actor_name}")
            await self._process_replica_command({
                "actor_name": actor_name,
                "action": action
            })
            return True
        else:
            log.warning(f"🚫 Cannot send command to {node_id} - no connection found")
            log.warning(f"   Available connections: {list(self.conn.keys())}")
            log.warning(f"   Available members: {list(self.members.keys())}")
            return False

    async def ensure_replicas(self):
        """Гарантирует нужное количество реплик (только для не-лидеров)"""
        if self._is_leader:
            return  # Лидер сам оркестрирует реплики

        for defn in registry._definitions.values():
            if not defn.dynamic and defn.replicas > 1:
                # Не-лидеры только проверяют статус
                current_count = len(registry.get_actor_replicas(defn.name))
                if current_count != defn.replicas:
                    log.debug(f"Static actor {defn.name}: {current_count}/{defn.replicas} replicas (waiting for leader)")

    async def _create_local_replica(self, defn: ActorDefinition):
        """Создает локальную реплику статического актора"""
        try:
            actor_instance = defn.cls()

            # Используем node_id в имени реплики для уникальности
            replica_name = f"{defn.name}-{self.config.node_id}"
            ref = self.system.create(actor_instance, name=replica_name)

            # Регистрируем реплику локально
            registry._register_replica(defn.name, self.config.node_id, ref)

            # Синхронизируем с кластером
            await self._sync_replicas_to_cluster(defn.name, self.config.node_id, ref)

            log.info(f"✅ Created local replica: {replica_name}")

            # Обновляем счетчик акторов
            if self.config.node_id in self.members:
                self.members[self.config.node_id]["actor_count"] = \
                    self.members[self.config.node_id].get("actor_count", 0) + 1
                self.crush_mapper.update_nodes(self.members)

        except Exception as e:
            log.error(f"❌ Failed to create replica for {defn.name}: {e}")

    async def _stop_local_replica(self, actor_name: str):
        """Удаляет локальную реплику и уведомляет кластер"""
        replicas = registry.get_actor_replicas(actor_name)
        if self.config.node_id in replicas:
            actor_ref = replicas[self.config.node_id]

            # Останавливаем актор
            self.system.stop(actor_ref)

            # Удаляем из registry
            if actor_name in registry._actor_replicas and self.config.node_id in registry._actor_replicas[actor_name]:
                del registry._actor_replicas[actor_name][self.config.node_id]

            # Уведомляем кластер
            await self._sync_replica_removal(actor_name, self.config.node_id)

            log.info(f"Removed local replica {actor_name} from node {self.config.node_id}")

    async def _sync_replicas_to_cluster(self, actor_name: str, node_id: str, actor_ref: ActorRef):
        """Синхронизирует информацию о репликах с кластером"""
        for target_node in self.conn:
            await self._send_msg(
                target_node,
                {
                    "type": "replica_update",
                    "actor_name": actor_name,
                    "node_id": node_id,
                    "actor_ref": {
                        "actor_id": actor_ref.actor_id,
                        "path": actor_ref.path,
                        "name": actor_ref.name
                    },
                    "action": "add"
                }
            )

    async def _sync_replica_removal(self, actor_name: str, node_id: str):
        """Синхронизирует удаление реплики с кластером"""
        for target_node in self.conn:
            await self._send_msg(
                target_node,
                {
                    "type": "replica_update",
                    "actor_name": actor_name,
                    "node_id": node_id,
                    "action": "remove"
                }
            )

    async def _replica_monitor_loop(self):
        """Мониторинг и поддержание реплик статических акторов"""
        while True:
            try:
                await self.ensure_replicas()
                await asyncio.sleep(10)
            except Exception as e:
                log.error(f"Error in replica monitoring: {e}")
                await asyncio.sleep(30)

    # Сетевые методы
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
        while True:
            now = time.time()
            for node_id, member in list(self.members.items()):
                if node_id == self.config.node_id:
                    continue

                if now - member["last_seen"] > 10:
                    if member["status"] != "dead":
                        member["status"] = "dead"
                        log.warning(f"Node {node_id} marked as dead")
                        self.crush_mapper.update_nodes(self.members)

                elif now - member["last_seen"] > 5:
                    if member["status"] != "suspect":
                        member["status"] = "suspect"
                        log.warning(f"Node {node_id} is suspect")
                        self.crush_mapper.update_nodes(self.members)

            await asyncio.sleep(2)

    async def _nodes_conn(self):
        for node in self.config.cluster_nodes:
            node_host, node_port = node.split(":")
            if node_host != self.config.node_id:
                await self._node_conn(host=node_host, port=int(node_port))

    async def _node_conn(self, host: str, port: int, max_retries: int = 5):
        for attempt in range(max_retries):
            try:
                reader, writer = await asyncio.open_connection(host, port)
                # Используем host:port как node_id
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

                log.info(f"Connected to {node_id}")
                return True

            except ConnectionRefusedError:
                if attempt < max_retries - 1:
                    await asyncio.sleep(2)
                else:
                    log.debug(f"Node {host}:{port} not available")
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
        # Используем IP:port как идентификатор
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

    async def _process_replica_update(self, message: Dict[str, Any]):
        """Обрабатывает обновления реплик от других нод"""
        actor_name = message["actor_name"]
        node_id = message["node_id"]
        action = message["action"]

        if action == "add":
            actor_ref_data = message["actor_ref"]
            # Создаем ActorRef из данных
            actor_ref = ActorRef(
                actor_id=actor_ref_data["actor_id"],
                path=actor_ref_data["path"],
                name=actor_ref_data["name"]
            )
            registry._register_replica(actor_name, node_id, actor_ref)
            log.info(f"Registered remote replica {actor_name} from node {node_id}")

        elif action == "remove":
            if actor_name in registry._actor_replicas and node_id in registry._actor_replicas[actor_name]:
                del registry._actor_replicas[actor_name][node_id]
                log.info(f"Removed remote replica {actor_name} from node {node_id}")

        self.crush_mapper.update_nodes(self.members)

    async def _process_replica_command(self, message: Dict[str, Any]):
        """Обрабатывает команды реплик от лидера"""
        actor_name = message["actor_name"]
        action = message["action"]

        log.info(f"Processing replica command: {action} for {actor_name}")

        # Находим определение актора
        defn = None
        for d in registry._definitions.values():
            if d.name == actor_name:
                defn = d
                break

        if not defn:
            log.error(f"Unknown actor definition: {actor_name}")
            return

        if action == "create":
            log.info(f"Creating local replica for {actor_name}")
            await self._create_local_replica(defn)
        elif action == "remove":
            log.info(f"Removing local replica for {actor_name}")
            await self._stop_local_replica(actor_name)

    async def _process_leader_announcement(self, message: Dict[str, Any]):
        """Обрабатывает анонс лидерства от другой ноды"""
        announced_leader = message["leader_id"]
        announcement_time = message["timestamp"]

        # Если объявленный лидер "меньше" текущего лидера, принимаем его
        alive_nodes = [
            node_id for node_id, member in self.members.items()
            if member.get("status") == "alive"
        ]

        if not alive_nodes:
            return

        alive_nodes.sort()
        true_leader = alive_nodes[0]

        # Сравниваем объявленного лидера с истинным лидером
        if announced_leader != true_leader:
            log.warning(f"Node {announced_leader} incorrectly announced leadership, true leader is {true_leader}")
            return

        # Если это не мы и объявленный лидер корректен, снимаем свое лидерство
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
        while True:
            for node in self.config.cluster_nodes:
                node_host, node_port = node.split(":")
                node_id = f"{node_host}:{node_port}"

                if (node_host != self.config.node_id and
                    node_id not in self.conn and
                    node_id not in self.members.get("dead", [])):
                    await self._node_conn(node_host, int(node_port))

            await asyncio.sleep(30)

    async def receive(self, sender: ActorRef, message: Any) -> None:
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
