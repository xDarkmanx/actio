# actio/cluster.py
# -*- coding: utf-8 -*

import logging
import asyncio
import json
import time
import random

from typing import Any
from typing import Dict
from typing import Set
from typing import Optional

from actio import Terminated

from . import Actor
from . import ActorRef

from . import ActioConfig

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

    def _calculate_weight(self, member_data):
        return 1.0

    def map_actor(self, actor_name, replicas=1):
        if not self.nodes:
            return []

        available_nodes = list(self.nodes.keys())
        return available_nodes[: min(replicas, len(available_nodes))]


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

    def set_config(self, config: ActioConfig):
        """Устанавливаем конфигурацию извне"""
        self.config = config

        self.crush_mapper.update_nodes({
            config.node_id: {
                "status": "alive",
                "last_seen": time.time(),
                "weight": config.node_weight,
                "resources": config.resources
            }
        })

        log.info(f"ActioSystem configured for node: {self.config.node_id}")

    async def cluster_init(self) -> None:
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
        }

        """ Connect to other node """
        await self._nodes_conn()

        asyncio.create_task(self._goss_loop())
        asyncio.create_task(self._failure_detect())
        asyncio.create_task(self._heartbeat())
        asyncio.create_task(self._background_connector())

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
                            "incarnation": self.members[self.config.node_id][
                                "incarnation"
                            ],
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
        """ """
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

                log.info(f"Connected to {node_id}")
                return True

            except ConnectionRefusedError:
                if attempt < max_retries - 1:
                    await asyncio.sleep(2)
                else:
                    log.debug(f"Node {host}:{port} not available")
                    return False

    async def _node_lstn(self, reader: asyncio.StreamReader, node_id: str):
        """ """
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

    async def _conn_hdl(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
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
            }

            log.info(f"Node {message['node_id']} joined the cluster")

            self.crush_mapper.update_nodes(self.members)

        elif msg_type == "gossip":
            # Merge membership information
            await self._merge_member(message["members"], message["incarnation"])

            self.crush_mapper.update_nodes(self.members)

        elif msg_type == "heartbeat":
            # Обновляем время последней активности
            if message["node_id"] in self.members:
                self.members[message["node_id"]]["last_seen"] = time.time()

    async def _heartbeat(self):
        """Отправка heartbeat всем подключенным нодам"""
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

    async def _merge_member(
        self, remote_members: Dict[str, Dict], remote_incarnation: int
    ):
        """Merge remote membership information with our state"""
        for node_id, remote_info in remote_members.items():
            if node_id == self.config.node_id:
                # Конфликт incarnation - выбираем выше
                if remote_incarnation > self.members[node_id]["incarnation"]:
                    self.members[node_id] = remote_info
                    self.members[node_id]["incarnation"] = remote_incarnation
                continue

            if node_id not in self.members:
                # Новая нода
                self.members[node_id] = remote_info
                self.goss_tgt.add(node_id)
                log.info(f"Discovered new node via gossip: {node_id}")
            else:
                # Выбираем более свежую информацию
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
        """Фоновая задача для подключения к недоступным нодам"""
        while True:
            for node in self.config.cluster_nodes:
                node_host, node_port = node.split(":")
                node_id = f"{node_host}:{node_port}"

                if (
                    node_host != self.config.node_id
                    and node_id not in self.conn
                    and node_id not in self.members.get("dead", [])
                ):
                    await self._node_conn(node_host, int(node_port))

            await asyncio.sleep(30)  # проверяем каждые 30 секунд

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
