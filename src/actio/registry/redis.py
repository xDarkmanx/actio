# src/actio/registry/redis.py
# -*- coding: utf-8 -*-
"""
Redis-based registry implementation for cluster-ready actor system.

Architecture:
- All communication via Redis (Pub/Sub + Keys)
- No direct node-to-node connections
- Host/Port stored only for monitoring/admin purposes
- Cold-start philosophy: no state restoration in registry
- Leader election via SETNX + TTL
- Weight-based actor placement via CRUSH
"""

import asyncio
import json
import logging
import os
import time
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from redis import asyncio as aioredis

from ..protocols import RegistryProtocol
from ..ref import ActorDefinition, ActorRef
from ..system import ActorSystem

log = logging.getLogger("actio.registry.redis")

# =============================================================================
# Key prefixes for Redis namespace isolation
# =============================================================================
KEY_PREFIX = "actio"
NODES_KEY = f"{KEY_PREFIX}:nodes"
ACTORS_KEY = f"{KEY_PREFIX}:actors"
DEFS_KEY = f"{KEY_PREFIX}:defs"
MSG_CHANNEL = f"{KEY_PREFIX}:msg"
CLUSTER_KEY = f"{KEY_PREFIX}:cluster"
LEADER_KEY = f"{KEY_PREFIX}:leader:current"

# =============================================================================
# TTL constants (seconds) - Variant 2 (balanced)
# =============================================================================
# Leader election
LEADER_HEARTBEAT_INTERVAL = 5    # Leader renews TTL every 5s
LEADER_TTL = 15                  # Leader TTL (3 missed heartbeats = death)
WORKER_CHECK_INTERVAL = 3        # Workers check leader every 3s

# Node registration
NODE_HEARTBEAT_INTERVAL = 10     # Nodes renew TTL every 10s
NODE_TTL = 30                    # Node TTL (3 missed heartbeats = dead)

# Cluster orchestration
QUORUM_TIMEOUT = 60              # Max time to wait for quorum
STABILIZATION_DELAY = 5          # Wait after quorum before orchestration

# Commands & acknowledgments
ACK_TTL = 60                     # TTL for command acknowledgments
COMMAND_TTL = 60                 # TTL for cluster commands


class RedisRegistry(RegistryProtocol):
    """
    Redis-based actor registry for standalone and cluster modes.

    Features:
    - Node discovery via heartbeat + TTL
    - Actor registration and lookup
    - Pub/Sub message delivery (local and cross-node)
    - Leader election via SETNX + TTL
    - Weight-based actor placement via CRUSH
    - Wave-based orchestration with parent-child constraints
    - Automatic recovery from node failures
    """

    def __init__(
        self,
        redis_url: str,
        node_id: str,
        node_weight: float = 1.0,
    ):
        """
        Initialize RedisRegistry.

        Args:
            redis_url: Redis connection URL (e.g., "redis://localhost:6379")
            node_id: Unique identifier for this node in the cluster
            node_weight: Weight of this node for CRUSH placement (default 1.0)
        """
        self.redis_url = redis_url
        self.node_id = node_id
        self.node_weight = node_weight

        self.redis: Optional[aioredis.Redis] = None
        self.pubsub: Optional[aioredis.client.PubSub] = None
        self._system: Optional[ActorSystem] = None
        self._definitions: Dict[str, ActorDefinition] = {}
        self._actor_refs: Dict[str, ActorRef] = {}

        # Background tasks
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._msg_listener_task: Optional[asyncio.Task] = None
        self._cmd_listener_task: Optional[asyncio.Task] = None
        self._leader_heartbeat_task: Optional[asyncio.Task] = None
        self._check_leader_task: Optional[asyncio.Task] = None
        self._node_death_monitor_task: Optional[asyncio.Task] = None
        self._periodic_cleanup_task: Optional[asyncio.Task] = None

        # State
        self._closed = False
        self._node_registered = False
        self._is_leader = False  # 🎯 Leader flag
        self._node_loads: Dict[str, float] = {}  # Track load for balanced placement

    # ==========================================================================
    # Connection management
    # ==========================================================================

    async def connect(self, system: Optional[ActorSystem] = None) -> None:
        """Establish connection to Redis and start background tasks."""
        if self.redis is not None:
            return

        if system:
            self._system = system

        log.info(f"Connecting to Redis at {self.redis_url} (node: {self.node_id})")
        self.redis = await aioredis.from_url(self.redis_url)

        # Test connection
        await self.redis.ping()
        log.info("Redis connection established")

        # Register this node in Redis
        # TODO: Add _check_node_resurrection() to detect when a dead node comes back online
        await self._register_node()
        self._node_registered = True

        # 🎯 Try to become leader (first come, first served via SETNX)
        await self._try_become_leader()

        # Start background tasks
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._msg_listener_task = asyncio.create_task(self._msg_listener_loop())
        self._cmd_listener_task = asyncio.create_task(self._cmd_listener_loop())

        # Leader-only: monitor for node deaths + periodic cleanup
        if self._is_leader:
            self._node_death_monitor_task = asyncio.create_task(self._node_death_monitor_loop())
            # 🎯 NEW: Start periodic cleanup task
            self._periodic_cleanup_task = asyncio.create_task(self._periodic_cleanup_loop())

        log.info(f"RedisRegistry connected for node {self.node_id} (leader: {self._is_leader})")

    async def disconnect(self) -> None:
        """Close Redis connection and stop background tasks."""
        if self._closed:
            return

        self._closed = True
        log.info(f"Disconnecting RedisRegistry for node {self.node_id}")

        # Stop all background tasks
        for task in [
            self._heartbeat_task,
            self._msg_listener_task,
            self._cmd_listener_task,
            self._leader_heartbeat_task,
            self._check_leader_task,
            self._node_death_monitor_task,
            self._periodic_cleanup_task,
        ]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Unregister node (best effort)
        if self._node_registered and self.redis:
            try:
                await self.redis.delete(f"{NODES_KEY}:{self.node_id}")
                await self.redis.srem(f"{NODES_KEY}:all", self.node_id)
                # If we were leader, remove leader key
                if self._is_leader:
                    await self.redis.delete(LEADER_KEY)
                log.info(f"Node {self.node_id} unregistered from Redis")
            except Exception as e:
                log.warning(f"Failed to unregister node {self.node_id}: {e}")

        # Close Redis connection
        if self.redis:
            await self.redis.close()
            self.redis = None

        log.info("RedisRegistry disconnected")

    # ==========================================================================
    # Node registration & heartbeat
    # ==========================================================================

    async def _register_node(self) -> None:
        """Register this node in Redis with heartbeat TTL."""
        if not self.redis:
            raise RuntimeError("Redis not connected")

        # 1. Remove from global nodes set
        await self.redis.srem(f"{NODES_KEY}:all", self.node_id)

        # 2. Delete node metadata
        await self.redis.delete(f"{NODES_KEY}:{self.node_id}")

        # 3. Delete node's actor list
        await self.redis.delete(f"{NODES_KEY}:{self.node_id}:actors")

        # 4. Clean up acknowledgments
        await self.redis.delete(f"{CLUSTER_KEY}:ack:{self.node_id}")

        # 5. 🔥 KEY STEP: Remove this node from ALL actor registrations
        # Scan all actor keys and remove this node_id from each set
        async for key in self.redis.scan_iter(f"{ACTORS_KEY}:*"):
            # Skip meta keys (actio:actors:meta:*)
            if b":meta:" not in key:
                await self.redis.srem(key, self.node_id)

        # Get host/port from environment (for monitoring only)
        host = os.getenv("HOST", "localhost")
        port = os.getenv("PORT", "5000")

        node_data = {
            "node_id": self.node_id,
            "host": host,
            "port": port,
            "registered_at": str(time.time()),
            "weight": str(self.node_weight),
            "load": str(self._node_loads.get(self.node_id, 0.0)),
            "status": "alive",
        }

        # Set node metadata with TTL
        await self.redis.hset(f"{NODES_KEY}:{self.node_id}", mapping=node_data)
        await self.redis.expire(f"{NODES_KEY}:{self.node_id}", NODE_TTL)

        # Add to set of all nodes
        await self.redis.sadd(f"{NODES_KEY}:all", self.node_id)

        log.debug(f"Node {self.node_id} registered in Redis (weight: {self.node_weight})")

    async def _heartbeat_loop(self) -> None:
        """Background task: renew node registration TTL."""
        while not self._closed and self.redis:
            try:
                await self._renew_node_ttl()
                await asyncio.sleep(NODE_HEARTBEAT_INTERVAL)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Heartbeat error for node {self.node_id}: {e}")
                await asyncio.sleep(NODE_HEARTBEAT_INTERVAL)

    # ==========================================================================
    # Leader election (first come, first served via SETNX)
    # ==========================================================================

    async def _renew_node_ttl(self) -> None:
        """Renew node registration TTL only (no cleanup).

        Called by heartbeat loop — must NOT touch actor registrations!
        """
        if not self.redis:
            return

        # Just renew the TTL for node metadata
        await self.redis.expire(f"{NODES_KEY}:{self.node_id}", NODE_TTL)
        # That's it — no scanning, no SREM, no cleanup!

    async def _try_become_leader(self) -> bool:
        """
        Try to become leader using atomic SETNX.

        Returns True if this node became leader, False otherwise.
        """
        if not self.redis:
            return False

        # 🎯 Atomic SETNX: only one node can succeed
        success = await self.redis.set(
            LEADER_KEY,
            self.node_id,
            nx=True,      # SET if Not eXists
            ex=LEADER_TTL  # TTL in seconds
        )

        if success:
            self._is_leader = True
            log.info(f"🎯 This node is now the cluster leader: {self.node_id}")

            # Start leader-specific tasks
            self._leader_heartbeat_task = asyncio.create_task(self._leader_heartbeat_loop())
            self._node_death_monitor_task = asyncio.create_task(self._node_death_monitor_loop())

            # Wait for quorum and stability, then orchestrate
            asyncio.create_task(self._wait_for_quorum_and_stability())

            return True
        else:
            self._is_leader = False
            log.info(f"❌ Leader already exists, this node is worker: {self.node_id}")

            # Start worker-specific task: check if leader is still alive
            self._check_leader_task = asyncio.create_task(self._check_leader_loop())

            return False

    async def _leader_heartbeat_loop(self) -> None:
        """Leader heartbeat: renew TTL every LEADER_HEARTBEAT_INTERVAL seconds."""
        while self._is_leader and not self._closed and self.redis:
            try:
                # Renew leader TTL
                await self.redis.expire(LEADER_KEY, LEADER_TTL)
                await asyncio.sleep(LEADER_HEARTBEAT_INTERVAL)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Leader heartbeat error: {e}")
                await asyncio.sleep(LEADER_HEARTBEAT_INTERVAL)

    async def _check_leader_loop(self) -> None:
        """Worker check: is leader still alive? Try to become leader if not."""
        while not self._is_leader and not self._closed and self.redis:
            try:
                leader = await self.redis.get(LEADER_KEY)

                if leader is None:
                    # Leader key missing → try to become leader
                    log.info("Leader key missing, attempting to become leader")
                    await self._try_become_leader()
                    # If we became leader, the loop will exit (_is_leader = True)

                await asyncio.sleep(WORKER_CHECK_INTERVAL)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Leader check error: {e}")
                await asyncio.sleep(WORKER_CHECK_INTERVAL)

    async def _node_death_monitor_loop(self) -> None:
        """Leader-only: monitor for dead nodes and trigger recovery."""
        while self._is_leader and not self._closed and self.redis:
            try:
                await self._check_for_dead_nodes()
                await asyncio.sleep(LEADER_HEARTBEAT_INTERVAL)  # Check every 5s
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Node death monitor error: {e}")
                await asyncio.sleep(LEADER_HEARTBEAT_INTERVAL)

    async def _check_for_dead_nodes(self) -> None:
        """Check for nodes that have died and trigger redistribution."""
        if not self._is_leader or not self.redis:
            return

        all_nodes = await self.redis.smembers(f"{NODES_KEY}:all")

        for node_bytes in all_nodes:
            node_id = node_bytes.decode() if isinstance(node_bytes, bytes) else node_bytes

            # Check if node TTL has expired
            ttl = await self.redis.ttl(f"{NODES_KEY}:{node_id}")
            if ttl <= 0 and node_id != self.node_id:
                log.warning(f"🪦 Node {node_id} appears dead (TTL expired), triggering recovery")
                await self._handle_node_death(node_id)

    async def _handle_node_death(self, dead_node_id: str) -> None:
        """
        Handle node death: redistribute actors from dead node.

        Called by leader when a node TTL expires.
        """
        if not self._is_leader:
            return

        log.warning(f"🪦 Node {dead_node_id} died, redistributing actors...")

        # 1. Which actors were on the dead node?
        actors_on_dead = await self.redis.smembers(f"{NODES_KEY}:{dead_node_id}:actors")

        if not actors_on_dead:
            log.info(f"No actors on dead node {dead_node_id}")
            await self._cleanup_dead_node(dead_node_id)
            return

        # 2. Get available nodes (excluding dead one)
        available_nodes = await self.get_alive_nodes()
        available_nodes = [n for n in available_nodes if n != dead_node_id]

        if not available_nodes:
            log.error("No available nodes to redistribute actors!")
            await self._cleanup_dead_node(dead_node_id)
            return

        # 3. Load node weights and current loads
        node_weights = await self._load_node_weights()
        node_loads = await self._load_node_loads()

        # 4. For each actor — recalculate placement
        for actor_bytes in actors_on_dead:
            actor_name = actor_bytes.decode() if isinstance(actor_bytes, bytes) else actor_bytes

            # Get actor metadata (replicas, weight, parent)
            meta = await self.redis.hgetall(f"{ACTORS_KEY}:meta:{actor_name}")
            if not meta:
                continue

            replicas_raw = meta.get(b"replicas", b"1")
            replicas = 'all' if replicas_raw == b'all' else int(replicas_raw)
            weight = float(meta.get(b"weight", b"0.01"))
            parent = meta.get(b"parent", b"").decode() or None

            # Get where parent currently lives (if any)
            parent_nodes = None
            if parent:
                parent_nodes_raw = await self.redis.smembers(f"{ACTORS_KEY}:{parent}")
                parent_nodes = [
                    n.decode() if isinstance(n, bytes) else n
                    for n in parent_nodes_raw
                ]

            # Recalculate placement with capacity awareness
            target_nodes = await self._calculate_placement(
                actor_name=actor_name,
                replicas=replicas,
                actor_weight=weight,
                parent_nodes=parent_nodes,
                available_nodes=available_nodes,
                node_weights=node_weights,
                node_loads=node_loads
            )

            # Send commands to create on new nodes
            for target_node in target_nodes:
                if target_node != dead_node_id:
                    await self._send_command(target_node, {
                        "action": "create_actor",
                        "actor_name": actor_name,
                        "parent": parent,
                        "reason": "node_death",
                        "dead_node": dead_node_id,
                        "replicas": replicas,
                        "weight": weight
                    })

            # Update Redis: remove from dead node, add to new nodes
            await self.redis.srem(f"{ACTORS_KEY}:{actor_name}", dead_node_id)
            await self.redis.srem(f"{NODES_KEY}:{dead_node_id}:actors", actor_name)

            for target_node in target_nodes:
                if target_node != dead_node_id:
                    await self.redis.sadd(f"{ACTORS_KEY}:{actor_name}", target_node)
                    await self.redis.sadd(f"{NODES_KEY}:{target_node}:actors", actor_name)
                    # Update load tracking
                    node_loads[target_node] = node_loads.get(target_node, 0.0) + weight
                    await self._update_node_load(target_node, node_loads[target_node])

            log.info(f"Redistributed {actor_name} from {dead_node_id} to {target_nodes}")

        # 5. Cleanup dead node
        await self._cleanup_dead_node(dead_node_id)
        log.info(f"✅ Node {dead_node_id} recovery completed")

    async def _cleanup_dead_node(self, node_id: str) -> None:
        """Remove all traces of a dead node from Redis."""
        if not self.redis:
            return

        # Remove node from all nodes set
        await self.redis.srem(f"{NODES_KEY}:all", node_id)

        # Remove node metadata
        await self.redis.delete(f"{NODES_KEY}:{node_id}")

        # Remove actors list for this node
        await self.redis.delete(f"{NODES_KEY}:{node_id}:actors")

        # Remove from leader key if it was leader
        current_leader = await self.redis.get(LEADER_KEY)
        if current_leader and current_leader.decode() == node_id:
            await self.redis.delete(LEADER_KEY)

        log.debug(f"Cleaned up dead node {node_id} from Redis")

    async def _load_node_weights(self) -> Dict[str, float]:
        """Load node weights from Redis."""
        weights = {}
        nodes = await self.get_available_nodes()

        for node_id in nodes:
            node_data = await self.redis.hgetall(f"{NODES_KEY}:{node_id}")
            weight = float(node_data.get(b"weight", b"1.0"))
            weights[node_id] = weight

        return weights

    async def _load_node_loads(self) -> Dict[str, float]:
        """Load current node loads from Redis."""
        loads = {}
        nodes = await self.get_available_nodes()

        for node_id in nodes:
            node_data = await self.redis.hgetall(f"{NODES_KEY}:{node_id}")
            load = float(node_data.get(b"load", b"0.0"))
            loads[node_id] = load

        return loads

    async def _update_node_load(self, node_id: str, new_load: float) -> None:
        """Update node load in Redis."""
        if not self.redis:
            return

        await self.redis.hset(f"{NODES_KEY}:{node_id}", "load", str(new_load))
        self._node_loads[node_id] = new_load
        log.debug(f"Node {node_id} load updated to {new_load}")

    async def _calculate_placement(
        self,
        actor_name: str,
        replicas: Union[int, str],
        actor_weight: float,
        parent_nodes: Optional[List[str]] = None,
        available_nodes: Optional[List[str]] = None,
        node_weights: Optional[Dict[str, float]] = None,
        node_loads: Optional[Dict[str, float]] = None  # ← ← ← Может быть tmp_loads из волны
    ) -> List[str]:
        if available_nodes is None:
            available_nodes = await self.get_alive_nodes()

        if node_weights is None:
            node_weights = await self._load_node_weights()

        # 🎯 Если node_loads не переданы — загружаем из Redis, иначе используем переданные
        if node_loads is None:
            node_loads = await self._load_node_loads()
        # Если переданы — используем их (например, tmp_loads из волны)

        # Если replicas='all', place on all valid nodes
        if replicas == 'all':
            if parent_nodes:
                return [n for n in available_nodes if n in parent_nodes]
            return available_nodes

        # Limit to parent nodes if specified (children constraint)
        candidate_nodes = available_nodes
        if parent_nodes:
            candidate_nodes = [n for n in available_nodes if n in parent_nodes]
            if not candidate_nodes:
                return []

        # Calculate available capacity for each node
        nodes_with_capacity = []
        for node_id in candidate_nodes:
            weight = node_weights.get(node_id, 1.0)
            load = node_loads.get(node_id, 0.0)  # ← ← ← Используем переданные нагрузки!
            available_capacity = max(0.0, weight - load - actor_weight)
            nodes_with_capacity.append((node_id, available_capacity))

        # Use CRUSH mapper with capacity awareness
        from ..crush import map_actor_with_capacity

        actual_replicas = min(int(replicas), len(nodes_with_capacity))
        target_nodes = map_actor_with_capacity(
            actor_name=actor_name,
            nodes_with_capacity=nodes_with_capacity,
            replicas=actual_replicas
        )

        return target_nodes

    # ==========================================================================
    # Orchestration (leader only)
    # ==========================================================================

    async def _wait_for_quorum_and_stability(self) -> None:
        """
        Wait for quorum and topology stabilization before orchestration.
        Only called by leader.
        """
        if not self._is_leader:
            return

        log.info("🎯 Leader waiting for quorum and stabilization...")
        start_time = time.time()

        WARMUP_PERIOD = 5.0
        log.info(f"⏳ Warm-up: waiting {WARMUP_PERIOD}s for nodes to register...")
        await asyncio.sleep(WARMUP_PERIOD)

        # Wait for quorum
        while time.time() - start_time < QUORUM_TIMEOUT:
            alive_nodes = await self.get_alive_nodes()
            quorum_size = self._get_quorum_size()

            if len(alive_nodes) >= quorum_size:
                log.info(f"✅ Quorum reached: {len(alive_nodes)}/{quorum_size} nodes alive")
                break

            log.debug(f"⏳ Waiting for quorum: {len(alive_nodes)}/{quorum_size} nodes alive")
            await asyncio.sleep(2)
        else:
            log.warning(f"⚠️ Quorum timeout: only {len(await self.get_alive_nodes())} nodes alive")
            # Continue anyway for small clusters

        # Wait for topology stabilization
        log.info(f"⏳ Waiting {STABILIZATION_DELAY}s for topology stabilization...")
        await asyncio.sleep(STABILIZATION_DELAY)

        log.info("🛑 Sending STOP_ALL to all nodes...")
        await self._send_stop_all_to_all_nodes()

        log.info("✅ Leader ready for orchestration")
        await self._orchestrate_all_actors()

    async def _send_stop_all_to_all_nodes(self) -> None:
        """Send STOP_ALL command to all alive nodes and wait for ACKs."""
        alive_nodes = await self.get_alive_nodes()
        wave_id = f"stop_all_{int(time.time())}"

        log.info(f"🛑 Sending STOP_ALL to {len(alive_nodes)} nodes (wave_id: {wave_id})")

        # Send command to all nodes (including self)
        for node_id in alive_nodes:
            await self._send_command(node_id, {
                "action": "STOP_ALL",
                "wave_id": wave_id
            })

        # Wait for acknowledgments (timeout: 10s)
        ack_timeout = 10.0
        start_time = time.time()
        received_acks = set()

        while time.time() - start_time < ack_timeout:
            # Check ACKs in Redis
            for node_id in alive_nodes:
                ack_key = f"{CLUSTER_KEY}:ack:{node_id}"
                ack_data = await self.redis.get(ack_key)

                if ack_data:
                    ack = json.loads(ack_data) if isinstance(ack_data, str) else json.loads(ack_data.decode())
                    if ack.get("wave_id") == wave_id and ack.get("status") == "STOPPED":
                        received_acks.add(node_id)

            if len(received_acks) >= len(alive_nodes):
                log.info(f"✅ All nodes acknowledged STOP_ALL: {received_acks}")
                break

            log.debug(f"⏳ Waiting for STOP_ALL ACKs: {len(received_acks)}/{len(alive_nodes)}")
            await asyncio.sleep(0.5)
        else:
            missing = set(alive_nodes) - received_acks
            log.warning(f"⚠️ STOP_ALL timeout: missing ACKs from {missing}")
            # Continue anyway - dead nodes will be cleaned up by TTL

    def _get_quorum_size(self) -> int:
        """Calculate quorum size based on expected cluster size."""
        # For now, use simple majority
        # In production, read from config: self.config.cluster_nodes
        return 2  # Minimum for 2-node cluster

    async def _orchestrate_all_actors(self) -> None:
        """Orchestrate all static actors in waves."""
        if not self._is_leader:
            return

        # Reset load tracking for fresh orchestration
        node_weights = await self._load_node_weights()

        # 🎯 НОВЫЙ ПОДХОД: tmp_loads для балансировки ВНУТРИ волны
        # Начинаем с базовой нагрузки (ActioSystem и другие уже размещённые)
        tmp_loads: Dict[str, float] = await self._load_node_loads()

        # Build generation waves (parents before children)
        generations = self._build_generation_waves()

        # Track where actors are placed (for children constraint)
        actor_placement: Dict[str, List[str]] = {}

        for gen_idx, gen_actors in enumerate(generations):
            wave_id = f"wave_{gen_idx}_{int(time.time())}"
            log.info(f"🌊 Starting wave {gen_idx}: {[a.name for a in gen_actors]}")

            for actor_def in gen_actors:
                # Get parent placement (for children constraint)
                parent_nodes = None
                if actor_def.parent:
                    parent_nodes = actor_placement.get(actor_def.parent)

                # 🎯 КЛЮЧЕВОЙ МОМЕНТ: передаём tmp_loads для учёта уже размещённых в этой волне
                target_nodes = await self._calculate_placement(
                    actor_name=actor_def.name,
                    replicas=actor_def.replicas,
                    actor_weight=actor_def.weight,
                    parent_nodes=parent_nodes,
                    node_weights=node_weights,
                    node_loads=tmp_loads  # ← ← ← Используем tmp_loads, а не загруженные из Redis!
                )

                if not target_nodes:
                    log.warning(f"⚠️ Could not place {actor_def.name}: no valid nodes")
                    continue

                # Send create commands to target nodes
                for node_id in target_nodes:
                    if node_id == self.node_id:
                        await self._create_actor_locally(actor_def)
                    else:
                        await self._send_command(node_id, {
                            "action": "create_actor",
                            "actor_name": actor_def.name,
                            "parent": actor_def.parent,
                            "wave_id": wave_id,
                            "replicas": actor_def.replicas,
                            "weight": actor_def.weight
                        })

                # Track placement for children
                actor_placement[actor_def.name] = target_nodes

                # 🎯 КЛЮЧЕВОЙ МОМЕНТ: обновляем tmp_loads СРАЗУ для следующего акторм в волне!
                for node_id in target_nodes:
                    tmp_loads[node_id] = tmp_loads.get(node_id, 0.0) + actor_def.weight
                    log.debug(f"📊 tmp_loads[{node_id}] updated to {tmp_loads[node_id]}")

                log.info(f"✅ Placed {actor_def.name} on {target_nodes}")

            # Wait for wave completion
            await asyncio.sleep(2)
            log.info(f"✅ Wave {gen_idx} completed")

        # 🎯 После завершения волны — синхронизируем tmp_loads с Redis
        for node_id, load in tmp_loads.items():
            await self._update_node_load(node_id, load)

        log.info("🎉 Cluster orchestration completed")

    async def _cleanup_stale_registrations(self) -> None:
        """
        Clean up stale actor registrations from Redis.

        Rules:
        - Remove registrations from nodes that are not alive
        - Remove registrations with invalid node_id (like "api" default)
        - Ensure actors with fixed replicas have exactly N registrations
        - Keep only registrations that match CRUSH placement
        """
        if not self.redis or not self._is_leader:
            return

        log.info("🧹 Starting stale registration cleanup...")

        # Get list of valid/alive nodes
        alive_nodes = set(await self.get_alive_nodes())
        valid_nodes = {n for n in alive_nodes if n and n != "api"}  # Filter out default "api"

        # Get all actor definitions
        actor_names = await self.redis.smembers(f"{DEFS_KEY}:all")

        for actor_bytes in actor_names:
            actor_name = actor_bytes.decode() if isinstance(actor_bytes, bytes) else actor_bytes

            # Skip dynamic actors (they're created on-demand)
            defn = self._definitions.get(actor_name)
            if not defn or defn.dynamic:
                continue

            actor_key = f"{ACTORS_KEY}:{actor_name}"
            current_nodes_raw = await self.redis.smembers(actor_key)
            current_nodes = {
                n.decode() if isinstance(n, bytes) else n
                for n in current_nodes_raw
            }

            if not current_nodes:
                continue

            # 🎯 Шаг 1: Удалить регистрации с невалидными node_id
            invalid = {n for n in current_nodes if not n or n == "api" or n not in alive_nodes}
            for node_id in invalid:
                await self.redis.srem(actor_key, node_id)
                await self.redis.srem(f"{NODES_KEY}:{node_id}:actors", actor_name)
                log.debug(f"🧹 Removed invalid/stale: {actor_name} on {node_id}")

            # 🎯 Шаг 2: Если replicas фиксированный — оставить только нужное количество
            if defn.replicas != 'all' and isinstance(defn.replicas, int):
                expected = int(defn.replicas)
                remaining = sorted([n for n in current_nodes if n in valid_nodes])

                if len(remaining) > expected:
                    # Keep first N by sorted order (deterministic)
                    to_keep = set(remaining[:expected])
                    to_remove = [n for n in remaining if n not in to_keep]

                    for node_id in to_remove:
                        await self.redis.srem(actor_key, node_id)
                        await self.redis.srem(f"{NODES_KEY}:{node_id}:actors", actor_name)
                        log.debug(f"🧹 Removed excess: {actor_name} on {node_id}")

            # 🎯 Шаг 3: Если replicas='all' — убедиться что акторм на всех живых нодах
            elif defn.replicas == 'all':
                missing = valid_nodes - current_nodes
                for node_id in missing:
                    # Not a cleanup, but log for debugging
                    log.debug(f"⚠️ {actor_name} should be on {node_id} (replicas='all') but not found")

        log.info("✅ Stale registration cleanup completed")

    def _build_generation_waves(self) -> List[List[ActorDefinition]]:
        """
        Build waves of actors ordered by parent-child dependencies.

        Returns list of lists, where each inner list is a generation.
        """
        generations = []
        remaining = set(self._definitions.values())

        while remaining:
            current_gen = []

            for defn in list(remaining):
                # Root actors or actors whose parents are already placed
                if not defn.parent:
                    current_gen.append(defn)
                else:
                    # Check if parent is in any previous generation
                    parent_placed = any(
                        d.name == defn.parent
                        for gen in generations
                        for d in gen
                    )
                    if parent_placed:
                        current_gen.append(defn)

            if not current_gen:
                # Circular dependency or missing parent
                log.error(f"❌ Could not place actors: {[a.name for a in remaining]}")
                break

            generations.append(current_gen)
            remaining -= set(current_gen)

        return generations

    async def _create_actor_locally(self, actor_def: ActorDefinition) -> Optional[ActorRef]:
        """Create an actor locally (used by leader during orchestration)."""
        if not self._system:
            return None

        # Find parent ActorRef if not root
        parent_ref = None
        if actor_def.parent:
            parent_ref = self._system.get_actor_ref_by_name(actor_def.parent)

        # Create actor instance
        try:
            actor_instance = actor_def.cls()
        except Exception as e:
            log.error(f"Failed to instantiate {actor_def.name}: {e}")
            return None

        # Create in system
        try:
            actor_ref = self._system._create(
                actor=actor_instance,
                parent=parent_ref,
                name=actor_def.name
            )
            log.info(f"✅ Created actor: {actor_def.name} on node {self.node_id}")
        except Exception as e:
            log.error(f"Failed to create actor {actor_def.name}: {e}")
            return None

        # Register in registry
        await self.register(actor_def.name, actor_ref, node_id=self.node_id)

        return actor_ref

    # ==========================================================================
    # Command sending (leader → workers)
    # ==========================================================================

    async def _send_command(self, target_node: str, command: Dict[str, Any]) -> None:
        """
        Send command to a specific node via Redis Pub/Sub.

        Args:
            target_node: Node ID to send command to
            command: Command dict with 'action' and other params
        """
        if not self.redis:
            return

        cmd_channel = f"{CLUSTER_KEY}:cmd:{target_node}"

        await self.redis.publish(cmd_channel, json.dumps(command))
        log.debug(f"📤 Sent command to {target_node}: {command.get('action')}")

    # ==========================================================================
    # Pub/Sub message listener (for cross-node messages)
    # ==========================================================================

    async def _msg_listener_loop(self) -> None:
        """Background task: listen for messages from OTHER nodes."""
        if not self.redis:
            return

        self.pubsub = self.redis.pubsub()

        # 🎯 Subscribe ONLY to this node's inbox
        channel = f"{NODES_KEY}:{self.node_id}:inbox"
        await self.pubsub.subscribe(channel)

        log.debug(f"Subscribed to node inbox: {channel}")

        try:
            while not self._closed:
                message = await self.pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=1.0
                )

                if message and message["type"] == "message":
                    # 🎯 This message is definitely for this node — forward to ActioSystem
                    await self._forward_to_root_actor(message)

        except asyncio.CancelledError:
            pass
        finally:
            if self.pubsub:
                await self.pubsub.unsubscribe(channel)
                await self.pubsub.close()
                self.pubsub = None

    async def _forward_to_root_actor(self, message: Dict[str, Any]) -> None:
        """
        Forward Pub/Sub message to ActioSystem.

        CRITICAL: payload["data"] IS the original route_message — forward it directly!
        """
        if not self._system:
            log.warning("No system reference, cannot forward cluster message")
            return

        root_ref = self._system.get_actor_ref_by_name("ActioSystem")
        if not root_ref:
            log.warning("ActioSystem not found, cannot forward cluster message")
            return

        try:
            data = message["data"]
            if isinstance(data, bytes):
                data = data.decode()

            # Unwrap the envelope from send_message()
            wrapper = json.loads(data)

            # 🎯 payload["data"] IS the original route_message — extract it!
            original_message = wrapper.get("data", {})

            # If original_message has node: prefix in destination — strip it
            destination = original_message.get("destination", "")
            if destination.startswith("node:"):
                _, actor_path = destination.split("/", 1)
                original_message["destination"] = actor_path
                log.debug(f"Stripped node prefix: {destination} → {actor_path}")

            # ✅ Forward the ACTUAL message to ActioSystem
            log.debug(f"📥 Forwarding to ActioSystem: {original_message.get('action')} → {original_message.get('destination')}")
            self._system.tell(root_ref, original_message)

        except Exception as e:
            log.error(f"Error forwarding cluster message: {e}", exc_info=True)

    # ==========================================================================
    # Cluster command listener (for leader commands)
    # ==========================================================================

    async def _cmd_listener_loop(self) -> None:
        """Background task: listen for cluster commands from leader."""
        if not self.redis:
            return

        cmd_channel = f"{CLUSTER_KEY}:cmd:{self.node_id}"
        pubsub = self.redis.pubsub()

        await pubsub.subscribe(cmd_channel)
        log.debug(f"Subscribed to command channel: {cmd_channel}")

        try:
            while not self._closed:
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=1.0
                )

                if message and message["type"] == "message":
                    await self._handle_command(message["data"])

        except asyncio.CancelledError:
            pass
        finally:
            await pubsub.unsubscribe(cmd_channel)
            await pubsub.close()

    async def _handle_command(self, data: Any) -> None:
        """Process cluster command from leader (STOP_ALL, REBALANCE, create_actor, etc.)."""
        try:
            if isinstance(data, bytes):
                data = data.decode()

            cmd = json.loads(data) if isinstance(data, str) else data
            action = cmd.get("action")

            log.info(f"Received command for node {self.node_id}: {action}")

            if action == "STOP_ALL":
                await self._handle_stop_all(cmd)
            elif action == "REBALANCE":
                await self._handle_rebalance(cmd)
            elif action == "create_actor":
                await self._handle_create_actor(cmd)
            elif action == "ack":
                # Acknowledgment from another node (for wave completion)
                await self._handle_ack(cmd)

        except Exception as e:
            log.error(f"Error handling command: {e}", exc_info=True)

    async def _handle_create_actor(self, cmd: Dict[str, Any]) -> None:
        """Handle create_actor command from leader."""
        actor_name = cmd.get("actor_name")
        parent_name = cmd.get("parent")

        if not actor_name or actor_name not in self._definitions:
            log.warning(f"Unknown actor definition: {actor_name}")
            return

        # 🎯 КРИТИЧНО: Проверка что актор ещё не создан локально
        # Это защищает от дубликатов на ОДНОЙ ноде (реплики на разных нодах — ок)
        if actor_name in self._actor_refs:
            log.debug(f"⏭️ Actor {actor_name} already exists locally, skipping duplicate")
            wave_id = cmd.get("wave_id")
            if wave_id:
                await self._send_ack(wave_id, "already_exists", actor_name)
            return

        actor_def = self._definitions[actor_name]

        # Find parent if specified
        parent_ref = None
        if parent_name:
            parent_ref = self._system.get_actor_ref_by_name(parent_name) if self._system else None

        # Create actor locally
        try:
            actor_instance = actor_def.cls()
            if self._system:
                actor_ref = self._system._create(
                    actor=actor_instance,
                    parent=parent_ref,
                    name=actor_name
                )
                await self.register(actor_name, actor_ref, node_id=self.node_id)
                log.info(f"✅ Created {actor_name} on {self.node_id} (command from leader)")

                # Send ack back to leader
                wave_id = cmd.get("wave_id")
                if wave_id:
                    await self._send_ack(wave_id, "created", actor_name)

        except Exception as e:
            log.error(f"Failed to create {actor_name}: {e}", exc_info=True)

    async def _handle_ack(self, cmd: Dict[str, Any]) -> None:
        """Handle acknowledgment from another node."""
        # TODO: Track acks for wave completion
        wave_id = cmd.get("wave_id")
        status = cmd.get("status")
        node_id = cmd.get("node_id")
        log.debug(f"Received ack: wave={wave_id}, status={status}, node={node_id}")

    async def _handle_stop_all(self, cmd: Dict[str, Any]) -> None:
        """Handle STOP_ALL command: stop all local actors and send ACK."""
        if self._system:
            # Stop all actors gracefully
            await self._system.shutdown(timeout=5.0)

        # Send acknowledgment to leader
        wave_id = cmd.get("wave_id", "stop_all")
        await self._send_ack("STOPPED", wave_id=wave_id)

        log.info(f"✅ STOP_ALL completed on node {self.node_id}")

    async def _handle_rebalance(self, cmd: Dict[str, Any]) -> None:
        """Handle REBALANCE command: rebuild actor tree."""
        log.info(f"REBALANCE received on node {self.node_id}")

        if self._system:
            await self.build_actor_tree(self._system)

        await self._send_ack("REBALANCED")

    async def _send_ack(self, status: str, actor_name: Optional[str] = None, wave_id: Optional[str] = None) -> None:
        """Send acknowledgment for a command or wave."""
        if not self.redis:
            return

        ack_key = f"{CLUSTER_KEY}:ack:{self.node_id}"
        ack_data = {
            "node_id": self.node_id,
            "status": status,
            "actor_name": actor_name,
            "wave_id": wave_id,
            "timestamp": str(time.time()),
        }

        await self.redis.setex(ack_key, ACK_TTL, json.dumps(ack_data))
        log.debug(f"Sent ACK [{status}] for node {self.node_id}")

    # ==========================================================================
    # RegistryProtocol implementation
    # ==========================================================================

    async def register_definition(self, definition: ActorDefinition) -> None:
        """Register an actor definition (static actors from @actio decorator)."""
        if self._closed:
            return

        self._definitions[definition.name] = definition

        # Persist definition metadata in Redis (for cluster discovery)
        if self.redis:
            def_data = {
                "name": definition.name,
                "parent": definition.parent or "",
                "replicas": str(definition.replicas),
                "weight": str(definition.weight),
                "dynamic": str(definition.dynamic).lower(),
                "registered_at": str(time.time()),
            }
            await self.redis.hset(f"{DEFS_KEY}:{definition.name}", mapping=def_data)
            await self.redis.sadd(f"{DEFS_KEY}:all", definition.name)

        log.debug(f"Registered definition: {definition.name}")

    async def register(
        self,
        name: str,
        actor_ref: ActorRef,
        node_id: Optional[str] = None
    ) -> None:
        """Register a running actor instance."""
        if self._closed:
            return

        target_node = node_id or self.node_id

        # Store local reference
        self._actor_refs[name] = actor_ref

        if not self.redis:
            log.warning(f"⚠️ Redis not available, skipping registration for {name}")
            return

        try:
            key = f"{ACTORS_KEY}:{name}"

            # Add to actor->nodes mapping
            await self.redis.sadd(key, target_node)

            # Add to node->actors mapping
            node_actors_key = f"{NODES_KEY}:{target_node}:actors"
            await self.redis.sadd(node_actors_key, name)

            # Store metadata
            meta = {
                "name": name,
                "node_id": target_node,
                "registered_at": str(time.time()),
            }
            await self.redis.hset(f"{ACTORS_KEY}:meta:{name}", mapping=meta)

            log.info(f"✅ Registered actor {name} on node {target_node}")

        except Exception as e:
            log.error(f"❌ Failed to register {name} on {target_node}: {e}", exc_info=True)

    async def unregister(self, name: str, node_id: Optional[str] = None) -> None:
        """Unregister an actor instance."""
        target_node = node_id or self.node_id

        # Remove local reference
        self._actor_refs.pop(name, None)

        if self.redis:
            # Where does this actor live?
            await self.redis.srem(f"{ACTORS_KEY}:{name}", target_node)

            # 🎯 NEW: Remove from node's actor list
            await self.redis.srem(f"{NODES_KEY}:{target_node}:actors", name)

            # Clean up meta if no nodes left
            nodes = await self.redis.smembers(f"{ACTORS_KEY}:{name}")
            if not nodes:
                await self.redis.delete(f"{ACTORS_KEY}:meta:{name}")

        log.debug(f"Unregistered actor {name} from node {target_node}")

    async def build_actor_tree(
        self,
        system: ActorSystem,
        timeout: float = 10.0
    ) -> None:
        """Build actor tree for static actors (cluster-aware)."""
        self._system = system
        log.info(f"Building actor tree on node {self.node_id}")

        # Get available nodes for placement decisions
        available_nodes = await self.get_available_nodes()
        log.debug(f"Available nodes for placement: {available_nodes}")

        # Load node weights for CRUSH
        node_weights = {n: self.node_weight if n == self.node_id else 1.0 for n in available_nodes}

        # Get static actor definitions
        static_defs = [
            d for d in self._definitions.values()
            if not d.dynamic
        ]

        # Create root actors ONLY if assigned to this node
        for defn in static_defs:
            if defn.parent is None:
                # Cluster placement decision
                if len(available_nodes) > 1:
                    from ..crush import should_create_actor_here

                    if should_create_actor_here(
                        defn.name,
                        self.node_id,
                        available_nodes,
                        replicas=defn.replicas,
                        actor_weight=defn.weight,
                        node_weights=node_weights
                    ):
                        log.info(f"Creating {defn.name} on node {self.node_id} (CRUSH assigned)")
                        await self._create_actor_recursive(system, defn, available_nodes)
                    else:
                        log.debug(f"Skipping {defn.name}: assigned to another node")
                else:
                    # Standalone (1 node): create all actors locally
                    await self._create_actor_recursive(system, defn, available_nodes)

        await asyncio.sleep(0.1)
        log.info(f"Actor tree built on node {self.node_id}")

    async def _periodic_cleanup_loop(self) -> None:
        """
        Background task: periodic stale data cleanup (leader only).
        Runs every 5 minutes to clean up orphaned actor registrations.
        """
        while self._is_leader and not self._closed and self.redis:
            try:
                await asyncio.sleep(300)  # 5 minutes
                # await self._cleanup_stale_registrations()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Periodic cleanup error: {e}")
                await asyncio.sleep(300)

    async def send_message(
        self,
        destination: str,
        message: Any,
        sender_ref: Optional[ActorRef] = None
    ) -> bool:
        """
        Send message via Pub/Sub to specific node channel.

        Messages are published to node:{target_node}:inbox — only that node receives.
        """
        if not self.redis:
            return False

        payload = {
            "destination": destination,
            "data": message,
            "sender_ref": str(sender_ref) if sender_ref else None,
            "source_node": self.node_id,
            "timestamp": str(time.time()),
        }

        # 🎯 Determine target node from destination (should have node: prefix)
        target_node = None
        if destination.startswith("node:"):
            node_part, _ = destination.split("/", 1)
            target_node = node_part.replace("node:", "")
        else:
            # No prefix — fallback to registry lookup (should not happen)
            actor_nodes = await self.get_actor_nodes(destination)
            if actor_nodes:
                target_node = actor_nodes[0]
            else:
                log.warning(f"Actor {destination} not found in cluster")
                return False

        # 🎯 Publish to SPECIFIC NODE channel
        channel = f"{NODES_KEY}:{target_node}:inbox"
        await self.redis.publish(channel, json.dumps(payload))

        log.debug(f"Published to {channel} (destination: {destination})")
        return True

    def get_actor_ref_by_name(self, name: str) -> Optional[ActorRef]:
        """Get local ActorRef by name."""
        return self._actor_refs.get(name)

    def print_actor_tree(self) -> None:
        """Print actor tree for debugging (local actors only)."""
        graph: Dict[Optional[str], List[str]] = {}
        for defn in self._definitions.values():
            graph.setdefault(defn.parent, []).append(defn.name)

        def print_node(parent: Optional[str], level: int = 0) -> None:
            indent = "│    " * level
            for child in sorted(graph.get(parent, [])):
                defn = self._definitions.get(child)
                if defn:
                    marker = " 🎯" if defn.dynamic else " ♻️"
                    count = 1 if child in self._actor_refs else 0
                    replica_info = f" [{count}/{defn.replicas}]"
                    log.warning(f"{indent}├── {child}{marker}{replica_info}")
                    print_node(child, level + 1)

        log.warning("Actor System Tree (local):")
        print_node(None)

    async def _create_actor_recursive(
        self,
        system: ActorSystem,
        parent_defn: ActorDefinition,
        available_nodes: List[str]
    ) -> Optional[ActorRef]:
        """
        Recursively create static actors with CRUSH placement.

        Only creates actors that should be on THIS node according to CRUSH.
        """
        # Find parent ActorRef if not root
        parent_ref = None
        if parent_defn.parent:
            parent_ref = system.get_actor_ref_by_name(parent_defn.parent)
            if not parent_ref:
                log.error(f"Parent actor '{parent_defn.parent}' not found for '{parent_defn.name}'")
                return None

        # 🎯 CRUSH check: should this actor be created on THIS node?
        if len(available_nodes) > 1:
            from ..crush import should_create_actor_here

            if not should_create_actor_here(
                parent_defn.name,
                self.node_id,
                available_nodes,
                replicas=parent_defn.replicas,
                actor_weight=parent_defn.weight
            ):
                log.debug(f"⏭️ Skipping {parent_defn.name}: assigned to another node by CRUSH")
                return None

        # Create actor instance
        try:
            actor_instance = parent_defn.cls()
        except Exception as e:
            log.error(f"Failed to instantiate {parent_defn.name}: {e}")
            return None

        # Create in system with parent
        try:
            actor_ref = system._create(
                actor=actor_instance,
                parent=parent_ref,
                name=parent_defn.name
            )
            log.info(f"✅ Created actor: {parent_defn.name} on node {self.node_id}")
        except Exception as e:
            log.error(f"Failed to create actor {parent_defn.name}: {e}")
            return None

        # Register in registry
        await self.register(parent_defn.name, actor_ref, node_id=self.node_id)

        # 🎯 Find and create static children (with CRUSH check!)
        child_defs = [
            d for d in self._definitions.values()
            if d.parent == parent_defn.name and not d.dynamic
        ]

        for child_defn in child_defs:
            # 🎯 CHECK: should this child be on this node?
            if len(available_nodes) > 1:
                from ..crush import should_create_actor_here

                if not should_create_actor_here(
                    child_defn.name,
                    self.node_id,
                    available_nodes,
                    replicas=child_defn.replicas,
                    actor_weight=child_defn.weight
                ):
                    log.debug(f"⏭️ Skipping child {child_defn.name}: assigned to another node")
                    continue

            # Recursively create child (pass available_nodes!)
            await self._create_actor_recursive(system, child_defn, available_nodes)

        return actor_ref

    # ==========================================================================
    # Monitoring & Admin API (query any node for cluster status)
    # ==========================================================================

    async def get_cluster_status(self) -> Dict[str, Any]:
        """
        Get full cluster status as JSON.

        Can be called on any node - returns consistent view from Redis.

        Returns:
            Dict with nodes, actors, definitions status
        """
        if not self.redis:
            return {"error": "Redis not connected"}

        status = {
            "timestamp": time.time(),
            "queried_from_node": self.node_id,
            "leader": await self.redis.get(LEADER_KEY),
            "nodes": {},
            "actors": {},
            "definitions": [],
        }

        # Get all nodes
        nodes = await self.redis.smembers(f"{NODES_KEY}:all")
        for node in nodes:
            node_id = node.decode() if isinstance(node, bytes) else node
            node_data = await self.redis.hgetall(f"{NODES_KEY}:{node_id}")
            if node_data:
                status["nodes"][node_id] = {
                    k.decode(): v.decode() for k, v in node_data.items()
                }

        # Get all actors
        actor_names = await self.redis.smembers(f"{ACTORS_KEY}:*")
        for actor in actor_names:
            actor_name = actor.decode() if isinstance(actor, bytes) else actor
            if ":meta:" in actor_name:
                continue
            actor_nodes = await self.redis.smembers(f"{ACTORS_KEY}:{actor_name}")
            status["actors"][actor_name] = [
                n.decode() if isinstance(n, bytes) else n for n in actor_nodes
            ]

        # Get all definitions
        defs = await self.redis.smembers(f"{DEFS_KEY}:all")
        status["definitions"] = [
            d.decode() if isinstance(d, bytes) else d for d in defs
        ]

        return status

    async def get_alive_nodes(self) -> List[str]:
        """Get list of nodes that are currently alive (TTL not expired)."""
        if not self.redis:
            return [self.node_id]

        nodes = await self.redis.smembers(f"{NODES_KEY}:all")
        alive = []

        for node in nodes:
            node_id = node.decode() if isinstance(node, bytes) else node
            ttl = await self.redis.ttl(f"{NODES_KEY}:{node_id}")
            if ttl > 0:  # TTL not expired
                alive.append(node_id)

        return alive

    async def get_available_nodes(self) -> List[str]:
        """Get list of active nodes."""
        if not self.redis:
            return [self.node_id]

        nodes = await self.redis.smembers(f"{NODES_KEY}:all")
        return [n.decode() if isinstance(n, bytes) else n for n in nodes]

    async def get_actor_nodes(self, actor_name: str) -> List[str]:
        """Get list of nodes hosting a specific actor."""
        if not self.redis:
            return [self.node_id] if actor_name in self._actor_refs else []

        nodes = await self.redis.smembers(f"{ACTORS_KEY}:{actor_name}")
        return [n.decode() if isinstance(n, bytes) else n for n in nodes]
