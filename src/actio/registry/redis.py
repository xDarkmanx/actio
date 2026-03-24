# src/actio/registry/redis.py
# -*- coding: utf-8 -*-
"""
Redis-based registry implementation for cluster-ready actor system.

Architecture:
- All communication via Redis (Pub/Sub + Keys)
- No direct node-to-node connections
- Host/Port stored only for monitoring/admin purposes
- Cold-start philosophy: no state restoration in registry
"""

import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

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

# =============================================================================
# TTL constants (seconds)
# =============================================================================
NODE_HEARTBEAT_TTL = 30
NODE_HEARTBEAT_INTERVAL = 10
COMMAND_TTL = 60
ACK_TTL = 60

class RedisRegistry(RegistryProtocol):
    """
    Redis-based actor registry for standalone and cluster modes.

    Features:
    - Node discovery via heartbeat + TTL
    - Actor registration and lookup
    - Pub/Sub message delivery (local and cross-node)
    - Cold-start philosophy: no state restoration in registry
    - Command/ack mechanism for cluster coordination
    - Monitoring: query any node for full cluster status
    """

    def __init__(
        self,
        redis_url: str,
        node_id: str,
    ):
        """
        Initialize RedisRegistry.

        Args:
            redis_url: Redis connection URL (e.g., "redis://localhost:6379")
            node_id: Unique identifier for this node in the cluster
        """
        self.redis_url = redis_url
        self.node_id = node_id

        self.redis: Optional[aioredis.Redis] = None
        self.pubsub: Optional[aioredis.client.PubSub] = None
        self._system: Optional[ActorSystem] = None
        self._definitions: Dict[str, ActorDefinition] = {}
        self._actor_refs: Dict[str, ActorRef] = {}
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._msg_listener_task: Optional[asyncio.Task] = None
        self._cmd_listener_task: Optional[asyncio.Task] = None
        self._closed = False
        self._node_registered = False

    # ==========================================================================
    # Connection management
    # ==========================================================================

    async def connect(self) -> None:
        """Establish connection to Redis and start background tasks."""
        if self.redis is not None:
            return

        log.info(f"Connecting to Redis at {self.redis_url} (node: {self.node_id})")
        self.redis = await aioredis.from_url(self.redis_url)

        # Test connection
        await self.redis.ping()
        log.info("Redis connection established")

        # Register this node
        await self._register_node()
        self._node_registered = True

        # Start background tasks
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._msg_listener_task = asyncio.create_task(self._msg_listener_loop())
        self._cmd_listener_task = asyncio.create_task(self._cmd_listener_loop())

        log.info(f"RedisRegistry connected for node {self.node_id}")

    async def disconnect(self) -> None:
        """Close Redis connection and stop background tasks."""
        if self._closed:
            return

        self._closed = True
        log.info(f"Disconnecting RedisRegistry for node {self.node_id}")

        # Stop background tasks
        for task in [self._heartbeat_task, self._msg_listener_task, self._cmd_listener_task]:
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

        # Get host/port from environment (for monitoring only)
        host = os.getenv("HOST", "localhost")
        port = os.getenv("PORT", "5000")

        node_data = {
            "node_id": self.node_id,
            "host": host,
            "port": port,
            "registered_at": str(time.time()),
            "status": "active",
        }

        # Set node metadata with TTL
        await self.redis.hset(f"{NODES_KEY}:{self.node_id}", mapping=node_data)
        await self.redis.expire(f"{NODES_KEY}:{self.node_id}", NODE_HEARTBEAT_TTL)

        # Add to set of all nodes
        await self.redis.sadd(f"{NODES_KEY}:all", self.node_id)

        log.debug(f"Node {self.node_id} registered in Redis")

    async def _heartbeat_loop(self) -> None:
        """Background task: renew node registration TTL."""
        while not self._closed and self.redis:
            try:
                await self._register_node()  # Renew TTL
                await asyncio.sleep(NODE_HEARTBEAT_INTERVAL)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Heartbeat error for node {self.node_id}: {e}")
                await asyncio.sleep(NODE_HEARTBEAT_INTERVAL)

    # ==========================================================================
    # Pub/Sub message listener
    # ==========================================================================

    async def _msg_listener_loop(self) -> None:
        """Background task: listen for messages via Pub/Sub."""
        if not self.redis:
            return

        self.pubsub = self.redis.pubsub()
        channel_pattern = f"{MSG_CHANNEL}:*"

        await self.pubsub.psubscribe(channel_pattern)
        log.debug(f"Subscribed to Pub/Sub pattern: {channel_pattern}")

        try:
            while not self._closed:
                message = await self.pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=1.0
                )

                if message and message["type"] == "pmessage":
                    await self._handle_pubsub_message(message)

        except asyncio.CancelledError:
            pass
        finally:
            if self.pubsub:
                await self.pubsub.punsubscribe()
                await self.pubsub.close()
                self.pubsub = None

    async def _handle_pubsub_message(self, message: Dict[str, Any]) -> None:
        """Process incoming Pub/Sub message and deliver to local actor."""
        try:
            channel = message["channel"]
            if isinstance(channel, bytes):
                channel = channel.decode()

            # Parse message payload
            data = message["data"]
            if isinstance(data, bytes):
                data = data.decode()

            payload = json.loads(data)
            destination = payload.get("destination")

            if not destination:
                log.warning(f"Message without destination: {payload}")
                return

            # Check if this actor exists locally
            actor_ref = self._actor_refs.get(destination)
            if actor_ref and self._system:
                # Deliver to local actor
                sender_ref = payload.get("sender_ref")
                message_data = payload.get("data", {})
                self._system.tell(actor_ref, message_data, sender=sender_ref)
                log.debug(f"Delivered message to local actor: {destination}")
            else:
                log.debug(f"Actor {destination} not found on node {self.node_id} (cross-node or not created)")

        except json.JSONDecodeError as e:
            log.error(f"Failed to parse Pub/Sub message: {e}")
        except Exception as e:
            log.error(f"Error handling Pub/Sub message: {e}")

    # ==========================================================================
    # Cluster command listener
    # ==========================================================================

    async def _cmd_listener_loop(self) -> None:
        """Background task: listen for cluster commands."""
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
        """Process cluster command (STOP_ALL, REBALANCE, etc.)."""
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

        except Exception as e:
            log.error(f"Error handling command: {e}")

    async def _handle_stop_all(self, cmd: Dict[str, Any]) -> None:
        """Handle STOP_ALL command: stop all local actors."""
        log.info(f"STOP_ALL received on node {self.node_id}")

        if self._system:
            await self._system.stop_all_actors()

        await self._send_ack("CLEAN")

    async def _handle_rebalance(self, cmd: Dict[str, Any]) -> None:
        """Handle REBALANCE command: rebuild actor tree."""
        log.info(f"REBALANCE received on node {self.node_id}")

        if self._system:
            await self.build_actor_tree(self._system)

        await self._send_ack("REBALANCED")

    async def _send_ack(self, status: str) -> None:
        """Send acknowledgment for a command."""
        if not self.redis:
            return

        ack_key = f"{CLUSTER_KEY}:ack:{self.node_id}"
        ack_data = {
            "node_id": self.node_id,
            "status": status,
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

        # Register in Redis: which nodes host this actor
        if self.redis:
            await self.redis.sadd(f"{ACTORS_KEY}:{name}", target_node)

            # Store actor metadata
            meta = {
                "name": name,
                "node_id": target_node,
                "registered_at": str(time.time()),
            }
            await self.redis.hset(f"{ACTORS_KEY}:meta:{name}", mapping=meta)

        log.debug(f"Registered actor {name} on node {target_node}")

    async def unregister(self, name: str, node_id: Optional[str] = None) -> None:
        """Unregister an actor instance."""
        target_node = node_id or self.node_id

        # Remove local reference
        self._actor_refs.pop(name, None)

        # Remove from Redis
        if self.redis:
            await self.redis.srem(f"{ACTORS_KEY}:{name}", target_node)
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
        """Build actor tree for static (non-dynamic) actors."""
        self._system = system
        log.info(f"Building actor tree on node {self.node_id}")

        # Get static actor definitions (dynamic=False)
        static_defs = [
            d for d in self._definitions.values()
            if not d.dynamic
        ]

        # Create root actors (parent=None)
        for defn in static_defs:
            if defn.parent is None:
                if await self._should_create_actor(defn.name):
                    await self._create_actor_recursive(system, defn)

        # Wait briefly for actors to start
        await asyncio.sleep(0.1)

        log.info(f"Actor tree built on node {self.node_id}")

    async def _should_create_actor(self, actor_name: str) -> bool:
        """Determine if this node should create the actor.

        For standalone: always True.
        For cluster: hash-based assignment (TODO: implement CRUSH).
        """
        return True

    async def _create_actor_recursive(
        self,
        system: ActorSystem,
        parent_defn: ActorDefinition
    ) -> Optional[ActorRef]:
        """Recursively create static actors and register them."""
        # Find parent ActorRef if not root
        parent_ref = None
        if parent_defn.parent:
            parent_ref = system.get_actor_ref_by_name(parent_defn.parent)
            if not parent_ref:
                log.error(f"Parent actor '{parent_defn.parent}' not found for '{parent_defn.name}'")
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
            log.info(f"Created actor: {parent_defn.name} on node {self.node_id}")
        except Exception as e:
            log.error(f"Failed to create actor {parent_defn.name}: {e}")
            return None

        # Register in registry
        await self.register(parent_defn.name, actor_ref, node_id=self.node_id)

        # Find and create static children
        child_defs = [
            d for d in self._definitions.values()
            if d.parent == parent_defn.name and not d.dynamic
        ]

        for child_defn in child_defs:
            await self._create_actor_recursive(system, child_defn)

        return actor_ref

    async def send_message(
        self,
        destination: str,
        message: Any,
        sender_ref: Optional[ActorRef] = None
    ) -> bool:
        """Send message to actor via Pub/Sub (local or cross-node)."""
        if not self.redis:
            return False

        payload = {
            "destination": destination,
            "data": message,
            "sender_ref": str(sender_ref) if sender_ref else None,
            "source_node": self.node_id,
            "timestamp": str(time.time()),
        }

        # Publish to actor-specific channel
        channel = f"{MSG_CHANNEL}:{destination}"
        await self.redis.publish(channel, json.dumps(payload))

        log.debug(f"Published message to {channel}")
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
        actor_names = await self.redis.smembers(f"{DEFS_KEY}:all")
        for actor in actor_names:
            actor_name = actor.decode() if isinstance(actor, bytes) else actor
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
