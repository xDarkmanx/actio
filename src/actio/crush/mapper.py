# src/actio/crush/mapper.py
# -*- coding: utf-8 -*-
"""
Simple CRUSH-like mapper for deterministic actor placement.

Given an actor name and available nodes, returns the node that should host it.
No external state, no consensus — just hash-based assignment.
"""

import hashlib
from typing import List
from typing import Any

def get_node_for_actor(actor_name: str, available_nodes: List[str]) -> str:
    """
    Deterministically assign an actor to a node.

    Same actor_name + same available_nodes → same result.
    If nodes change, assignment may change (expected behavior).

    Args:
        actor_name: Name of the actor to place
        available_nodes: List of active node IDs

    Returns:
        Node ID that should host this actor

    Raises:
        ValueError: If available_nodes is empty
    """
    if not available_nodes:
        raise ValueError("Cannot place actor: no available nodes")

    # Simple hash-based assignment
    # Using SHA256 for better distribution than built-in hash()
    hash_input = f"{actor_name}:{','.join(sorted(available_nodes))}"
    hash_bytes = hashlib.sha256(hash_input.encode()).digest()
    hash_int = int.from_bytes(hash_bytes[:8], byteorder='big')

    # Map to node index
    node_index = hash_int % len(available_nodes)
    return sorted(available_nodes)[node_index]


def should_create_actor_here(
    actor_name: str,
    current_node_id: str,
    available_nodes: List[str],
    replicas: Any = 1  # ← Новый параметр из ActorDefinition
) -> bool:
    """
    Check if current node should create the given actor.

    - replicas='all' → create on ALL nodes
    - replicas=N → create on N nodes (TODO: implement)
    - replicas=1 → CRUSH-based single placement
    """
    # 🎯 Root / replicated actors run on all nodes
    if replicas == 'all':
        return True

    # 🎯 Single instance: CRUSH-based placement
    target = get_node_for_actor(actor_name, available_nodes)
    return target == current_node_id
