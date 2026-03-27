# src/actio/crush/mapper.py
# -*- coding: utf-8 -*-
"""
Simple CRUSH-like mapper for deterministic actor placement with weight-based load balancing.

Given an actor name, weight, and available nodes with their weights,
returns the nodes that should host the actor for balanced distribution.

No external state, no consensus — just hash-based assignment with capacity awareness.
"""

import hashlib
from typing import List, Tuple, Dict, Union, Optional

def get_node_for_actor(
    actor_name: str,
    available_nodes: List[str]
) -> str:
    """
    Deterministically assign an actor to a single node.

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

    # Simple hash-based assignment using SHA256 for better distribution
    hash_input = f"{actor_name}:{','.join(sorted(available_nodes))}"
    hash_bytes = hashlib.sha256(hash_input.encode()).digest()
    hash_int = int.from_bytes(hash_bytes[:8], byteorder='big')

    # Map to node index
    node_index = hash_int % len(available_nodes)
    return sorted(available_nodes)[node_index]


def map_actor_with_capacity(
    actor_name: str,
    nodes_with_capacity: List[Tuple[str, float]],  # [(node_id, available_capacity), ...]
    replicas: int
) -> List[str]:
    """
    Map actor to nodes considering available capacity.

    Nodes with higher available capacity have higher priority,
    but final selection is hash-based for determinism.

    Args:
        actor_name: For hashing (determinism)
        nodes_with_capacity: [(node_id, available_capacity), ...]
        replicas: Number of replicas needed

    Returns:
        List of node_ids (sorted) where actor should be placed
    """
    if not nodes_with_capacity or replicas <= 0:
        return []

    # Filter nodes with positive capacity
    available = [(n, c) for n, c in nodes_with_capacity if c > 0]

    if not available:
        return []

    # Sort by capacity descending (higher capacity = higher priority)
    available.sort(key=lambda x: x[1], reverse=True)

    # Hash-based selection with capacity weighting
    selected = []
    for i in range(min(replicas, len(available))):
        # Deterministic hash input
        hash_input = f"{actor_name}:{i}:{','.join([n[0] for n in available])}"
        hash_val = hashlib.sha256(hash_input.encode()).hexdigest()

        # Select node (weighted by capacity via sorting)
        idx = int(hash_val[:8], 16) % len(available)
        selected.append(available[idx][0])

    return sorted(selected)


def should_create_actor_here(
    actor_name: str,
    current_node_id: str,
    available_nodes: List[str],
    replicas: Union[int, str] = 1,
    actor_weight: float = 0.01,
    node_weights: Optional[Dict[str, float]] = None,
    node_loads: Optional[Dict[str, float]] = None,
    parent_nodes: Optional[List[str]] = None
) -> bool:
    """
    Check if current node should create the given actor.

    Rules:
    - replicas='all' → create on ALL available nodes
    - replicas=N → create on N nodes (capacity-aware)
    - If parent_nodes specified → children only on those nodes
    - Actor weight + node weight + current load → balanced placement

    Args:
        actor_name: Name of the actor
        current_node_id: ID of the node checking
        available_nodes: List of alive node IDs
        replicas: Number of replicas (or 'all')
        actor_weight: Resource weight of this actor
        node_weights: {node_id: weight} — node capacities
        node_loads: {node_id: current_load} — current resource usage
        parent_nodes: If specified, only place on these nodes (for children)

    Returns:
        True if this node should create the actor
    """
    # Root / replicated actors run on all nodes
    if replicas == 'all':
        return current_node_id in available_nodes

    # Limit to parent nodes if specified (children constraint)
    candidate_nodes = available_nodes
    if parent_nodes:
        candidate_nodes = [n for n in available_nodes if n in parent_nodes]
        if not candidate_nodes:
            return False  # No valid parent nodes available

    # Handle single replica case (simple CRUSH)
    if replicas == 1:
        target = get_node_for_actor(actor_name, candidate_nodes)
        return target == current_node_id

    # Multi-replica with capacity awareness
    node_weights = node_weights or {n: 1.0 for n in candidate_nodes}
    node_loads = node_loads or {n: 0.0 for n in candidate_nodes}

    # Calculate available capacity for each node
    nodes_with_capacity = []
    for node_id in candidate_nodes:
        weight = node_weights.get(node_id, 1.0)
        load = node_loads.get(node_id, 0.0)
        available_capacity = max(0.0, weight - load)
        nodes_with_capacity.append((node_id, available_capacity))

    # Get target nodes via capacity-aware mapping
    target_nodes = map_actor_with_capacity(
        actor_name=actor_name,
        nodes_with_capacity=nodes_with_capacity,
        replicas=min(int(replicas), len(nodes_with_capacity))
    )

    return current_node_id in target_nodes
