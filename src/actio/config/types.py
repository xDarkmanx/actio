# actio/config/types.py
# -*- coding: utf-8 -*-
"""Configuration types for actio."""

from typing import List
from typing import Dict
from typing import Any

from pydantic import BaseModel
from pydantic import Field
from pydantic import field_validator

class ActioConfig(BaseModel):
    """Configuration for actio node.

    Used for cluster mode. For standalone mode, minimal config is sufficient.
    """
    node_id: str = Field(..., description="Unique node identifier")
    node_ip: str = Field("0.0.0.0", description="Node IP address for cluster communication")
    cluster_port: int = Field(default=7946, description="Port for cluster communication")
    cluster_nodes: List[str] = Field(default_factory=list, description="List of cluster node addresses")

    # Gossip and failure detection (used by cluster registries)
    gossip_interval: float = Field(default=1.0, gt=0, description="Interval between gossip messages")
    failure_timeout: float = Field(default=10.0, gt=0, description="Timeout to mark node as failed")
    heartbeat_interval: float = Field(default=3.0, gt=0, description="Heartbeat interval")

    # Load balancing (used by CrushMapper)
    node_weight: float = Field(default=1.0, gt=0, description="Node weight for CrushMapper")
    resources: Dict[str, Any] = Field(default_factory=dict, description="Node resources for load balancing")

    @field_validator("cluster_nodes")
    @classmethod
    def validate_cluster_nodes(cls, v: List[str]) -> List[str]:
        """Validate and clean cluster node list."""
        return [node.strip() for node in v if node.strip()]

    @field_validator("node_id")
    @classmethod
    def validate_node_id(cls, v: str) -> str:
        """Validate node_id is not empty."""
        v = v.strip()
        if not v:
            raise ValueError("node_id cannot be empty")
        return v

    class Config:
        extra = "allow"
