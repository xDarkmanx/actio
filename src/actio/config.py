# actio/config.py
# -*- coding: utf-8 -*

from typing import List
from typing import Dict
from typing import Any

from pydantic import BaseModel
from pydantic import Field
from pydantic import field_validator


class ActioConfig(BaseModel):
    node_id: str = Field(..., description="Unique node ID")
    node_ip: str = Field('0.0.0.0', description="Node ip address def: 0.0.0.0")
    cluster_port: int = Field(default=7946, description="Actio cluster port communication")
    cluster_nodes: List[str] = Field(default_factory=list, description="Cluster node list")

    gossip_interval: float = Field(default=1.0, gt=0, description="GOSSIP Interval")
    failure_timeout: float = Field(default=10.0, gt=0, description="Failure detect timeout")
    heartbeat_interval: float = Field(default=3.0, gt=0, description="Heartbeat interval")

    # Балансировка и метрики
    node_weight: float = Field(default=1.0, gt=0, description="Node weight for CrushMapper")
    resources: Dict[str, Any] = Field(default_factory=dict, description="Ресурсы ноды для балансировки")

    @field_validator('cluster_nodes')
    @classmethod
    def validate_cluster_nodes(cls, v: List[str]) -> List[str]:
        """ Node format validation """
        for node in v:
            if ':' not in node:
                raise ValueError(f"Node must be in 'host:port' format, got: {node}")
        return v

    @field_validator('node_id')
    @classmethod
    def validate_node_id(cls, v: str) -> str:
        """ Node id validation """
        if not v or not v.strip():
            raise ValueError("node_id cannot be empty")
        return v.strip()

    class Config:
        extra = "allow"
