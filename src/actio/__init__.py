# actio/__init__.py
# -*- coding: utf-8 -*-

from .config import ActioConfig

from .ref import ActorRef
from .ref import ActorDefinition

from .messages import PoisonPill
from .messages import DeadLetter
from .messages import Terminated

from .registry import ActorRegistry
from .registry import registry
from .registry import actio

from .actor import Actor
from .actor import ActorSystem
from .cluster import ClusterActor

__version__ = "0.0.7"
__author__ = "Semenets V. Pavel"
__license__ = "MIT"

__all__ = [
    # from config
    'ActioConfig',

    # from ref.py
    'ActorRef',
    'ActorDefinition',

    # from messages.py
    'PoisonPill',
    'DeadLetter',
    'Terminated',

    # from registry.py
    'ActorRegistry',
    'registry',
    'actio',

    # from actor.py
    'Actor',
    'ActorSystem',

    # from cluster.py
    'ClusterActor'
]
