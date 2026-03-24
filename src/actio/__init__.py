# actio/__init__.py
# -*- coding: utf-8 -*-
"""Pure Python actor system for concurrent and distributed applications."""

from __future__ import annotations

from .config import ActioConfig
from .ref import ActorRef
from .ref import ActorDefinition
from .messages import PoisonPill
from .messages import DeadLetter
from .messages import Terminated
from .registry import LocalRegistry
from .registry import actio
from .registry import flush_pending_definitions
from .actor import Actor
from .system import ActorSystem

__version__ = "0.1.4"
__author__ = "Semenets V. Pavel"
__license__ = "MIT"

__all__ = [
    # Config
    'ActioConfig',

    # Ref
    'ActorRef',
    'ActorDefinition',

    # Messages
    'PoisonPill',
    'DeadLetter',
    'Terminated',

    # Registry
    'LocalRegistry',
    'actio',
    'flush_pending_definitions'

    # Actor
    'Actor',
    'ActorSystem',
]
