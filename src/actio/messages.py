# actio/messages.py
# -*- coding: utf-8 -*-

from typing import Any
from dataclasses import dataclass

from . import ActorRef


@dataclass(frozen=True)
class PoisonPill:
    pass


@dataclass(frozen=True)
class DeadLetter:
    actor: ActorRef
    message: Any


@dataclass(frozen=True)
class Terminated:
    actor: ActorRef
