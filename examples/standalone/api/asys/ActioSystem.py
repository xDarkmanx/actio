# api/asys/ActioSystem.py
# -*- coding: utf-8 -*-

import logging

from typing import Any

# Actor Subsystem
from actio import Actor
from actio import ActorRef
from actio import actio

# Actor Messages
from actio import Terminated

from ...cfg.api import config as cfg

log = logging.getLogger("api.asys.ActioSystem")

@actio(name='ActioSystem', parent=None)
class ActioSystem(Actor):
    def __init__(self):
        super().__init__()

        self.registry = None

    async def started(self) -> None:
        log.info(f"Starting root Actor: mode => {cfg.ACTIO_MODE}")

    async def receive(self, sender: ActorRef, message: Any) -> None:
        if isinstance(message, Terminated):
            log.warning(f"Received Terminated from {sender}")

    async def stopped(self) -> None:
        pass
