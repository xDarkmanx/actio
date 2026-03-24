# api/asys/services/Test.py
# -*- coding: utf-8 -*-

import logging

from typing import Dict
from typing import Any

from actio import Actor
from actio import ActorRef
from actio import actio

log = logging.getLogger('api.asys.services.Test')

@actio(name='Test', parent='ActioSystem')
class Test(Actor):
    def __init__(self):
        super().__init__()

    async def started(self):
        log.info(f"Test Actor started")

    async def receive(self, sender: ActorRef, message: Dict[str, Any]) -> None:
        action = message.get('action')

        match action:
            case 'get_status':
                log.warning(f'Receive Message {message}')
            case _:
                log.error(f"Unknown action: in {message}")

    async def stopped(self):
        pass

