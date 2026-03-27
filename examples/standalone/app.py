# app.py
# -*- coding: utf-8 -*-

import logging

from fastapi import FastAPI

from contextlib import asynccontextmanager

from actio import ActorSystem
from actio import flush_pending_definitions

from cfg.api import config as cfg

from api.asys import ActioSystem
_ = ActioSystem

log = logging.getLogger('app')


@asynccontextmanager
async def app_lifespan(app: FastAPI):
    log.info('Start Actio Standalone')
    match cfg.ACTIO_REGISTRY:
        case 'local':
            from actio import LocalRegistry
            registry = LocalRegistry()
        case 'redis':
            from actio import RedisRegistry
            registry = RedisRegistry(
                node_id=cfg.ACTIO_NODE_ID,
                redis_url=cfg.REDIS_URI
            )
            await registry.connect()
        case _:
            raise ValueError(f"Unknown registry: {cfg.ACTIO_REGISTRY}")

    await flush_pending_definitions(registry)

    asys = ActorSystem(registry=registry)
    await registry.build_actor_tree(system=asys, timeout=1.0)
    registry.print_actor_tree()

    yield

    await asys.shutdown()
    log.info('Shutdown Actio Standalone')


app = FastAPI(
    title="Actio Standalone Server",
    docs_url=None,
    version='0.0.1',
    lifespan=app_lifespan
)
