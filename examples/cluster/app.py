# app.py
# -*- coding: utf-8 -*-

import logging
import asyncio

from fastapi import FastAPI

from contextlib import asynccontextmanager

from actio import ActorSystem
from actio import flush_pending_definitions

from .api.asys import ActioSystem
_ = ActioSystem

from .cfg.api import config as cfg

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
                redis_url=cfg.REDIS_URI,
                node_weight=cfg.ACTIO_NODE_WEIGHT,
            )
        case _:
            raise ValueError(f"Unknown registry: {cfg.ACTIO_REGISTRY}")

    await flush_pending_definitions(registry)
    asys = ActorSystem(registry=registry)

    if cfg.ACTIO_REGISTRY == 'redis':
        await registry.connect(system=asys)
    else:
        await registry.connect()

    if cfg.ACTIO_REGISTRY == 'redis' and cfg.ACTIO_MODE == 'cluster':
        from actio.registry.redis import LEADER_KEY
        leader = await registry.redis.get(f"{LEADER_KEY}:leader:current")
        leader_id = leader.decode() if leader else "unknown"
        log.info(f"⏳ {cfg.ACTIO_MODE} mode: waiting for leader ({leader_id}) to orchestrate")
    else:
        log.info("🎯 Standalone mode: building actor tree locally")
        await registry.build_actor_tree(system=asys, timeout=10.0)

    yield

    await asys.shutdown()
    log.info('Shutdown Actio Cluster')

app = FastAPI(
    title="Actio Cluster Server",
    docs_url=None,
    version='0.0.1',
    lifespan=app_lifespan
)
