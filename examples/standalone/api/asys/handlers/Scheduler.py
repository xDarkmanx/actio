# api/asys/handlers/Scheduler.py
# -*- coding: utf-8 -*-

import logging

from typing import Dict
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.executors.asyncio import AsyncIOExecutor

from actio import Actor
from actio import ActorRef
from actio import actio

log = logging.getLogger("api.asys.handlers.Scheduler")

@actio(name='Scheduler', parent='ActioSystem', replicas=1)
class Scheduler(Actor):
    def __init__(self):
        super().__init__()

        self.scheduler = None
        self.scheduled_jobs = {}

    async def started(self) -> None:
        log.info("Scheduler started")

        await self._init_scheduler()
        await self._schedule_parser_tasks()

    async def _schedule_parser_tasks(self):
        pass

    async def _init_scheduler(self):
        try:
            log.info('Initializing scheduler')

            self.scheduler = AsyncIOScheduler(
                executors={
                    'default': AsyncIOExecutor(),
                },
                job_defaults={
                    'coalesce': True,
                    'max_instances': 1,
                    'misfire_grace_time': 30
                }
            )

            self.scheduler.start()
            log.info('Scheduler started')

        except Exception as e:
            log.error(f'Error initializing scheduler: {e}')

    async def stopped(self):
        if self.scheduler:
            self.scheduler.shutdown()
            log.info('Scheduler shutdown completed')
