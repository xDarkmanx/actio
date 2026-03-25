# __init__.py
# -*- coding: utf-8 -*-

import logging

from fastapi.middleware.cors import CORSMiddleware

from .app import app

from .cfg.api import config as cfg

log = logging.getLogger('app.main')

app.add_middleware(
    CORSMiddleware,
    allow_methods=['*'],
    allow_headers=['*'],
    allow_credentials=True
)

api_prefix = f'{cfg.API_PREFIX}/v{cfg.API_VER}'
