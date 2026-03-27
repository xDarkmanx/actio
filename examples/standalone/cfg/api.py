# cfg/api.py
# -*- coding: utf-8 -*-

from os import path

from pydantic import Field
from pydantic_settings import BaseSettings


class ApiConfig(BaseSettings):
    # Project Dir
    CURR_DIR: str = path.abspath(path.dirname(__file__))
    BASE_DIR: str = path.abspath(f'{CURR_DIR}/../')

    # Api Configuration
    API_PREFIX: str = Field('/api', description="Api server prefix")
    API_VER: str = Field('2', description="Api version")

    # Redis Configuration
    REDIS_ENABLED: bool = Field(False, description="Enable redis server")
    REDIS_URI: str = Field('redis://localhost:6379', description="Redis uri")

    # Actio Lib Configuration
    ACTIO_MODE: str = Field('standalone', description="Actor Library Mode")
    ACTIO_NODE_ID: str = Field('api', description="Actor Node ID")
    ACTIO_REGISTRY: str = Field('local', description="Actor Library Registry Type")


try:
    config = ApiConfig()
except Exception as e:
    print(f'Configuration error: {e}')
    print('Committing suicide...')
    exit(1)
