"""Config for pylogstash"""
from pydantic import BaseSettings
from typing import Dict, Any

class Settings(BaseSettings):
    multiprocessing: bool = False
    num_procs: int = 8

    
    redis_stream: str = 'mystream'
    sentinel_hosts: list = ['localhost', 'redis-sentinel-1','redis-sentinel-2']
    redis_port: str = '26379'
    redis_batch_size: int = 1 # max number of items per batch
    redis_batch_timeout: int = 1 #milliseconds

    #elastic_host: list = [['hq-ref-v-elastic-1', 'hq-ref-v-elastic-2', 'hq-ref-v-elastic-3'],'localhost']
    elastic_host: list = ['localhost', 'elastic-cluster-1']

    class Config:
        pass

settings = Settings()