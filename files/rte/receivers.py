"""Receivers for pylogstash"""


import aredis
import asyncio
import aioify
from functools import wraps, partial
from redis.sentinel import Sentinel

class StrictRedisOverride(aredis.StrictRedis):
    def __init__(self, host, port, db):
        self.host = host
        self.port = port 
        self.db = db
        self.formatted_stream_index = ''
        super().__init__(host=self.host, port=self.port, db=self.db)
        
    async def xread(self, streams, count=None, block=None) -> dict:
        """
        Edited Apr 1, 2021 by Jordan Jarvis
        overrides aredis/commands/streams.py
        Function xread
        Changed: kwargs to normal arguments to allow for streamname to be based 
        off variable instead of variable name

        
        Available since 5.0.0.

        Time complexity:
        For each stream mentioned: O(log(N)+M) with N being the number
        of elements in the stream and M the number of elements being returned.
        If M is constant (e.g. always asking for the first 10 elements with COUNT),
        you can consider it O(log(N)). On the other side, XADD will pay the O(N)
        time in order to serve the N clients blocked on the stream getting new data.

        Read data from one or multiple streams,
        only returning entries with an ID greater
        than the last received ID reported by the caller.

        :param count: int, if set, only return this many items, beginning with the
               earliest available.
        :param block: int, milliseconds we want to block before timing out,
                if the BLOCK option is not used, the command is synchronous
        :param streams: stream_name - stream_id mapping
        :return dict like {stream_name: [(stream_id: entry), ...]}
        """
        pieces = []
        if block is not None:
            if not isinstance(block, int) or block < 0:
                raise RedisError("XREAD block must be a positive integer")
            pieces.append("BLOCK")
            pieces.append(str(block))
        if count is not None:
            if not isinstance(count, int) or count < 1:
                raise RedisError("XREAD count must be a positive integer")
            pieces.append("COUNT")
            pieces.append(str(count))
        pieces.append("STREAMS")
        pieces.extend(streams)
        return await self.execute_command('XREAD', *pieces)


import time
class RedisReceiver():
    def __init__(self, sentinel_hosts, port, db, batch_size, timeout, shared_list = {}):
        sentinel_hosts2 = []
        for host in sentinel_hosts: 
            sentinel_hosts2.append((host, 26379))
        self.shared_list = shared_list
        self.batch_size = batch_size
        self.timeout = timeout
        self.port = port 
        self.db = db
        print(sentinel_hosts)
        while True:
            try:
                sentinel = Sentinel(sentinel_hosts2)
                self.host, self.port = sentinel.discover_master("mymaster")
                self.p = StrictRedisOverride(self.host, self.port, self.db)
                self.r = self.p.pubsub()
                print("Redis connected successfully.")
                break
            except:
                print("Error, couldn't connect to redis host(s). Retrying connection in 20 seconds.")
                time.sleep(20)
                print("Retrying")
                continue
        self.last_indexes = {}
        self.streams = []

    def set_last_indexes(self,last_indexes):
        #
        # 
        # (self.shared_list)
        self.shared_list['indexes'] = last_indexes


    def get_last_indexes(self):
        #print(self.shared_list)
        return self.shared_list['indexes']

    def subscribe(self, streams):
        """sets stream name"""
        if isinstance(streams, str):
            self.streams = [streams]
        elif isinstance(streams,(list,set,tuple)):
            self.streams = list(streams)
        else:

            #exit()
            raise TypeError
            #exit()
            #throw exception unknown datatype for subscribe

    async def connect(self):
        """connect to the subscribed redis stream"""
        await self.p.ping()
        print("Redis connected successfully")
        for stream in self.streams:
            await self.r.subscribe(stream)

    async def read(self):
        """reads from the redis stream and returns what it reads"""
        return_value = []
        for i in self.streams:
            
            return_value.append(await self.p.xread(self.formatted_stream_index,count=self.batch_size,block=self.timeout))
        return return_value