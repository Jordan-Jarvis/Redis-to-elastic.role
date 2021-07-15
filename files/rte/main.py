"""Point of entry"""
import aredis as _redis
import dispatchers
import receivers
import config
import multiprocessing as mp
import asyncio
import time
import os
import sys
time.sleep(0.5)
print(sys.argv[0])
os.chdir(os.path.dirname(sys.argv[0]))

lock = mp.Lock()

def driver(shared_list):
    try:
        r = receivers.RedisReceiver(sentinel_hosts=config.settings.sentinel_hosts, port=config.settings.redis_port, db=0, batch_size=config.settings.redis_batch_size, timeout=config.settings.redis_batch_timeout, shared_list = shared_list)
        r.subscribe(config.settings.redis_stream) # can be string or list of strings
        d = dispatchers.ElasticDispatcher(hosts=config.settings.elastic_host)
        d.connect(r)
        d.stream()
    finally:
        pass

def main():
    manager = mp.Manager()
    shared_list = manager.dict(        {
            'indexes' : {},
            'requests' : 0,
        })
    if config.settings.multiprocessing == True:
        processes = []
        for i in range(config.settings.num_procs):
            processes.append(mp.Process(target=driver, args=[shared_list]))
        for val in processes:
            val.start()
        for val in processes:
            val.join()
    else:
        driver({
            'indexes' : {},
            'requests' : 0,
        })
    
        
    #p2 = mp.Process(target=driver, args=(shared_list,lock))


        

if __name__ == '__main__':
    main()