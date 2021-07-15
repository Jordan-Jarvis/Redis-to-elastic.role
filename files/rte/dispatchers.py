"""This sends data to a service"""
import multiprocessing
from ssl import create_default_context
from typing import NoReturn
import elasticsearch as _elasticsearch
import asyncio
import timeit
import pickle
import json
import time
from functools import wraps

"""a basic timer class"""
class Timer():
    def __init__(self):
        self.starttime = 0.0

    def start(self):
        self.starttime = time.time()

    def get_time(self):
        return time.time() - self.starttime

    def reset(self):
        self.start()
import atexit

import os
class PersistentList(list):
    def __init__(self, key, resetkey = False):
        self.key = key
        try:
            if resetkey:
                os.remove(key + ".pickle")
        except:
            pass
        try:
            with open(key + ".pickle",'rb') as openfile:
                for val in pickle.load(openfile):
                    self.append(val)
        except:
            pass
    def _save(self):
        filen = open(self.key + ".pickle", "wb")
        pickle.dump(self, filen)
        print("SAVED")

    def __enter__(self):
        return self
    def __exit__(self,ext_type,exc_value,traceback):
        self._save()

lock = multiprocessing.Lock()
class Error(Exception):
    """Base class for other exceptions"""
    pass

class CouldNotConnectToES(Exception):
    def __init__(t):
        print("The elastic service could not connect to the server. Ensure it is online and that the computer has access")
    """Raised when unable to sniff es server"""
    pass

class ElasticDispatcher():
    def __init__(self, hosts): 
        
        self.timer = Timer()
        self.timer.start()
        self.es_list = []
        for host in hosts:
            self.es_list.append([_elasticsearch.Elasticsearch(host,
            #sniff_on_start=True,
            sniff_on_connection_fail=True,
            sniffer_timeout=60,
            sniff_timeout=10),0,'0-0',host,PersistentList(host)]) # hosts list contains an integer which represents connection status and last index location
        for es in self.es_list:
            atexit.register(es[4]._save)
        self.last_index_save = 0
        self.r = None
        self.last_indexes = {}
        self.queue = []
        
    def connect(self, receiver):
        self.r = receiver
        try:
            self.r.set_last_indexes(self.get_last_index_locations())
            print("Elastic is connected")
        except _elasticsearch.exceptions.NotFoundError:
            pass
        tmp = self.r.get_last_indexes()
        for val in self.r.streams:
            try:
                if tmp[val] == None:
                    pass
            except KeyError:
                tmp[val] = '0-0'
        self.r.set_last_indexes(tmp)
        self.save_index_location()
        self.last_indexes = self.r.get_last_indexes()
        self.set_formatted_stream_index()

    def set_formatted_stream_index(self):
        formatted_stream_index = []
        temp = []
        tmp = self.r.get_last_indexes()
        for val in tmp:
            formatted_stream_index.append(val)
            temp.append(tmp[val])
        formatted_stream_index.extend(temp)
        self.r.formatted_stream_index = formatted_stream_index

    def disconnect(self):
        self.r = None
        self.last_indexes = None

    def save_index_location(self, index='last_index_location'):
        body = {index:self.r.get_last_indexes()}
        print('saved index location')
        print(body)
        body = json.dumps(self.convert_to_utf8(body))
        
        res = self.es_list[0][0].index(index=index, id=1, body=body)
        return res

    def check_buffer_files(self,host):
        try:
            indecies = pickle.load( open( f"{host}.p", "rb" ) ) # opens any pickled (.p) files and tries to send them to es again
            successes = []
            for b in range(len(indecies)):
                successes.append(False) # make all false and turn them successful on success
            failcount = 0
            for i, val in enumerate(indecies):
                if self.index_to_host(host, val['index'],val['id'],val['body']):
                    successes[i] = True
                else:
                    failcount += 1
                if failcount >= 1:
                    break
            for i, val in enumerate(reversed(successes)):
                if val:
                    indecies.pop(i) # if it was added successfully

        except IOError:
            #couldn't read the file. may not exist.
            return
        
    def check_buffer(self):
        #see if anything is in buffer and try to index it.
        timer_expired = False
        if self.timer.get_time() > 20:
            timer_expired = True
            self.timer.reset()
        for val in self.es_list:
            numfails = 0
            for i, val1 in enumerate(val[4]): 
                try:       
                    if val[1] == 0:
                        res = val[0].index(index=val1['index'], id=val1['id'], body=val1['body'])
                        val[4].pop(i)
                    else:
                        if timer_expired == True:
                            res = val[0].index(index=val1['index'], id=val1['id'], body=val1['body'])
                            val[1]=0
                except:
                    numfails += 1
                    val[1]=1
                    print("Elastic connection failed for "+ val[3])

                if numfails >= 1:
                    # we failed 3 times, no need to overburden the system with failing requests
                    # skipping the rest for now
                    val[1]=0
                    break
            if numfails < 1:
                #if it didn't fail too much then check the buffer file saved to disk.
                self.check_buffer_files(val[3])
            else:
                val[1] = 1
            # for i, value in enumerate(reversed(tmplist)):
            #     if value == True:
            #         print("OOO")# is val 1-4 the es servers? Also it looks like here it's checking if it's connected (is that the 0?) and if it is it removes it from the array
            #         val[4].pop(i) # if any failed they will remain in the buffer

    async def index_to_all(self, index, id, body, from_file = False):
        for es in self.es_list:
            if es[1] == 1:
                es[4].append({'index': index, 'id': id, 'body': self.convert(body)})
                if len(es[4]) % 10:
                    es[4]._save()
                print("after",es[4])
                continue

            try:

                    #self.check_buffer() # need to check if any data is cached then add it to indexing
                res = es[0].index(index=index, id=id, body=body)
            except:
                print("Could not connect to " + es[3] + " es server.")
                self.convert(body)
                es[4].append({'index': index, 'id': id, 'body': self.convert(body)})
                if len(es[4]) % 10:
                    es[4]._save()
                es[1] = 1
 


    def get_last_index_locations(self, index='last_index_location'):
        
        res = self.es_list[0][0].get(index=index, id=1)

        return res['_source'][index]

    def convert(self,input):
        if isinstance(input, dict):
            return {self.convert(key): self.convert(value) for key, value in input.items()}
        elif isinstance(input, list):
            return [self.convert(element) for element in input]
        elif isinstance(input, bytes):
            print(input)
            return input.encode('utf-8')
        else:
            print(type(input))
            return input

    async def wait_for_value(self):
        try:
            await self.r.connect()
            while True:
                
                return_value = await self.r.read()
                for val in self.es_list:
                    if val[1] == 1:
                        if self.timer.get_time() < 20:
                            continue
                        else:
                            #enable connection check
                            print("enabled conection check for " + val[3])
                            val[1]=0
                            self.timer.reset()

                    if len(val[4]) != 0:
                                #if cached data exists
                        self.check_buffer()
                if len(return_value[0]) > 0:
                    return_value = self.convert_to_utf8(return_value[0])
                    if len(return_value) == 0:
                        self.check_buffer()
                        continue
                    with lock:
                                
                        last_indexes = self.r.get_last_indexes()
                        for val in return_value:
                            for val2 in return_value[val][len(return_value[val])-1]:
                                last_indexes[val] = val2
                        self.r.set_last_indexes(last_indexes)

                    await self.push(return_value,last_indexes)

                    
        except Exception as exc:
            exit()

    def recurse(self,k,v):
            if isinstance(k,bytes):
                k = k.decode()
            if isinstance(v,dict):
                v = self.convert_to_utf8(v)
            elif isinstance(v,(tuple,list,set)):
                v = [self.convert_to_utf8(x) for x in v]
            return k,v

    def convert_to_utf8(self,val):
        rval = {}        
        if isinstance(val,tuple):
            val = list(val)
            if len(val) > 1:
                for i in range(0,len(val),2):
                    k,v = self.recurse(val[i],val[i+1])
                    rval[k] = v
        else:
            for k,v in val.items():
                k,v = self.recurse(k,v)
                rval[k] = v
        return rval

    async def push(self, return_value, last_indexes):

        self.last_index_save += 1
        with lock:
            self.r.shared_list['requests'] += 1
        if self.last_index_save > 100:
            self.last_index_save = 0

            self.save_index_location()
        self.set_formatted_stream_index()
        for stream in self.r.streams:
            try:
                tmp = return_value[stream]
            except KeyError:
                continue
            if len(self.queue) > 10:
                for i in range(5):
                    # print(await self.queue[0][0])
                    # print(await self.queue[1][0])
                    self.queue.pop(0)
                    self.queue.pop(0)
            #await self.index_es(stream, return_value, last_indexes)
            self.queue.append([asyncio.create_task(self.index_to_all(stream, return_value, last_indexes)),[stream,return_value,last_indexes]])


    async def index_es(self,stream,return_value, last_indexes):
            # try:
            #     self.es.update(
            #             index=stream,
            #             id=last_indexes[stream],
            #             body={
            #                 'message': str(return_value[stream])
            #             }
            #         )
            # except _elasticsearch.exceptions.RequestError:

                self.index_to_all(
                        index=stream,
                        id=self.r.get_last_indexes()[stream],
                        body={
                            'message': str(return_value[stream])
                        }
                    )

    def stream(self):
        loop = asyncio.get_event_loop()
        try:
            asyncio.ensure_future(self.wait_for_value())
            loop.run_forever()

            print("Closing Loop")
            loop.close()
        except:
            self.save_index_location()
            exit()
            