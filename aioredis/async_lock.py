import asyncio
from common.async_sentinel import AsyncRedisSentinelHelper
from threading import Thread
import feapson
import pandas as pd
import numpy as np
import time


# 自定义哨兵线程监控锁的过期时间
class SentinelThread(Thread):
    def __init__(self, func, args=(), loop=None):
        super(SentinelThread, self).__init__()
        self.func = func
        self.args = args
        self.loop = loop    # 每个线程有个事件循环, 

    def run(self):
        self.loop.create_task(self.func(*self.args))  # 在当前事件循环中创建一个任务, 任务会被放到调度器中


#todo 实现分布式锁测试
class lockTest(object):
    def __init__(self) -> None:
        super().__init__()
        # redis info
        uri_dic = dict(
            sentinel_list=[('47.107.93.106', 36379), ('47.107.93.106', 36380), ('47.107.93.106', 36381)],
            password="vnLxIZuYgx2BOMIWGWBK5DRfYEfr!fCP",
            service_name='mymaster',
            db=0
        )
        # create redis link
        self.rds = AsyncRedisSentinelHelper(uri_dic)
        self._uuid = "9c89151d-28ec-31da-b1fc-16ca16dabe64"
        self.loop = asyncio.get_event_loop() # 主线程的事件循环
    

    async def extend_expire_time(self, expire_timeout=60):
        while not self.stop_threads:
            try:
                reader = await self.rds.writer
                lock = """
                    if (redis.call('exists', KEYS[1]) == 1) then 
                        if (redis.call('TTL', KEYS[1]) <= ARGV[2]/3) then
                            redis.call('expire', KEYS[1], tonumber(ARGV[2]));
                        end
                    end;
                """
                lock = reader.register_script(lock)
                await lock(keys=["lock:details", "December:details"], args=[self._uuid, expire_timeout])
                time.sleep(3)
            except Exception:
                break
            

    async def lock_key(self, expire_timeout=60, start=0, endpoint=-1):
        reader = await self.rds.writer
        lock = """
            if (redis.call('exists', KEYS[1]) == 1) then 
                return 0;
            end;
            redis.call('setnx', KEYS[1], ARGV[1])
            redis.call('expire', KEYS[1], tonumber(ARGV[2]));
            local result = redis.call('lrange', KEYS[2], ARGV[3], ARGV[4])
            return result;
        """
        lock = reader.register_script(lock)
        self.stop_threads = False
        t1 = SentinelThread(self.extend_expire_time, loop=self.loop) # 哨兵线程, 监控锁的过期时间
        t1.start()
        result = await lock(keys=["lock:details", "December:details"], args=[self._uuid, expire_timeout, start, endpoint])
        self.stop_threads = True
        if isinstance(result, list) and result:
            result = list(map(lambda e: feapson.loads(e), result))
        elif isinstance(result, int):
            result = []
        return result


    async def unlock_key(self):
        reader = await self.rds.writer
        unlock = """
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call("del",KEYS[1])
            else
                return 0
            end
        """
        unlock = reader.register_script(unlock)
        result = await unlock(keys=["lock:details"], args=[self._uuid])
        return result


async def main():
    try:
        testlock = lockTest()
        result = await testlock.lock_key()
        data_list = np.array(result)  # <class 'numpy.ndarray'>
        new_data_list = np.array(data_list)
        # 利用pandas的 json_normalize 对json数据进行解析，将json串解析为 DataFrame格式
        df = pd.json_normalize(new_data_list)
        writer = pd.ExcelWriter("F:/Python/spiter/DButil/data/steamspy_detail.xlsx", engine='openpyxl')
        df.to_excel(writer, index=False, sheet_name="steamdb", encoding="utf_8_sig")
        writer.save()
    finally:
        pass
        if await testlock.unlock_key():
            print("锁释放成功...")
        else:
            print("锁释放失败...") # 可能存在的情况: 执行业务逻辑代码时,锁过期了<弊端: 其他进程会拿到锁，开始执行，此时就会有两个进程都在操作共享资源>


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())