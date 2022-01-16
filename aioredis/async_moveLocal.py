# _*_ coding:utf-8 _*_
"""
*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*
Author: zhangsanyong
Date: 2022-01-06 15:06:28
LastEditors: zhangsanyong
LastEditTime: 2022-01-06 15:06:29
FilePath: /tornado/DButil/aioredis/async_moveLocal.py
Description:
*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*
"""
import asyncio
from common.async_sentinel import AsyncRedisSentinelHelper
from common.async_redis import AsyncRedis
from loguru import logger
from collections import deque

'''
关于本实验使用lua脚本的原由:
-- Redis服务器会单线程原子性执行Lua脚本，保证Lua脚本在处理的过程中不会被任意其它请求打断
使用Lua脚本的好处:
-- 减少网络开销: 可以将多个命令用一个请求完成减少了网络往返时延
-- 原子操作: Redis会将整个脚本作为一个整体执行，中间不会被其他命令插入
-- 复用: 客户端发送的脚本会保存在Redis服务器中，其他客户端可以复用这一脚本(在All Redis Info 中可查看 used_memory_lua)
疑问?:
-- 其他客户端如何使用redis服务器上的lua脚本
'''


class MoveLocalData(object):
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
        self.loop = asyncio.get_event_loop()  # 主线程的事件循环
        self.retry_count = 3

    async def extend_expire_time(self, expire_timeout=60):
        '''
        description: 检测锁是否存在，不存在则继续等待
        存在则判断过期时间是否小于总时间的3分之2了，小于则续费时间
        param expire_timeout: 默认过期时间
        '''
        reader = await self.rds.writer
        while not self.stop_task:
            try:
                lock = """
                    if (redis.call('exists', KEYS[1]) == 1) then
                        if (redis.call('TTL', KEYS[1]) <= ARGV[2]/3) then
                            redis.call('expire', KEYS[1], tonumber(ARGV[2]));
                        end
                    end;
                """
                lock = reader.register_script(lock)
                await lock(keys=["lock:details", "January:details"], args=[self._uuid, expire_timeout])
                await asyncio.sleep(3)
            except BaseException:
                break
    
    async def len_key(self):
        '''
            - description: 求key的长度
        '''
        reader = await self.rds.writer
        lock = """
            if (redis.call('exists', KEYS[1]) == 1) then
                return redis.call('llen', KEYS[1])
            else
                return 0
            end;
        """
        lock = reader.register_script(lock)
        result = await lock(keys=["January:details"])
        return result
    
    # 装饰器做安全函数
    def safe_function(func):
        async def wrapper(self, *arg, **kwargs):
            for retry in range(self.retry_count):
                try:
                    result = await func(self, *arg, **kwargs)
                    return result
                except BaseException:
                    logger.error("第{}次重试, error: {}".format((retry + 1), e))
                    self.total_retry_count += 1
                    await asyncio.sleep(2)
            return deque()
        return wrapper
        
    @safe_function
    async def lock_key(self, expire_timeout=60, startpoint=0, endpoint=100):
        '''
        description: 判断锁是否存在，存在则判断是否是本节点的锁，是则直接取数据, 锁不存在则加锁然后取数据
        param expire_timeout: 默认超时时间
        param startpoint: 列表遍历开始点
        param endpoint: 列表遍历结束点
        '''
        reader = await self.rds.writer
        lock = """
            if (redis.call('exists', KEYS[1]) == 1) then
                if redis.call('get', KEYS[1]) == ARGV[1] then
                    local result = redis.call('lrange', KEYS[2], ARGV[3], ARGV[4])
                    return result;
                else
                    return 0
                end
            else
                redis.call('setnx', KEYS[1], ARGV[1])
                redis.call('expire', KEYS[1], tonumber(ARGV[2]));
                local result = redis.call('lrange', KEYS[2], ARGV[3], ARGV[4])
                return result;
            end
        """
        lock = reader.register_script(lock)
        self.stop_task = False
        asyncio.create_task(self.extend_expire_time())  # 哨兵协程, 监控锁的过期时间
        result = await lock(keys=["lock:details", "January:details"], args=[self._uuid, expire_timeout, startpoint, endpoint])
        self.stop_task = True
        if isinstance(result, list) and result:
            result = deque(map(lambda e: e, result))
        elif isinstance(result, int):
            logger.error("资源被锁! 等待2s......")
            raise ValueError
        return result

    async def unlock_key(self):
        '''
            - description: 释放key的锁
        '''
        reader = await self.rds.writer
        unlock = """
            if (redis.call('exists', KEYS[1]) == 1) then
                if redis.call('get', KEYS[1]) == ARGV[1] then
                    return redis.call("del",KEYS[1])
                else
                    return 0
                end
            else
                return 1
            end
        """
        unlock = reader.register_script(unlock)
        result = await unlock(keys=["lock:details"], args=[self._uuid])
        return result



async def splitArray(internal:int=1000, len_list:int=0):
    area_detail = dict()
    start = 0
    end = -1
    temp_total = len_list
    index = 1
    while temp_total > internal:
        start = end + 1
        end = internal + end
        temp_total -= internal
        area_detail[index] =[start, end]
        index += 1
    start = end + 1
    area_detail[index] =[start, len_list]
    return area_detail


async def main():
    try:
        fuckdata = MoveLocalData()
        uri_dic = dict(
            host="127.0.0.1",
            port=6379,
            db=0
        )
        redis_pool = AsyncRedis(uri_dic)
        len_list = await fuckdata.len_key()
        area_detail = await splitArray(len_list=len_list)
        for key, area in area_detail.items():
            redis_lcoal = await redis_pool.client()
            logger.success(f"开始读取第{key}部分数据({area[0]}--{area[1]})")
            data_list = await fuckdata.lock_key(startpoint=area[0], endpoint=area[1])
            if isinstance(data_list, deque) and data_list:
                logger.success(f"成功获取到第{key}部分数据, 正在转存至本地...")
                await redis_lcoal.lpush("test:details", *data_list)
                await fuckdata.unlock_key()
            else:
                logger.error(f"result:0, 本次操作没有获取到数据!!!")
    except BaseException as e:
        logger.error(e)
        await fuckdata.unlock_key()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
