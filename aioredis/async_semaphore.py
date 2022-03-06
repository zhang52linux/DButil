# _*_ coding: utf-8 _*_
import time
import asyncio
from common.async_sentinel import AsyncRedisSentinelHelper
from threading import Thread
import feapson
# 自定义哨兵线程信号量的过期时间


class SentinelThread(Thread):
    def __init__(self, func, args=(), loop=None):
        super(SentinelThread, self).__init__()
        self.func = func
        self.args = args
        self.loop = loop    # 每个线程有个事件循环,

    def run(self):
        self.loop.create_task(self.func(*self.args))  # 在当前事件循环中创建一个任务, 任务会被放到调度器中


# todo 实现信号量测试: 信号量是一种锁，用于限制资源访问的进程数<两个要素: 拥有者zset, 计数器>
# todo 信号量的使用场景/作用: 信号量可以做分布式限流
# 实现步骤: 申请信号量<相当于往拥护者的zset集合中插入新的元素><元素类型为字典{信号拥有者身份identifier: 当前时间戳score}>
# 之后按照score取排名， 如果排名超过信号量设置的最大值, 则取消信号量的分配
# 信号量的移除, 直接将信号量集合中的指定元素删除
# todo 问题: 如果是按照上面的思路会存在一个问题, 如果现在有两台服务器, A服务器的系统时间比B服务器的快,
# todo 那么B服务器会分配到更多的信号量<因为它获取信号量的时间比A快, 在A申请的时候, B可能已经进入到信号量集合中了>
# todo 信号量的过期时间: 设置一个过期时间, 定期检查集合中过期的元素，并将其移除<将(now - timeout)范围以下的元素全部删除>
# 如果有个计数信号量定义的值是1，那么它其实就等同于 mutex (互斥锁)
class semaphoreTest(object):
    def __init__(self) -> None:
        super().__init__()
        # redis info
        uri_dic = dict(
            sentinel_list=[('xx.xx.xx.xx', 6379), ('xx.xx.xx.xx', 6379), ('xx.xx.xx.xx', 6379)],
            password="password",
            service_name='mymaster',
            db=0
        )
        # create redis link
        self.rds = AsyncRedisSentinelHelper(uri_dic)
        self._identifier = "9c89151d-28ec-31da-b1fc-16ca16dabe64"
        self._timeout = 60
        self._limit = 10
        self._semaphore_name = "sema:details"
        self.loop = asyncio.get_event_loop()  # 主线程的事件循环

    # 1、第一步清除过期的信号量持有者
    # 2、直接判断信号持有者中是否还有剩余的信号量可用
    # 3、有就直接分配, 没有就返回

    async def acquire_semaphore(self):
        writer = await self.rds.writer
        now = time.time()
        lua_script = """
            redis.call('zremrangebyscore', KEYS[1], '-inf', ARGV[1])
            if redis.call('zcard', KEYS[1]) < tonumber(ARGV[2]) then
                redis.call('zadd', KEYS[1], ARGV[3], ARGV[4])
                return ARGV[4]
            end
        """
        script_acquire = writer.register_script(lua_script)
        iden = await script_acquire(keys=[self._semaphore_name], args=[now - self._timeout, self._limit, now, self._identifier])
        if iden:
            return iden
        return None

    # 有序集合移除对应的是集合内的key,所以我们不需要watch，直接删除即可<>
    async def release_semaphore(self):
        writer = await self.rds.writer
        lua_script = """
            return redis.call('zrem', KEYS[1], ARGV[1])
        """
        script_release = writer.register_script(lua_script)
        status_code = await script_release(keys=[self._semaphore_name], args=[self._identifier])
        return status_code

    # 刷新信号量, 判断指定的iden分数是否存在<todo: 如果是在单机上, 这种做法是到不到效果的, 适合用在不断需要获取信号量的场景, 比如服务器的入站流量<源源不断>>
    # 源源不断的流量进来需要不断获取信号量, 也会不断的刷新掉原有的信号量<这个时候, 如果原有的操作还没结束, 原有的信号量被刷掉，就会导致无法达到限流的效果>
    # 因此需要保持原有信号量的操作
    async def refresh_fair_semaphore(self):
        writer = await self.rds.writer
        while not self.stop_threads:
            try:
                lua_script = """
                    if redis.call('zscore', KEYS[1], ARGV[1]) then
                        return redis.call('zscore', KEYS[1], ARGV[2], ARGV[1])
                    end
                """
                script_refresh = writer.register_script(lua_script)
                await script_refresh(keys=[self._semaphore_name], args=[self._identifier, time.time()])
                time.sleep(3)
            except BaseException:
                break

    async def get_game_details(self, start=0, endpoint=1500):
        identifier = await self.acquire_semaphore()  # 获取信号量
        writer = await self.rds.writer
        if identifier:
            lock = """
                if (redis.call('exists', KEYS[1]) == 0) then
                    return 0;
                end;
                local result = redis.call('lrange', KEYS[1], ARGV[1], ARGV[2])
                return result;
            """
            lock = writer.register_script(lock)
            self.stop_threads = False
            t1 = SentinelThread(self.refresh_fair_semaphore, loop=self.loop)  # 哨兵线程, 监控信号量的过期时间
            t1.start()
            result = await lock(keys=["December:details"], args=[start, endpoint])  # 拿到信号量后就可以直接进行操作
            self.stop_threads = True
            if isinstance(result, list) and result:
                result = list(map(lambda e: feapson.loads(e), result))
            elif isinstance(result, int):
                result = []
            return result


async def main():
    sema = semaphoreTest()
    result = await sema.get_game_details()
    print(result)
    status_code = await sema.release_semaphore()  # 释放信号量
    if status_code:
        print("删除信号量成功!")
    else:
        print("删除信号量失败...")


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
