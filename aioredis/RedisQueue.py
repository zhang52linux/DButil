# _*_ coding: utf-8 _*_
import asyncio
from common.async_sentinel import AsyncRedisSentinelHelper
import time

# todo redis实现简单的消息队列<没有优先级>


class RedisQueue(object):
    def __init__(self, name, namespace='queue', **uri_dic):
        self.redis_client = AsyncRedisSentinelHelper(uri_dic)
        self.key = '%s:%s' % (namespace, name)

    async def qsize(self):
        return await self.redis_client.llen(self.key)  # 返回队列里面list内元素的数量

    async def put(self, item):
        await self.redis_client.save_rpush_data(self.key, [item])  # 添加新元素到队列最右方

    async def get(self, timeout=None):
        # 返回队列第一个元素，如果为空则等待至有元素被加入队列(超时时间阈值为timeout，如果为None则一直等待<处于阻塞状态>)
        item = await self.redis_client.read_blpop_data(self.key, timeout=timeout)
        return item


async def input():
    uri_dic = dict(
        sentinel_list=[('47.107.93.106', 36379), ('47.107.93.106', 36380), ('47.107.93.106', 36381)],
        password="vnLxIZuYgx2BOMIWGWBK5DRfYEfr!fCP",
        service_name='mymaster',
        db=0
    )
    q = RedisQueue('rq', **uri_dic)  # 新建队列名为rq
    for i in range(5):
        await q.put(i)
    print("input.py: data {} enqueue {}".format(i, time.strftime("%c")))
    time.sleep(1)


async def output():
    uri_dic = dict(
        sentinel_list=[('47.107.93.106', 36379), ('47.107.93.106', 36380), ('47.107.93.106', 36381)],
        password="vnLxIZuYgx2BOMIWGWBK5DRfYEfr!fCP",
        service_name='mymaster',
        db=0
    )
    q = RedisQueue('rq', **uri_dic)  # 新建队列名为rq
    result = await q.get()
    print(result)


async def _test():
    # while True:
    #     await input()
    #     await asyncio.sleep(1)
    # await input()
    await output()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(_test())
