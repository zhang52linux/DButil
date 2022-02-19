# _*_ coding: utf-8 _*_
import asyncio
import aioredis.sentinel
import feapson


'''
基于主从复制原理实现:
    - 好处:
        - 数据冗余, 实现同一个字段在多个数据库中存在
        - 负载均衡, 主从复制实现了读写分离
        - 故障恢复, 主节点挂掉后, 从节点补上
    - 相关命令:
        - slaveof host port, 修改当前服务器的主服务器, 并丢弃原来的数据集, 开始复制新的主的数据
        - slaveof no one, 使当前服务器成为主服务器<从原来的集群中脱离>, 不会丢弃原来的数据集, 如果原来的主节点没有宕机, 则该节点还是会变成从节点<回到原来的集群>
        - 如果有哨兵的存在, 那么当一个主节点坏掉后, slaveof no one 是不会真正起到作用的<不久就会被同步到新的主节点上>
节点连接master后:
    - 1、首先会对主节点进行一次全量复制
    - Full resync from master: c0d0a98b944757469adcfab7dc0162f65af82d02:990491445
'''


class AsyncRedisSentinelHelper():
    def __init__(self, config: dict):
        self.service_name = config["service_name"]
        self.password = config.get("password", None)
        self.db = config["db"]
        self.sentinel = aioredis.sentinel.Sentinel(config["sentinel_list"])
        self.init_master_redis_connect()
        self.init_slave_redis_connect()

    async def get_master_redis(self) -> tuple:
        info = await self.sentinel.discover_master(self.service_name)
        return info

    async def get_slave_redis(self) -> tuple:
        info = await self.sentinel.discover_slaves(self.service_name)
        return info

    @property
    async def reader(self):
        return self.slave

    @property
    async def writer(self):
        return self.master

    def init_master_redis_connect(self):
        self.master = self.sentinel.master_for(
            service_name=self.service_name,
            socket_timeout=600,
            retry_on_timeout=True,
            socket_keepalive=True,
            decode_responses=True,
            password=self.password,
            max_connections=1000,
            encoding='utf-8',
            db=self.db
        )

    def init_slave_redis_connect(self):
        self.slave = self.sentinel.slave_for(
            service_name=self.service_name,
            socket_timeout=600,
            retry_on_timeout=True,
            socket_keepalive=True,
            password=self.password,
            max_connections=1000,
            decode_responses=True,
            encoding='utf-8',
            db=self.db
        )

    async def set_key(self, key, value):
        async with self.master.pipeline(transaction=True) as pipe:
            ok1 = await (pipe.set(key, value).execute())
        return ok1

    async def get_key(self, key):
        async with self.master.pipeline(transaction=True) as pipe:
            ok1 = await (pipe.get(key).execute())
        return ok1

    async def save_rpush_data(self, key: str, value: list) -> list:
        result = await self.rpush_data(key, value=value)
        if isinstance(result, list) and result:
            result = result[0]
        return result

    async def read_lrange_data(self, key: str, start: int, end: int) -> list:
        result = await self.lrange_data(key, start=start, end=end)
        if isinstance(result, list) and result:
            result = result[0]
        return result

    async def read_blpop_data(self, key: str, timeout=None) -> tuple:
        result = await self.blpop_data(key, timeout=timeout)
        if isinstance(result, list) and result:
            result = result[0]
        return result

    async def rpush_data(self, key: str, value: list) -> bool:
        async with self.master.pipeline(transaction=True) as pipe:
            value = list(map(lambda e: feapson.dumps(e), value))
            ok1 = await (pipe.rpush(key, *value).execute())
        return ok1

    async def lrange_data(self, key: str, start: int, end: int) -> bool:
        async with self.slave.pipeline(transaction=True) as pipe:
            ok1 = await (pipe.lrange(key, start=start, end=end).execute())
        return ok1

    async def blpop_data(self, key: str, timeout=None) -> bool:
        async with self.slave.pipeline(transaction=True) as pipe:
            ok1 = await (pipe.blpop(key, timeout=timeout).execute())
        return ok1


async def main():
    # redis info
    uri_dic = dict(
        sentinel_list=[('1.15.237.25', 36379), ('1.15.237.25', 36380), ('1.15.237.25', 36381)],
        password="vnLxIZuYgx2BOMIWGWBK5DRfYEfr!fCP",
        service_name='mymaster',
        db=0
    )
    # create redis link
    rsh = AsyncRedisSentinelHelper(uri_dic)
    # test
    result = await rsh.rpush_data("test", [520, 13, 14])
    print(result)


if __name__ == '__main__':
    asyncio.run(main())
