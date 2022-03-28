# _*_ coding:utf-8 _*_
import aioredis.sentinel
import feapson


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
