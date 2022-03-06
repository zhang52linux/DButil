# _*_ coding: utf-8 _*_
import asyncio
import aioredis
import random
import aiohttp
import uvloop  # pip3 install uvloop <速度更快, 适用linux>
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


class AsyncRedis(object):

    def __init__(self, config):
        self.default_db = config["db"]
        self.config = dict(
            url="redis://{0}:{1}".format(config['host'], config['port']),
            password=config.get("password", None),
            socket_timeout=600,
            retry_on_timeout=True,
            socket_keepalive=True,
            decode_responses=True,
            encoding="utf-8",   # 设置编码, 返回后的结果会自动 decode
            max_connections=1000,
            db=config["db"]
        )
        self.client_map = {}

    @staticmethod
    async def _isclosed(client):
        try:
            return "PONG" != await client.ping()
        except BaseException:
            return True

    async def get_async_redis_client_pool(self, db):
        if db not in self.client_map or await self._isclosed(self.client_map[db]):
            pool = aioredis.ConnectionPool.from_url(**self.config)
            self.client_map[db] = aioredis.Redis(connection_pool=pool)
        return self.client_map[db]

    async def client(self, db=None) -> aioredis.Redis:
        return await self.get_async_redis_client_pool(self.default_db if db is None else db)
