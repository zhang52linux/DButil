# _*_ coding: utf-8 _*_
import asyncio
import aioredis
import random
import aiohttp
# aioredis==2.0.0
# 修改事件循环的策略, <适用window>
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # 解决设置代理后ssl报错
# import uvloop  # pip3 install uvloop <速度更快, 适用linux>
# asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


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


async def get_Asyncproxy():
    uri_dic = dict(
        host="8.135.50.150",
        port=22222,
        db=0
    )
    redis_pool = AsyncRedis(uri_dic)
    client = await redis_pool.client()
    res = await client.hgetall("idata_proxy_pool")
    proxy_list = list(map(lambda key: key.split('|')[0], res.values()))
    proxyHost = random.choice(proxy_list)
    proxyPort = "8000"
    proxyUser = "proxy"
    proxyPass = "12qwaszx"
    proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {"host": proxyHost, "port": proxyPort, "user": proxyUser, "pass": proxyPass}
    return proxyMeta


async def main():
    proxy = await get_Asyncproxy()
    print(proxy)
    conn = aiohttp.TCPConnector(ssl=False)  # 防止ssl报错
    async with aiohttp.ClientSession(connector=conn, trust_env=True) as session:
        async with session.get('https://httpbin.org/ip', proxy=proxy) as response:
            print(await response.text())

if __name__ == "__main__":
    asyncio.run(main())
