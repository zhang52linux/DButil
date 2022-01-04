# _*_ coding:utf-8 _*_
"""
*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*
Author: zhangsanyong
File Name: browser.py
Create Date: 2022/01/04 10:38
Description:
*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*
"""
import json
import aiohttp
import asyncio
import asyncio
from common.async_sentinel import AsyncRedisSentinelHelper
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # 解决设置代理后ssl报错

class BrowserLoginExcutor:
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

    def start(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.run())
    
    async def run(self):
        redis_pool = await self.rds.writer
        cookie, proxy = await self.get_cookie(redis_pool=redis_pool)
        self.base_headers = {'user-agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:91.0) Gecko/20100101 Firefox/91.0'}
        self.proxy = "http://proxy:12qwaszx@{}:8000".format(proxy)
        self.base_headers["cookie"] = cookie
        timeout = aiohttp.ClientTimeout(total=5)
        async with aiohttp.ClientSession() as session:
            async with session.get("https://steamdb.info/", headers=self.base_headers, proxy=self.proxy, timeout=timeout) as resp:
                print(await resp.text())


    @staticmethod
    async def get_cookie(redis_pool):
        refresh_key = '{}_refresh_cookie_proxy'.format('steamdb')
        await redis_pool.sadd(refresh_key, '1')
        key = '{}cookie_proxy'.format('steamdb')
        cookie = await redis_pool.srandmember(key, 1)
        if not cookie:
            return None, None
        data = json.loads(cookie[0])
        return data['cookies'], data['ip']


if __name__ == '__main__':
    BrowserLoginExcutor().start()