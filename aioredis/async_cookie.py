# _*_ coding:utf-8 _*_
"""
*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*
Author: zhangsanyong
Date: 2022-01-06 15:06:28
LastEditors: zhangsanyong
LastEditTime: 2022-01-06 15:06:29
FilePath: /tornado/DButil/aioredis/async_cookie.py
Description: 请求获取cf5s盾的cookie
*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*
"""
import asyncio
import aiohttp
from loguru import logger


class FuckCfCookie:
    def start(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.run())

    async def run(self):
        timeout = aiohttp.ClientTimeout(total=3)
        async with aiohttp.ClientSession() as session:
            for i in range(1000):
                try:
                    result = await self.get_cf_cookie()
                    headers = {
                        "User-Agent": result["ua"],
                        "cookie": result["cookie"]
                    }
                    proxy = "http://proxy:12qwaszx@{}:8000".format(result["ip"])
                    async with session.get("https://steamdb.info/", headers=headers, proxy=proxy, timeout=timeout) as resp:
                        logger.info(resp.status)
                        await asyncio.sleep(0.2)
                except BaseException:
                    logger.error("error")
                    await asyncio.sleep(3)

    @staticmethod
    async def get_cf_cookie():
        async with aiohttp.ClientSession() as session:
            async with session.get("http://120.53.247.246/learnowo/cloudflare") as resp:
                result = await resp.json()
                return result


if __name__ == '__main__':
    FuckCfCookie().start()
    