# _*_ coding:utf-8 _*_
"""
*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*
Author: zhangsanyong
Date: 2022-01-06 15:06:28
LastEditors: zhangsanyong
LastEditTime: 2022-01-06 15:06:29
FilePath: /tornado/DButil/aioredis/async_cookie.py
Description: 请求获取cf5s盾的cookie(git rm -rf --cached aioredis/async_cookie.py)
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
        total_count = 1000
        pass_count = 0
        async with aiohttp.ClientSession() as session:
            for i in range(total_count):
                try:
                    result = await self.get_cf_cookie()
                    headers = {
                        "User-Agent": result["ua"],
                        "cookie": result["cookie"]
                    }
                    proxy = "http://proxy:12qwaszx@{}:8000".format(result["ip"])
                    async with session.get("https://steamdb.info/", headers=headers, proxy=proxy, timeout=timeout) as resp:
                        assert resp.status == 200
                        logger.info(resp.status)
                        pass_count += 1
                        await asyncio.sleep(0.3)
                except BaseException:
                    logger.error("error")
                    await asyncio.sleep(3)
            print("测试案例{}个,成功{}个,失败{}个,通过率{:.4%}".format(total_count, pass_count, (total_count - pass_count), (pass_count / total_count)))

    @staticmethod
    async def get_cf_cookie():
        async with aiohttp.ClientSession() as session:
            async with session.get("http://120.53.247.246/learnowo/cloudflare") as resp:
                result = await resp.json()
                return result


if __name__ == '__main__':
    FuckCfCookie().start()
