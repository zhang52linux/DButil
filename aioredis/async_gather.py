# _*_ coding:utf-8 _*_
"""
*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*
Author: zhangsanyong
Date: 2022-01-06 15:06:28
LastEditors: zhangsanyong
LastEditTime: 2022-01-06 15:06:29
Description: 请求获取cf5s盾的cookie(git rm -rf --cached aioredis/async_cookie.py)
*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*
"""
import asyncio
import aiohttp

import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
from loguru import logger
import time
import warnings
warnings.filterwarnings('ignore')


class FuckCfCookie:
    pass_count = 0

    def start(self):
        asyncio.run(self.main())

    async def main(self):
        total_count = 28
        start_time = time.time()
        timeout = aiohttp.ClientTimeout(total=3)
        async with aiohttp.ClientSession() as session:
            await asyncio.gather(*[self.run(session, timeout) for i in range(total_count)])
        end_time = time.time()
        logger.success("测试案例{}个,成功{}个,失败{}个,通过率{:.4%},用时:{}".format(total_count, self.pass_count, (total_count - self.pass_count), (self.pass_count / total_count), (end_time - start_time)))

    async def run(self, session, timeout):
        try:
            result = await self.get_cf_cookie()
            headers = {
                "User-Agent": result["ua"],
                "cookie": result["cookie"]
            }
            proxy = "http://xxxx:xxxxxxxxx@{}:8000".format(result["ip"])
            async with session.get("https://steamdb.info/", headers=headers, proxy=proxy, timeout=timeout) as resp:
                assert resp.status == 200
                logger.info(resp.status)
                self.pass_count += 1
            await asyncio.sleep(1)
        except BaseException:
            logger.error("error")

    @staticmethod
    async def get_cf_cookie():
        async with aiohttp.ClientSession() as session:
            async with session.get("http://xx.xx.xx.xx/learnowo/cloudflare") as resp:
                result = await resp.json()
                return result


if __name__ == '__main__':
    FuckCfCookie().start()
