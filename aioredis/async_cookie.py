# _*_ coding:utf-8 _*_
"""
*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*
Author: zhangsanyong
File Name: browser.py
Create Date: 2022/01/04 10:38
Description:
*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*
"""
import asyncio
import aiohttp


class FuckCfCookie:
    def start(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.run())

    async def run(self):
        result = await self.get_cf_cookie()
        headers = {
            "User-Agent": result["ua"],
            "cookie": result["cookie"]
        }
        proxy = "http://proxy:12qwaszx@{}:8000".format(result["ip"])
        async with aiohttp.ClientSession() as session:
            async with session.get("https://steamdb.info/", headers=headers, proxy=proxy) as resp:
                print(await resp.text())

    @staticmethod
    async def get_cf_cookie():
        async with aiohttp.ClientSession() as session:
            async with session.get("http://120.53.247.246/learnowo/cloudflare") as resp:
                result = await resp.json()
                return result


if __name__ == '__main__':
    FuckCfCookie().start()
