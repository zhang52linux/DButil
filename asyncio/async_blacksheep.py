# _*_ coding:utf-8 _*_
"""
*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*
Author: zhangsanyong
Date: 2022-01-06 15:06:28
LastEditors: zhangsanyong
LastEditTime: 2022-01-06 15:06:29
FilePath: /tornado/DButil/aioredis/async_blacksheep.py
Description: 
*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*
"""

import asyncio
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
from blacksheep.client import ClientSession
from loguru import logger


async def client_example():
    async with ClientSession() as client:
        response = await client.get("http://118.190.217.176/learnowo/cloudflare")

        assert response is not None
        text = await response.text()
        logger.success(text)

if __name__ == '__main__':
    asyncio.run(client_example())
