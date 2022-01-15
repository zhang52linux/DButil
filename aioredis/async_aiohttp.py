# _*_ coding:utf-8 _*_
"""
*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*
Author: zhangsanyong
Date: 2022-01-06 15:06:28
LastEditors: zhangsanyong
LastEditTime: 2022-01-06 15:06:29
FilePath: /tornado/DButil/aioredis/async_aiohttp.py
Description: 请求获取cf5s盾的cookie(git rm -rf --cached aioredis/async_aiohttp.py)
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

'''
vrrp概念:
- VRRP(Virtual Router Redundancy Protocol，虚拟路由器冗余协议)
vrrp作用:
- 将可以承担网关功能的一组路由器加入到备份组中，形成一台虚拟路由器，这样主机的网关设置成虚拟网关，就能够实现冗余
- 备份组: 局域网内(各es实例的私网IP)的一组路由器(相当于阿里云的高可用虚拟IP绑定两个个ecs实例<最多两个>)
vrrp原理:
- 功能上相当于一台虚拟路由器
- 虚拟路由器具有IP地址，称为虚拟IP地址
- 网络内的主机通过这个虚拟路由器与外部网络(相当于阿里云的弹性公网IP)进行通信
- 路由器根据优先级，选举出Master路由器，承担网关功能
vrrp应用:
- keepalived
'''


class FuckCfCookie:
    pass_count = 0

    def start(self):
        self.retry_count = 5
        self.total_retry_count = 0
        self.total_count = 1000
        asyncio.run(self.main())
    
    
    # 装饰器做安全函数
    def safe_function(func):
        async def wrapper(self, *arg, **kwargs):
            for retry in range(self.retry_count):
                try:
                    await func(self, *arg, **kwargs)
                    return True
                except (asyncio.exceptions.TimeoutError, AssertionError, aiohttp.client_exceptions.ClientProxyConnectionError) as e:
                    logger.error("第{}次重试, error: {}".format((retry + 1), e))
                    self.total_retry_count += 1
                    await asyncio.sleep(2)
        return wrapper

    async def main(self):
        start_time = time.time()
        timeout = aiohttp.ClientTimeout(total=3)
        async with aiohttp.ClientSession() as session:
            for _ in range(self.total_count):
                await self.run(session, timeout)
        end_time = time.time()
        logger.success("测试案例{}个,成功{}个,失败{}个,重试{}个,通过率{:.4%},用时:{}, 平均{}s".format(self.total_count, self.pass_count, (self.total_count - self.pass_count), self.total_retry_count, (self.pass_count / self.total_count), (end_time - start_time), round((end_time - start_time)/(self.pass_count + self.total_retry_count), 2)))
    
    @safe_function
    async def run(self, session, timeout):
        result = await self.get_cf_cookie()
        headers = {
            "User-Agent": result["ua"],
            "cookie": result["cookie"]
        }
        proxy = "http://proxy:12qwaszx@{}:8000".format(result["ip"])
        async with session.get("https://steamdb.info/", headers=headers, proxy=proxy, timeout=timeout) as resp:
            assert resp.status == 200
            logger.info(resp.status)
            self.pass_count += 1
    

    @staticmethod
    async def get_cf_cookie():
        async with aiohttp.ClientSession() as session:
            async with session.get("http://120.53.247.246/learnowo/cloudflare") as resp:
                result = await resp.json()
                return result


if __name__ == '__main__':
    FuckCfCookie().start()
