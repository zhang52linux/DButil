# _*_ coding:utf-8 _*_
"""
*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*
Author: zhangsanyong
Date: 2022-01-06 15:06:28
LastEditors: zhangsanyong
LastEditTime: 2022-01-06 15:06:29
FilePath: /tornado/DButil/aioredis/async_playwright.py
Description: nohup python -u "/home/ubuntu/tornado/DButil/aioredis/async_playwright.py" > /home/ubuntu/logs/supervisor/playwright_server/tornado_playwright_server.log 2>&1 &
*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*
"""
import datetime
import re
import json
import aiohttp
import asyncio
from playwright.async_api import async_playwright
from loguru import logger
from common.async_sentinel import AsyncRedisSentinelHelper

PROXY_URL = 'http://api01.idataapi.cn:8000/proxyip?status=ok&apikey=yPCg6ig17c811fD2EzCtm0oz7FE8BtCeS0vQxd9n6S4ZqBO9fvMJZFgR2GaOxZoT'


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
        asyncio.run(self.run())

    async def run(self):
        redis_pool = await self.rds.writer
        while True:
            try:
                redis_key = 'steamdbcookie_proxy'
                refresh_key = 'steamdb_refresh_cookie_proxy'
                status = await redis_pool.exists(refresh_key)
                if status:
                    ignore_default_args = ['--enable-automation']
                    user_agent = 'Mozilla/5.0 (X11; Linux x86_64; rv:91.0) Gecko/20100101 Firefox/91.0'
                    proxy_ip = await self.get_proxy()   # 得到用于请求cookie的代理ip
                    proxy = {"server": "http://{}:8000/".format(proxy_ip), "username": "proxy", "password": "12qwaszx"}
                    # proxy = None
                    playwright = await async_playwright().start()
                    browser = await playwright.firefox.launch(ignore_default_args=ignore_default_args, headless=True, proxy=proxy)
                    context = await browser.new_context()
                    page = await context.new_page()
                    await page.set_extra_http_headers({'User-Agent': user_agent})
                    try:
                        await page.goto("https://steamdb.info/")
                        await self.wait_for_frame(context)
                        await asyncio.sleep(1)
                        # Press Enter
                        cookies = await context.cookies()
                        cookies = {i['name']: i['value'] for i in cookies}
                        cookies = ';'.join(['{}={}'.format(k, v) for k, v in cookies.items()])
                        cookies = re.search('cf_clearance.+?0-150', cookies).group()
                        result = {
                            'ip': proxy_ip,
                            'cookies': cookies
                        }
                        logger.success(f'{datetime.datetime.now()} redis_pool.delete(redis_key)')
                        await redis_pool.delete(redis_key)    # 先删除之前的redis_key
                        logger.success(f'{datetime.datetime.now()} redis_pool.sadd(redis_key, json.dumps(result))')
                        await redis_pool.sadd(redis_key, json.dumps(result))  # 将获取到的结果存入redis_key
                        await redis_pool.delete(refresh_key)  # 最后删除refresh_key, 完成一次cookie的更新
                        await asyncio.sleep(60)
                    except Exception as e:
                        logger.error(e)
                    finally:
                        await page.close()
                        await context.close()
                        await browser.close()
                else:
                    logger.info(f'{datetime.datetime.now()} no flag: await asyncio.sleep(5)')
                    await asyncio.sleep(5)
            except BaseException:
                logger.error(f'{datetime.datetime.now()} error: await asyncio.sleep(5)')
                await asyncio.sleep(5)

    async def get_proxy(self, retry=3):
        try:
            async with aiohttp.ClientSession() as session:
                resp = await session.get(PROXY_URL)
                data = await resp.json()
                proxies = [(dt["proxy_use_time"], dt["proxy_ip"]) for dt in data["data"]]
                return sorted(proxies, key=lambda i: i[0])[0][-1]
        except Exception:
            if retry:
                return await self.get_proxy(retry - 1)
            return None

    async def wait_for_frame(self, context):
        for i in range(6):
            cookies = await context.cookies()
            cookies = {i['name']: i['value'] for i in cookies}
            if 'cf_clearance' in cookies:
                break
            await asyncio.sleep(2)


if __name__ == '__main__':
    BrowserLoginExcutor().start()
