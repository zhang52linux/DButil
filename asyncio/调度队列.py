# _*_ coding: utf-8 _*_
import asyncio
import time
import requests
import execjs
import aioredis
import random
import aiohttp
import aiofiles
import httpx


class AsyncSpider():
    def __init__(self) -> None:
        pass

    # 随机获取代理
    async def get_proxy(self):
        try:
            redis = aioredis.from_url("redis://8.135.50.150:22222", db=0)
            res = await redis.hgetall("idata_proxy_pool")
            proxy_list = list(map(lambda key: key.decode("utf-8").split('|')[0], res.values()))
            random_proxy = random.choice(proxy_list)
            proxyMeta = f'http://proxy:12qwaszx@{random_proxy}:8000'
            proxies = {"http": proxyMeta, "https": proxyMeta}
            print(proxies)
            return proxies
        except Exception as e:
            self.logger.error(e)

    async def init_cookie(self):
        self.proxies = await self.get_proxy()
        dest_url = f"http://www.ceair.com/member/auth!fullLoginCheck.shtml?locationHost=www.ceair.com&_={int(time.time() * 1000)}"
        headers = {'X-Requested-With': 'XMLHttpRequest', 'User-Agent': f'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36'}
        async with aiohttp.ClientSession() as self.session:
            async with self.session.get(dest_url, headers=headers):
                seriesid = await self.get_seriesid()
                # result = await self.session.get("https://www. baidu.com/")  # 异步
                # requests.get("https://www.baidu.com/")   # 同步对比
                # time.sleep(2)
                await self.summit_seriesid(seriesid)
                

    async def get_seriesid(self):
        async with aiofiles.open('./jsScript/seriesid.js', mode='r', encoding='utf8') as file:
            Js = execjs.compile(await file.read())
        encrypt_seriesid = Js.call('encrypt_seriesid')
        return encrypt_seriesid

    async def summit_seriesid(self, seriesid):
        seriesid = await self.get_seriesid()
        url = "http://observer.ceair.com/ta.png?h=event&c=button&a=submit&l=mainProcessFlightSearch&p={\"seriesid\":\"" + seriesid + "\"}"
        headers = {'Accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
                   'Host': 'observer.ceair.com',
                   'Referer': 'http://www.ceair.com/',
                   'User-Agent': f'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36', }
        async with self.session.get(url, headers=headers):
            print(list(map(lambda item: f'{item.key}:{item.value}', list(self.session.cookie_jar))))
            await asyncio.sleep(1)



async def get_cookie():
    start = time.time()
    checkTicket = AsyncSpider()
    for _ in range(3):
        await checkTicket.get_proxy()
    end = time.time()
    print(end - start)



if __name__ == '__main__':
    # 将协程函数封装成task, wait加入调度队列中, 才能充分利用协程
    loop = asyncio.get_event_loop()
    start = time.time()
    checkTicket = AsyncSpider()
    tasks = [loop.create_task(checkTicket.init_cookie()) for _ in range(15)]
    loop.run_until_complete(asyncio.wait(tasks))
    end = time.time()
    print(end - start)

    # 直接运行多协程函数(不加入调度队列)
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(checkTicket.init_cookie())
    
