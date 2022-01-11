# _*_ coding: utf-8 _*_
import asyncio
import aiohttp
import aioredis
import random


# 修改事件循环的策略，不能放在协程函数内部，这条语句要先执行
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # 解决设置代理后ssl报错


async def get_Asyncproxy():
    redis = aioredis.from_url("redis://8.135.50.150:22222", db=0)
    res = await redis.hgetall("idata_proxy_pool")
    proxy_list = list(map(lambda key: key.decode("utf-8").split('|')[0], res.values()))
    proxyHost = random.choice(proxy_list)
    proxyPort = "8000"
    proxyUser = "proxy"
    proxyPass = "12qwaszx"
    proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {"host": proxyHost, "port": proxyPort, "user": proxyUser, "pass": proxyPass}
    return proxyMeta


async def main():
    proxy = await get_Asyncproxy()
    print(proxy)
    # proxy_auth = aiohttp.BasicAuth('proxy', '12qwaszx')
    # async with aiohttp.ClientSession(auth=aiohttp.BasicAuth('proxy', '12qwaszx')) as session:
    #     async with session.get('http://httpbin.org/ip', proxy=proxy, proxy_auth=proxy_auth) as response:
    #         print(await response.text())

    # 有代理验证的可以放在BasicAuth里面, 也可以直接放在代理里
    proxy_auth = aiohttp.BasicAuth('UlmVai5dyw', 'PaV4w3LAIA')  # Proxy Authentication Required
    conn = aiohttp.TCPConnector(ssl=False)  # 防止ssl报错
    async with aiohttp.ClientSession(connector=conn, trust_env=True) as session:
        async with session.get('https://httpbin.org/ip', proxy=proxy) as response:
            print(await response.text())

    # conn = aiohttp.TCPConnector(ssl=False)  # 防止ssl报错
    # async with aiohttp.ClientSession(connector=conn, trust_env=True) as session:
    #     async with session.get('https://httpbin.org/ip', proxy=proxy) as response:
    #         print(await response.text())


if __name__ == '__main__':
    asyncio.run(main())
