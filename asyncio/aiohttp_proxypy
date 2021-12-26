# import asyncio
# import aiohttp
# import aioredis
# import random
#
# async def get_proxy():
#     redis = aioredis.from_url("redis://8.135.50.150:22222", db=0)
#     res = await redis.hgetall("idata_proxy_pool")
#     proxy_list = list(map(lambda key: key.decode("utf-8").split('|')[0], res.values()))
#     return random.choice(proxy_list)
#
# async def main():
#    headers = {"user-agent": f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/{random.randint(1, 999)}.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36"}
#    random_proxy = await get_proxy()
#    print(random_proxy)
#    proxy = f'http://{random_proxy}:8000'
#    print(proxy)
#    async with aiohttp.ClientSession(headers=headers) as session:
#        try:
#            async with session.get('https://httpbin.org/get', proxy=proxy) as response:
#                print(await response.text())
#        except Exception as e:
#            print('Error：', e.args)
#
# if __name__ == '__main__':
#    asyncio.run(main())


import asyncio
import aiohttp
import aioredis
import random

# proxy = 'http://124.229.230.188:8000'
# proxy_conf = {
#
# }
# ip = '124.229.228.171'
# proxy = {"http": f'{ip}:8000', "https": f'{ip}:8000'}
# proxy = "http": "http://%(user)s:%(pwd)s@%(proxy)s/"
# async def main():
#    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth('proxy','12qwaszx')) as session:
#        try:
#            async with session.get('http://httpbin.org/ip',proxy=proxy) as response:
#                print(await response.text())
#        except Exception as e:
#            print('Error：', e.args)

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
