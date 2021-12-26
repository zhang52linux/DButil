import httpx
import asyncio
import random
import aioredis
# import asyncio
# asyncio.wait()
# await 可等待对象
# client = httpx.Client(http2=True, verify=False)  # 相当于requests.Session()


async def get_proxy():
    redis = aioredis.from_url("redis://8.135.50.150:22222", db=0)
    res = await redis.hgetall("idata_proxy_pool")
    data_list = list(map(lambda key: key.decode("utf-8").split('|')[0], res.values()))
    return random.choice(data_list)


async def main():
    headers = {"user-agent": f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/{random.randint(1, 999)}.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36"}
    random_proxy = await get_proxy()
    print(random_proxy)
    search_url = f'https://s.taobao.com/search?initiative_id=staobaoz_20210817&ie=utf8&spm=a21bo.21814703.201856-taobao-item.2&sourceId=tb.index&search_type=item&ssid=s5-e&commend=all&imgfile=&q=联想y9000&suggest=history_1&_input_charset=utf-8&wq=biyunt&suggest_query=biyunt&source=suggest&bcoffset=4&p4ppushleft=%2C48&s=44&data-key=s&data-value=88'
    proxies = {'http:': f'http://proxy:12qwaszx@{random_proxy}:8000', 'https:': f'http://proxy:12qwaszx@{random_proxy}:8000'}
    # print(httpx.get(f"https://httpbin.org/get", proxies=proxies).json())
    # print(requests.get(f"http://httpbin.org/get", proxies=proxies).json())
    # pip install httpx[http2]  支持http2, 高并发可以使用
    # httpx.request( method , url , * , params=None , content=None , data=None , files=None , json=None , headers=None , cookies=None , auth=None , proxies=None , timeout=Timeout(timeout=5.0) ) , follow_redirects=False , verify=True , cert=None , trust_env=True )
    async with httpx.AsyncClient(proxies=proxies, http2=True) as client:
        # response = await client.request('GET', f"https://httpbin.org/get", headers=headers)
        response = await client.request('GET', search_url, headers=headers)
        # pattern = re.compile('window.sohu_user_ip="(.*?)"')
        # current_ip = pattern.search(response.text).group(1)
        print("当前IP为: " + response.text)
        client.cookies.update({"1P_JAR": "2021-11-02-01"})   # 支持与requests一样的update更新cookie的方法
        print(client.cookies)  # <class 'httpx.Cookies'>
        print(dict(client.cookies))   # 可以直接通过dict转化                       # 新机器: B6zyNqP1dA3rjIV8Pj3I
        # print(type(dict(client.cookies)))
        # requests.utils.dict_from_cookiejar(client.cookies)
        # print(type(client.cookies))  # <class 'httpx.Cookies'>
        # print(response.http_version)   # HTTP/2


asyncio.run(main())
