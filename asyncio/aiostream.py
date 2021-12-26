import asyncio
from aiostream import stream
# itertools的异步版本
from aiostream.stream import list as alist


async def generate_numbers(n):
    for x in range(n):
        yield x


async def generate_numbers1(n):
    for x in range(n):
        yield x


async def consume_some_numbers(n, m):
    zs = stream.take(generate_numbers(n), m)  # m  参数表示将这个1-n的生成器分成几块
    t = await stream.list(zs)  # 返回切片的结果列表
    print(t)


async def test_pass():
    x = generate_numbers(10)
    x1 = generate_numbers1(10)
    print(type(x))  # async_generator
    async for it in await alist(x):
        print(it)
    # async for item in x1:
    #     print(type(item))
    #     print(item)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_pass())
    # loop.run_until_complete(consume_some_numbers(10, 10))
