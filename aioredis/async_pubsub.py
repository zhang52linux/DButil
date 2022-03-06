import asyncio
import async_timeout
import aioredis
from common.async_redis import AsyncRedis

STOPWORD = "STOP"


async def reader(channel: aioredis.client.PubSub):
    while True:
        try:
            async with async_timeout.timeout(1):
                message = await channel.get_message(ignore_subscribe_messages=True)
                if message is not None:
                    print(f"(Reader) Message Received: {message}")
                    if message["data"] == STOPWORD:
                        print("(Reader) STOP")
                        return 666
                await asyncio.sleep(0.01)
        except asyncio.TimeoutError:
            return 0


async def main():
    uri_dic = dict(
        host="xx.xx.xx.xx",
        password="xxxxxxxx",
        port=3306,
        db=0
    )
    redis_pool = AsyncRedis(uri_dic)
    client = await redis_pool.client()  # 获取连接池
    pubsub = client.pubsub()
    print(pubsub)  # <aioredis.client.PubSub object at 0x00000219DFBEBB80>
    await pubsub.subscribe("channel:1", "channel:2")

    future = asyncio.create_task(reader(pubsub))
    # await asyncio.sleep(3)
    # await client.publish("channel:1", "STOPWORD")
    # await client.publish("channel:1", "Hello")
    # await client.publish("channel:2", "World")
    aaa = client.publish("channel:1", STOPWORD)
    # print(await aaa)
    # await asyncio.sleep(30)
    # print(await aaa)
    # print(await future)


if __name__ == "__main__":
    asyncio.run(main())
