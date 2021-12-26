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
                        break
                await asyncio.sleep(0.01)
        except asyncio.TimeoutError:
            pass


async def main():
    uri_dic = dict(
        host="82.157.144.118",
        password="vnLxIZuYgx2BOMIWGWBK5DRfYEfr!fCP",
        port=31568,
        db=0
    )
    redis_pool = AsyncRedis(uri_dic)
    client = await redis_pool.client()  # 获取连接池
    pubsub = client.pubsub()
    print(pubsub)
    await pubsub.subscribe("channel:1", "channel:2")

    future = asyncio.create_task(reader(pubsub))

    await client.publish("channel:1", "Hello")
    await client.publish("channel:2", "World")
    await client.publish("channel:1", STOPWORD)

    await future


if __name__ == "__main__":
    asyncio.run(main())
