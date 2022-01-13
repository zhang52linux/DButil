import aiohttp
import asyncio
from loguru import logger

async def bossSpider(index):
    logger.success(f"----------------------------->{index}")
    await asyncio.sleep(20)


async def main():
    # await bossSpider(1)
    # await bossSpider(2)
    await asyncio.gather(*[bossSpider(1), bossSpider(2)])
    # async with aiohttp.ClientSession() as session:
    #     async with session.get('https://www.zhipin.com/job_detail/') as response:

    #         print("Status:", response.text)
    #         print("Content-type:", response.headers['content-type'])

    #         html = await response.text()
    #         print("Body:", html[:15], "...")

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
