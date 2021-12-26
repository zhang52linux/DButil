import aiohttp
import asyncio


async def main():

    async with aiohttp.ClientSession() as session:
        async with session.get('https://www.zhipin.com/job_detail/') as response:

            print("Status:", response.text)
            print("Content-type:", response.headers['content-type'])

            html = await response.text()
            print("Body:", html[:15], "...")

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
