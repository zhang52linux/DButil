# 异步上下文管理器
import asyncio
from asyncio.tasks import wait

class AsyncContextManager:
    def __init__(self) -> None:
        # 数据库连接配置
        self.conn = conn
    
    async def do_something(self):
        # 异步操作数据库
        return 666
    
    async def __aenter__(self):
        # 异步链接数据库
        # self.conn = await asyncio.sleep(1)
        return self
    
    async def __aexit__(self, exc_type, exc, tb):
        await asyncio.sleep(1)


# 这里有个小细节---就是不管是async with 还是 async for 都要写在async 函数中
async def func():
    async with AsyncContextManager() as f:
        result = await f.do_something()
        print(result)
    

asyncio.run(func())