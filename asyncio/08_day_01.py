# 原子操作：将两步操作设计成一个事务，事务里可以有多个步骤，其中任何一步出现问题，事务都将失败，前面的步骤全部回滚，就像什么事都没发生。这种操作就叫做原子操作，这种特性就叫做原子性
# 在 Python 多线程中，变量是共享的，这也是相较多进程的一个优点，线程占用资源要少得多，但也导致多个 CPU 同时操作多个线程时会引起结果无法预测的问题，也就是说 Python 的线程不安全
import aiofiles

class AsyncContextManager:
    def __init__(self, filename, mode, encode) -> None:
        self.mode = mode
        self.file = aiofiles.open(filename, mode=mode, encoding=encode)
    
    async def do_something(self, content=None):
        if self.mode == "r":
            return await self.file.read()
        else:
            return self.file.write(content)
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc, tb):
        await self.file.aclose()