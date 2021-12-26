import asyncio

# 自定义cookie
# 获取当前访问网站的cookie  我们最好使用session.cookie_jar.filter_cookies()获取网站cookie
# 获取网站的响应状态码   status_code


class AsyncContextManager:
    def __init__(self, filename, mode, encode) -> None:
        # 数据库连接配置
        self.file = open(filename, mode=mode, encoding=encode)
    
    async def do_something(self):
        # 异步操作数据库
        return self.file.read()
    
    async def __aenter__(self):
        # 异步链接数据库
        return self
    
    async def __aexit__(self, exc_type, exc, tb):
        self.file.close()


async def main():
    async with AsyncContextManager("./01_day_01.py", "r", "utf8") as file:
        text = await file.do_something()
    print(text)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

