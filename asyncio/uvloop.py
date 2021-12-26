import asyncio
# 修改事件循环的策略, <适用window>
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # 解决设置代理后ssl报错
# import uvloop  # pip3 install uvloop <速度更快, 适用linux>
# asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


# 编写asyncio的代码, 与之前写的代码一致

# 内部的事件循环自动化为uvloop
# asyncio.run(...)
