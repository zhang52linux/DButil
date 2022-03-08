#_*_ coding: utf8 _*_
import asyncio
# 修改事件循环的策略, <适用window>
# asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # 解决设置代理后ssl报错
import uvloop  # pip3 install uvloop <速度更快, 适用linux>
import aiohttp
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
'''
协程:
- 协程: 线程自己创建的执行体, 给它们指定执行入口，申请内存给它们做执行栈，线程就可以按需调度这些执行体了
- 为了实现执行体之间的切换, 线程也记录了它们的控制信息(id、栈的位置、执行入口、执行现场...)
- 切换时，先保存当前执行体的执行现场，然后切换到另一个执行体
- 用户程序不能操作内核空间, 因此只能给协程分配用户栈，而操作系统对协程一无所知, 因此协程又称为'用户态线程'
- 可等待对象有三种主要类型: 协程, 任务 和 Future
- create_task(coro()) 将coro 协程 封装为一个 Task 并调度其执行
- Future 是一种特殊的 低层级 可等待对象，表示一个异步操作的 最终结果
'''

# 编写asyncio的代码, 与之前写的代码一致
# asyncio.sleep 相当于重新创建了一个future对象加入到事件循环中, 并且调用call_later(延迟，回调,,,)
# 规定在延迟多少秒后调用回调函数futures._set_result_unless_cancelled, 设置返回值<不设置就一直是无状态，一直会在事件循环中>
# 异步在多用户网络请求中才能发挥最大的用处, 用while True 或 for会见异步的效果抵消，使用gather才能有异步效果
# 如下的test就是一个协程, 线程会为其指定执行入口、分配用户栈内存空间、id等信息
# asyncio.run() 最先开始调度执行的协程<线程创建的执行体>
# 协程与io多路复用，可以在协程遇到io阻塞<网络io阻塞、文件io阻塞等>时，切换到下一个协程继续运行，不会傻傻等待

import time
async def test(timeout=1):
    async with aiohttp.ClientSession() as session:
        async with session.get("http://xxx.xxx.xxx.xxxx/learnowo/cloudflare") as resp:
            resp = await resp.text() # 每个网络请求都有响应时间,响应得快的先取出来，慢的继续在事件循环中
            print(resp)
            await asyncio.sleep(timeout) # asyncio.sleep事件循环没有停, 而<time.sleep(timeout)会暂停整个线程>
            print(f"时间超时: {timeout}")


async def main():
    start_time = time.time()
    await asyncio.gather(*[test(30), test(1)])
    end_time = time.time()
    print(end_time - start_time) # 总时间 = 等于实际get请求响应时间 + sleep(future等待时间)
    
    
# 内部的事件循环自动化为uvloop
asyncio.run(main())
