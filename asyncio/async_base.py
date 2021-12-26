# 进程与线程:
# 对操作系统来说，线程是最小的执行单元，进程是最小的资源管理单元。
# 无论进程还是线程，都是由操作系统所管理的

# 协程就是在一个线程中遇到io阻塞后, 会去执行其他的, 是由程序员自己构造出来的, 对操作系统而言实际不存在
# 协程有效的缓解了，系统线程过多造成的占用大量内存空间, 和线程与线程之间的切换需要大量的时间
# 协程的注意事项:
# 1、协程只有在等待IO的过程中才能重复利用线程
# 2、假设协程运行在线程之上，并且协程调用了一个阻塞IO操作，这时候会发生什么？实际上操作系统并不知道协程的存在，
# 它只知道线程，因此在协程调用阻塞IO操作的时候，操作系统会让线程进入阻塞状态，当前的协程和其它绑定在该线程之上的协程都会陷入阻塞而得不到调度
# 3、协程只有和异步IO结合起来，才能发挥最大的威力

# python中有三种可等待对象: 协程对象, Future对象, Task对象
# await + 可等待对象
import asyncio
import time


def today():
    time.sleep(3)
    return 12

# 简单地调用一个协程并不会使其被调度执行


async def nested():
    await asyncio.sleep(3)
    return 42


async def main():
    print(f"all started at {time.strftime('%X')}")
    # Nothing happens if we just call "nested()".
    # A coroutine object is created but not awaited,
    # so it *won't run at all*.
    today()

    # Let's do it differently now and await it:
    print(await nested())  # will print "42".

    print(f"all finished at {time.strftime('%X')}")

asyncio.run(main())
