import asyncio, time


# Python 异步的实现 非阻塞
# Python Future对象属于可等待对象，因此可以在其他协程中被等待
async def set_after(fut, delay, value):
    await asyncio.sleep(delay)
    print('输出异步执行的结果')
    # 将future的结果抛出
    fut.set_result(value)

# 模拟一个耗时的其他任务
def compute_add(x,y):
    time.sleep(3)  # 会将当前线程卡住3s, 所以在使用协程是不用使用同步io，否则效果会大大折扣
    print('运行完毕...')
    return x + y

async def main():
    print(f"all started at {time.strftime('%X')}")

    loop = asyncio.get_running_loop() # 返回当前 系统 线程中正在运行的事件循环。
    # 创建一个Future对象<相当于没有绑定任何事件的task> 它是可等待对象 可以在await语句中使用 可以理解为把协程变成了一个Future对象，可以让Future对象在最开始的时候执行，然后在最后等待它的执行结果
    fut = loop.create_future()
    # 创建一个task, 任务并执行，此时loop事件循环中有两个task, 一个没有绑定事件，会一直运行, 而另外一个task为之前的task添加事件,使其可以正常退出
    loop.create_task(set_after(fut, 6, '... world'))

    # 执行其他的任务，并不会影响上面 Future对象 任务
    # print('hello ...')
    print(compute_add(7, 8))  # 在协程异步io中使用同步io，会导致整个程序变为同步

    # 取决于要在哪里等待结果 和nodejs不同的地方就在于，nodejs可以不去等待，当程序自己执行完毕后会退出，而Python这里需要自己去思考在哪里等待结果，当所有任务执行完毕 才退出
    print(await fut)  # 对应于socket_wait将结果从内核中取出来

    print(f"all finished at {time.strftime('%X')}")

asyncio.run(main())
