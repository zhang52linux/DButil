# asyncio_create_task.py
import asyncio
from asyncio.futures import Future
from asyncio.tasks import Task
# Future 是 Task类的基类

# _state 由Future维持着的一个值, 判断任务是否已经完成, 完成就不再等待

async def task_func():
    print('in task_func')
    return 'the result'


async def main(loop):
    print('creating task')
    task = loop.create_task(task_func())
    print('waiting for {!r}'.format(task))
    return_value = await task
    print('task completed {!r}'.format(task))
    print('return value: {!r}'.format(return_value))


async def set_after(fut):
    await asyncio.sleep(2)
    fut.set_result("666666")

async def main2():
    event_loop = asyncio.get_event_loop()
    try:
        # 创建一个任务(Future对象， 没有绑定任何行为, 则这个任务永远不知道什么时候结束)
        fut = event_loop.create_future()  
        
        # 创建一个任务(Task对象， 绑定了set_after函数, 函数内部在两秒后,给fut赋值
        tsk = event_loop.create_task(set_after(fut))

        # 等待Future 对象获取， 最终结果, 否则一直等待下去
        data = await fut

        print(data)
        event_loop.run_until_complete(main(event_loop))
    finally:
        event_loop.close()
