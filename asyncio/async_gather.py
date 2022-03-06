# _*_ coding:utf-8 _*_
"""
*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*
Author: zhangsanyong
Date: 2022-03-05 16:06:28
LastEditors: zhangsanyong
LastEditTime: 2022-03-05 16:06:28
Description: asyncio的gather与顺序执行的速度差异比较
*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*
"""
import asyncio


async def factorial(name, number):
    f = 1
    for i in range(2, number + 1):
        print(f"Task {name}: Compute factorial({number}), currently i={i}...")
        await asyncio.sleep(3)
        f *= i
    print(f"Task {name}: factorial({number}) = {f}")
    return f


async def main():
    # Schedule three calls *concurrently*:
    L = await asyncio.gather(
        factorial("A", 2),
        factorial("B", 3),
        factorial("C", 4),
    )
    print(L)

asyncio.run(main())
