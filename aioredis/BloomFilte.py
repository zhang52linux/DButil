# _*_ coding:utf-8 _*_
"""
*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*
Author: zhangsanyong
Date: 2022-01-06 15:06:28
LastEditors: zhangsanyong
LastEditTime: 2022-01-06 15:06:29
FilePath: /tornado/DButil/aioredis/BloomFilte.py
Description: 布隆过滤器
*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*
"""

'''
介绍布隆过滤器前先了解三个概念:
1、缓存穿透
- 缓存穿透就是数据的查询直接穿过缓存和后面的存储数据库
- 表现为某个查询的key在缓存中不存在, 在存储数据库上也不存在
- 危害: 数据库会一直做无用的查询, 消耗服务器性能
- 作用: 能够在节省存储空间的情况下迅速判断一个元素是否在一个集合中
- 应用场景：反垃圾邮件、url去重、将已存在的缓存放到布隆过滤器中，当黑客访问不存在的缓存时迅速返回避免缓存及DB挂掉
2、缓存击穿
- 缓存击穿指原本缓存数据库中有数据, 但是某个时间过期了, 数据的查询工作就跑到后面的存储数据库上了
- 解决方法: 1、设置不同的过期时间如随机数 2、多设置几层缓存
3、缓存雪崩
- 缓存雪崩值的是缓存数据库中某一段时间大量数据全部过期和缓存击穿类似, 雪崩指的大量key或全部key过期
- 解决方法: 1、设置不同的过期时间如随机数 2、多设置几层缓存

布隆过滤器原理:
- 开辟m位的bitArray(位数组), 开始所有的数据全部为0, 当一个元素进来，通过多个哈希函数计算不同的哈希值
并通过哈希值找到对应的bitArray下标处, 将里面的值置为1
- 关于多个哈希函数, 它们计算出来的值必须在[0,m)之中
'''

from pybloom_live import BloomFilter
from bitarray import bitarray

bf = BloomFilter(10000) # this BloomFilter must be able to store at least 10000 elements
result = bf.add("today") # 会自动根据capacity分配适当大小的bitarray
print(result)
result = bf.add("is")
print(result)
result = bf.add("today")
print(result)
print(bf.__contains__("today"))
print(len(bf.bitarray))

print(bitarray(10, endian='little'))
'''
return:
False
False
True
'''



