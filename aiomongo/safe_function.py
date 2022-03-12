# _*_ coding:utf-8 _*_
import asyncio
from functools import wraps
import traceback
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
from loguru import logger
import warnings
warnings.filterwarnings('ignore')
# 装饰器做安全函数


def retry_if_exception(ex: Exception, retry_cout: int = 3, wait: int = 2, out_exc=True):
    """ 捕获异常进行重试

    :param ex: 异常
    :param retry: 重试次数
    :param wait: 重试间隔(秒)
    :param out_exc: 输出错误栈
    """
    def safe_function(func):
        @wraps(func)
        async def wrapper(self, *arg, **kwargs):
            for retry in range(retry_cout):
                try:
                    result = await func(self, *arg, **kwargs)
                    return result
                except ex as e:
                    logger.warning(f'第{(retry + 1)}次重试, {func.__name__ }:{func.__doc__ }:{e.args}')
                    out_exc and logger.error(traceback.format_exc())
                    self.total_retry_count += 1
                    await asyncio.sleep(wait)
            return False
        return wrapper
    return safe_function
