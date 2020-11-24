"""拦截器

每个 Interceptor 包含 enter，leave 两个函数

例: execute(ctx, [interceptor1,interceptor2]) 将顺序执行： interceptor1.enter(ctx),interceptor2.enter(ctx),interceptor2.leave(ctx),interceptor1.leave(ctx), ctx['response'] 将成为 execute 的返回值

"""

from collections.abc import Mapping
from abc import ABC, abstractmethod
import logging
import pprint


class Interceptor(ABC):
    def __init__(self, name):
        self.name = name

    async def enter(self, ctx):
        pass

    async def leave(self, ctx):
        pass


async def enter_interceptor(interceptor, ctx):
    if isinstance(interceptor, Mapping):
        if 'enter' in interceptor:
            await interceptor['enter'](ctx)
    elif isinstance(interceptor, Interceptor):
        await interceptor.enter(ctx)
    elif interceptor is None:
        logging.warning("interceptor is None")
    else:
        raise Exception(f"{interceptor} is not a interceptor")


async def leave_interceptor(interceptor, ctx):
    if isinstance(interceptor, Mapping):
        if 'leave' in interceptor:
            await interceptor['leave'](ctx)
    elif isinstance(interceptor, Interceptor):
        await interceptor.leave(ctx)
    elif interceptor is None:
        logging.warning("interceptor is None")
    else:
        raise Exception(f"{interceptor} is not a interceptor")


async def execute(ctx, interceptors):
    ctx['_leave_'] = []

    for interceptor in interceptors:
        try:
            await enter_interceptor(interceptor, ctx)
            ctx['_leave_'].append(interceptor)
        except Exception:
            logging.exception('运行 enter 出错')
            # 如果有出错，试图运行后面的
            continue

    while True:
        try:
            interceptor = ctx['_leave_'].pop()
            await leave_interceptor(interceptor, ctx)
        except IndexError:
            break
        except Exception:
            logging.exception('运行 leave 出错')
            continue

    return ctx.get('response')


def add_response(ctx, res):
    if 'response' in ctx:
        ctx['response'].append(res)
    else:
        ctx['response'] = [res]


class SetContext(Interceptor):
    # 用于在 ctx 中保存 kwargs 的值
    def __init__(self, **kwargs):
        super().__init__('SetContext')
        self.kwargs = kwargs

    async def enter(self, ctx):
        ctx.update(self.kwargs)

    async def leave(self, ctx):
        pass


def set_context(**kwargs):
    return SetContext(**kwargs)


class Monitor(Interceptor):
    def __init__(self, print=True):
        super().__init__('Monitor')
        self.print = print

    async def enter(self, ctx):
        if self.print:
            pprint.pprint(ctx)

    async def leave(self, ctx):
        pass
