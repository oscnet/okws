# 处理一般 websockets 数据的保存和取出


# 不支持 公共-400档深度频道


import logging

logger = logging.getLogger(__name__)

MAXLENGTH = 1000


async def redis_clear(redis, path="okex/*"):
    # 清除 redis 服务器中的相关数据
    keys = await redis.keys(path)
    for key in keys:
        await redis.delete(key)


def endswith(s, ends):
    for end in ends:
        if s.endswith(end):
            return end
    return None


uni_id = {
    "/trade": 'trade_id',
    "/order": 'order_id',
    "/order_algo": 'algo_id'
}


async def write(ctx):
    if 'response' not in ctx:
        table = ctx['data']['table']
        logger.debug(f"receive from ws: {table}")

        # 对于数组，使用各个 id 排序，最多保存 MAXLENGTH 项
        end = endswith(table, uni_id.keys())
        if end is not None:
            # 使用数组 要有一个唯一数做为 key
            uid = uni_id[end]
            key = f"okex/{ctx['name']}/{table}"
            for data in ctx['data']['data']:
                await ctx['redis'].zadd(key, data[uid], data[uid])
                len = await ctx['redis'].zcard(key)
                if len > MAXLENGTH:
                    await ctx['redis'].zremrangebyrank('key', 0, len - MAXLENGTH)
                await ctx['redis'].hmset_dict(f"{key}/{data[uid]}", data)
            ctx['response'] = True

        elif table == 'spot/account':
            # currency 唯一
            for data in ctx['data']['data']:
                key = f"okex/{ctx['name']}/{table}/{data['currency']}"
                await ctx['redis'].hmset_dict(key, data)
            ctx['response'] = True
        elif table == 'futures/account':
            for data in ctx['data']['data']:
                for k, v in data.items():
                    key = f"okex/{ctx['name']}/{table}/{k}"
                    await ctx['redis'].hmset_dict(key, v)
            ctx['response'] = True
        elif table == 'futures/instruments':
            key = f"okex/{ctx['name']}/{table}"
            # 先删除原有数据
            await redis_clear(ctx['redis'], key+'*')
            for data in ctx['data']['data'][0]:
                await ctx['redis'].sadd(key, data['instrument_id'])
                await ctx['redis'].hmset_dict(f"{key}/{data['instrument_id']}", data)
            ctx['response'] = True
        else:

            for data in ctx['data']['data']:
                # 对所有一个 instrument_id 只有一条数据的有效，如果是有多条数据，需要在前面处理
                if 'instrument_id' in data:
                    key = f"okex/{ctx['name']}/{table}/{data['instrument_id']}"
                    await ctx['redis'].hmset_dict(key, data)
                    ctx['response'] = True
                else:
                    logger.error(f"不知道如何处理：{table}\r\n{data}")


async def read(ctx):
    if 'response' not in ctx:
        end = endswith(ctx['path'], uni_id.keys())
        if end is not None:
            key = f"okex/{ctx['name']}/{ctx['path']}"
            index = 0
            if 'n' in ctx:
                l = await ctx['redis'].zcard(key)
                if ctx['n'] < l:
                    index = l - ctx['n']
            ids = await ctx['redis'].zrange(key, index, encoding='utf-8')
            datas = []
            for uid in ids:
                data = await ctx['redis'].hgetall(f"{key}/{uid}", encoding='utf-8')
                datas.append(data)
            # logger.info(candles)
            ctx['response'] = datas
        elif ctx['path'] == 'spot/account':
            if 'currency' in ctx:
                key = f"okex/{ctx['name']}/{ctx['path']}/{ctx['currency']}"
                ctx['response'] = await ctx['redis'].hgetall(key, encoding='utf-8')
            else:
                raise ValueError(f"需要参数 currency！ ")

        elif ctx['path'] == 'futures/instruments':
            key = f"okex/{ctx['name']}/{ctx['path']}"
            ids = await ctx['redis'].smembers(key, encoding='utf-8')
            datas = []
            for uid in ids:
                data = await ctx['redis'].hgetall(f"{key}/{uid}", encoding='utf-8')
                datas.append(data)
            # logger.info(candles)
            ctx['response'] = datas

        else:
            if 'instrument_id' in ctx:
                key = f"okex/{ctx['name']}/{ctx['path']}/{ctx['instrument_id']}"
                ctx['response'] = await ctx['redis'].hgetall(key, encoding='utf-8')
            else:
                raise ValueError(f"需要参数 instrument_id！ ")


config = {
    "write": {'leave': write},
    "read": {'leave': read}
}
