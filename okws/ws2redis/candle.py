# 处理 k 线数据的保存和取出
import logging
from datetime import datetime
import json

logger = logging.getLogger(__name__)

MAXLENGTH = 1000


# 保存到 redis

async def write(ctx):
    table = ctx['data']['table']
    if ('response' not in ctx) and (table.find("candle") > 0):
        for d in ctx['data']['data']:
            # logger.info(d)
            candle = dict(zip(["timestamp", "open", "high", "low",
                               "close", "volume", "currency_volume"], d['candle']))
            key = f"okex/{ctx['name']}/{table}:{d['instrument_id']}"
            dt = datetime.strptime(
                candle['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
            # await ctx['redis'].delete(key)
            ret = await ctx['redis'].zrank(key, candle['timestamp'])
            # logger.info(f"{candle['timestamp']}: {ret}")
            if ret is None:
                # 开始新的  timestamp 数据，表示上个 `timestamp` 的 K 线已经确定，可以使用。
                l = await ctx['redis'].zcard(key)
                if l > 0:
                    ts = await ctx['redis'].zrange(key, l-1, l-1, encoding='utf-8')
                    last_candle = await ctx['redis'].hgetall(f"{key}/{ts[0]}", encoding='utf-8')
                    # logger.info(f"last candle={last_candle}")
                    await ctx['redis'].publish(key, json.dumps({'candle': last_candle, 'timestamp': ts[0]}))

            await ctx['redis'].zadd(key, dt.timestamp(), candle['timestamp'])
            l = await ctx['redis'].zcard(key)
            if l > MAXLENGTH:
                await ctx['redis'].zremrangebyrank('key', 0, l - MAXLENGTH)
            await ctx['redis'].hmset_dict(f"{key}/{candle['timestamp']}", candle)
        # 标记已处理
        ctx['response'] = True


# 从 redis 取数据
async def read(ctx):
    """取 k 线数据
    Args:
        ctx:
            channel: okex 频道名, 如 'swap/candle60s'
            {'instrument_id': 'BTC-USD-SWAP','n':100} n 可选参数，取最新的 n 条 k 线数据
    返回：最新的 n 条 k 线数据列表。
        subscribe 'okex/name/swap/candle60s' 可以在有新 k 线时得到通知，通知内容为最新 k 线的 timestamp,表示这个 timestamp 之前的 k 线已经确定，可以使用。

    ```
        [
            {
                'timestamp': '2020-11-12T13:20:00.000Z',
                'open': '15866.1',
                'high': '15877.3',
                'low': '15852.5',
                'close': '15877.3',
                'volume': '5966',
                'currency_volume': '37.5977'
            },
        ]
    ```

    例：`get('pub', 'swap/candle60s', {'instrument_id': 'BTC-USD-SWAP','n':100})`
    """
    if ('response' not in ctx) and ctx['path'].find("candle") > 0:
        if 'instrument_id' not in ctx:
            raise Exception(" params 参数中没有 instrument_id")
        real_path = f"okex/{ctx['name']}/{ctx['path']}:{ctx['instrument_id']}"
        index = 0
        if 'n' in ctx:
            l = await ctx['redis'].zcard(real_path)
            if ctx['n'] < l:
                index = l - ctx['n']

        timestamps = await ctx['redis'].zrange(real_path, index, encoding='utf-8')
        candles = []
        for timestamp in timestamps:
            candle = await ctx['redis'].hgetall(f"{real_path}/{timestamp}", encoding='utf-8')
            candles.append(candle)
        # logger.info(candles)
        ctx['response'] = candles


config = {
    "write": {'enter': write},
    "read": {'enter': read}
}
