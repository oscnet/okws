import asyncio
import pytest
import okws
import json
import logging
import os
import aioredis
import ccxt.async_support as ccxt
import okws.aioclient as aclient
from okws.ws2redis.app import app
pytestmark = pytest.mark.asyncio
logger = logging.getLogger(__name__)


# 取环境变量 oktest 作为测试交易所帐号
def get_okex_params():
    oktest = os.environ.get('oktest')
    if oktest is None:
        raise Exception("请设置 oktest 环境变量！")
    return json.loads(oktest)


async def public_test(client):
    await asyncio.sleep(2)
    await client.send(json.dumps(
        {"op": "subscribe", "args": ["swap/candle300s:BTC-USD-SWAP"]}))
    await asyncio.sleep(3)
    proxy = await aclient.client()

    # test candle
    k = await proxy.get('pub', 'swap/candle300s', {'instrument_id': 'BTC-USD-SWAP'})
    ts = list(map(lambda x: x['timestamp'], k))
    logger.info(ts)
    assert type(k[0]) == dict
    assert type(k) == list

    await proxy.close()
    client.close()


async def account(okex, ws_client, proxy):
    await ws_client.send(json.dumps(
        {"op": "subscribe", "args": ["spot/account:USDT"]}))

    await asyncio.sleep(1)

    await okex.account_post_transfer({
        "currency": "USDT",
        "amount": "1",
        "from": "1",  # 币币帐户
        "to": "6",  # 资金帐户
    })

    await asyncio.sleep(1)
    k = await proxy.get('test', 'spot/account', {'currency': 'USDT'})
    logger.info(k)

    await okex.account_post_transfer({
        "currency": "USDT",
        "amount": "1",
        "from": "6",
        "to": "1",
    })

    await asyncio.sleep(1)
    k = await proxy.get('test', 'spot/account', {'currency': 'USDT'})
    logger.info(k)

    assert "balance" in k
    assert "available" in k


async def private_test(cl):
    okex = ccxt.okex(get_okex_params())
    proxy = await aclient.client()

    await asyncio.sleep(5)

    await account(okex, cl, proxy)

    # close
    await okex.close()
    await asyncio.sleep(5)
    await proxy.close()
    cl.close()
    await asyncio.sleep(1)


async def redis_delete_all():
    # 清险所有数据
    redis = await aioredis.create_redis("redis://localhost")
    keys = await redis.keys("okex/*")
    logger.info(keys)
    for key in keys:
        await redis.delete(key)
    redis.close()
    await redis.wait_closed()


async def test_app():
    # await redis_delete_all()

    client1 = okws.Websockets(app('pub'))
    client2 = okws.Websockets(app('test', get_okex_params()))

    # with pytest.raises(CancelledError):
    await asyncio.gather(
        client1.run(),
        client2.run(),
        public_test(client1),
        private_test(client2)
    )
