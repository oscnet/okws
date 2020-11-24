import asyncio
import pytest
import logging
from okws.client import create_client
from okws.server import run
import ccxt.async_support as ccxt
from tests.test_okex_app import get_okex_params

pytestmark = pytest.mark.asyncio
logger = logging.getLogger(__name__)


async def position(okex, api):
    ret = await okex.subscribe('tests', "futures/position:DOT-USD-210326")
    assert ret['errorCode'] == 80000

    # 买入一个多单  /api/futures/v3/order

    await api.futures_post_order({
        "instrument_id": "DOT-USD-210326",
        "type": "1",  # 开多
        "order_type": "4",
        "size": "1",
    })

    await asyncio.sleep(1)
    ret = await okex.get('tests', "futures/position", {"instrument_id": "DOT-USD-210326"})
    qty = int(ret['long_qty'])
    assert qty > 0
    logger.info(ret)

    await api.futures_post_order({
        "instrument_id": "DOT-USD-210326",
        "type": "3",  # 平多
        "order_type": "4",
        "size": "1",
    })

    await asyncio.sleep(1)
    ret = await okex.get('tests', "futures/position", {"instrument_id": "DOT-USD-210326"})
    logger.info(ret)
    assert qty - int(ret['long_qty']) == 1


async def ticker(okex):
    ret = await okex.subscribe('tests', "spot/ticker:ETH-USDT")
    assert ret['errorCode'] == 80000

    await asyncio.sleep(2)
    ret = await okex.get('tests', "spot/ticker", {"instrument_id": "ETH-USDT"})
    logger.info(ret)
    assert 'best_bid' in ret
    assert 'timestamp' in ret
    ts = ret['timestamp']

    await asyncio.sleep(5)
    ret = await okex.get('tests', "spot/ticker", {"instrument_id": "ETH-USDT"})
    logger.info(ret)
    assert ts != ret['timestamp']


async def client():
    api = ccxt.okex(get_okex_params())

    await asyncio.sleep(2)
    okex = await create_client()
    await okex.redis_clear()

    ret = await okex.open_ws('tests', get_okex_params())
    logger.info(ret)
    assert ret['errorCode'] == 80000

    # 等待 tests 服务开启
    await asyncio.sleep(10)

    ret = await okex.servers()
    logger.info(f"servers:{ret}")
    assert ret['errorCode'] == 80000
    assert ret['message'] == ['tests']

    await ticker(okex)

    # -------------------------测试 futures/position -----------------------------------------------------
    # await position(okex, api)

    # --------------------------------全量合约信息频道-------------------------------------------------------
    # futures/instruments

    await asyncio.sleep(1)
    ret = await okex.subscribe('tests', "futures/instruments")
    assert ret['errorCode'] == 80000
    await asyncio.sleep(1)

    ret = await okex.get('tests', "futures/instruments")
    # logger.info(ret)
    assert len(ret) > 10

    # -----------------------------------------------------------------------------------------------------
    ret = await okex.close_ws('tests')
    logger.info(ret)

    ret = await okex.close_ws('tests--------')
    logger.info(ret)
    assert ret['errorCode'] == 80011

    ret = await okex.servers()
    logger.info(f"servers:{ret}")
    assert ret['errorCode'] == 80000
    assert ret['message'] == []

    await okex.server_quit()
    await okex.close()
    await asyncio.sleep(1)

    await api.close()


async def test_server():
    await asyncio.gather(
        run(),
        client()
    )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(filename)s[%(lineno)d] - %(levelname)s: %(message)s')

    asyncio.run(test_server())
