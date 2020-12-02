import okws
import pytest
import asyncio
import logging
import aioredis

pytestmark = pytest.mark.asyncio
redis_url = 'redis://localhost'
CHANNELS = ['TRADECHANNEL', 'aaaa']
logger = logging.getLogger(__name__)


async def send_exit_cmd(client):
    # 测试当 redis 收到 'exit' 时退出
    await asyncio.sleep(0.5)
    redis = await aioredis.create_redis(redis_url)
    await redis.publish(CHANNELS[0], 'Hello1')
    await redis.publish(CHANNELS[1], 'Hello2')
    await asyncio.sleep(0)
    await redis.publish(CHANNELS[0], 'exit')
    # await redis.publish(CHANNELS[1], 'exit')
    redis.close()
    await redis.wait_closed()
    # clientX.stop()


async def on_cmd(request):
    if request['_signal_'] == 'ON_DATA':
        msg = request['_data_']
        logger.info(f"Got message:{msg} on {request['_channel_']}")
        assert msg == 'exit' or msg == 'Hello1' or msg == 'Hello2'
        assert request['_channel_'] in CHANNELS
        if msg == 'exit':
            logger.info(f"close client {request['_channel_']}")
            request['_server_'].close()
            # or  return -1
    return 0


async def test_cmd():
    try:
        client = okws.Redis(CHANNELS, on_cmd)
        await asyncio.gather(
            client.run(),
            send_exit_cmd(client))
    except asyncio.CancelledError:
        logger.info("catch CancelledError!!")
