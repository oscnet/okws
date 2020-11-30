import asyncio
import pytest
import okws
import logging

# pytest -o log_cli=true --log-cli-level=info  -s

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
pytestmark = pytest.mark.asyncio

ws_url = "wss://real.okex.com:8443/ws/v3"


async def app(request):
    await okws.interceptor.execute(request, [okws.okex.Decode()])
    logging.info(request)


async def test_client():
    logger.info("start test")
    client = okws.Websockets(app, ws_url)

    async def stop():
        await asyncio.sleep(30)
        client.close()

    # with pytest.raises(CancelledError):
    await asyncio.gather(
        client.run(),
        stop()
    )
