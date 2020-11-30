import asyncio
import logging
import socket
from asyncio.exceptions import CancelledError, TimeoutError
import websockets
from websockets.exceptions import ConnectionClosed
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class Websockets:
    """连接到 OKEX ws 服务器
    当接收到服务器数据时，会调用 app(request)，并且对于 app(request) 的返回数据，会原样发送到 ws 服务器，也可以在 app 中使用 request['_server_'].send 向 ws 服务器发送数据
    request:
        _signal_: 有 READY，CONNECTED，ON_DATA，DISCONNECTED，EXIT。
             READY 当要开始联接 ws 服务器前
             CONNECTED 已成功联接上 ws 服务器
             TIMEOUT  当不能从服务器取得数据，超出 超时时发出。
             ON_DATA  接收到 ws 服务器数据，数据在 request['_data_']中.
             DISCONNECTED 当联接中断时发送
             EXIT  clientX 退出
        _server_: self


    例：
    from okws.ws.okex import Client

    async def app(request):
        print(request)

    clientX = Client(app)
    asyncio.run(clientX.run())  # or asyncio.create_task(clientX.run())

    """

    def __init__(self, app, ws_url="wss://real.okex.com:8443/ws/v3", timeout=25):
        """初始化

        Args:
            app ([type]): websocket 回调函数，当接收到服务器数据时，会调用 app(request)
            ws_url (str, optional): [description]. Defaults to "wss://real.okex.com:8443/ws/v3".
        """
        self.ws_url = ws_url
        self.timeout = timeout
        self.ws = None
        self.app = app
        self.lock = None
        self.task = None

    async def run_app(self, signal, **request):
        request["_signal_"] = signal
        request["_server_"] = self
        responses = await self.app(request)
        if responses and isinstance(responses, list):
            for res in responses:
                logger.info(f"send cmd:{res}")
                await self.send(res)
                # await asyncio.sleep(1)

    async def ping(self):
        await asyncio.wait_for(self.send("ping"), timeout=10)
        return await asyncio.wait_for(self.ws.recv(), timeout=10)

    # 重试 最短时间停10s,最大停15分钟后重试
    @retry(wait=wait_exponential(multiplier=1, min=10, max=900))
    async def serve(self):
        try:
            logger.info("READY")
            await self.run_app("READY")
            async with websockets.connect(self.ws_url) as self.ws:
                logger.info("已连接到 ws 服务器")
                await self.run_app("CONNECTED")
                while True:
                    # await asyncio.sleep(0)
                    try:
                        res_b = await asyncio.wait_for(
                            self.ws.recv(), timeout=self.timeout
                        )
                        await self.run_app("ON_DATA", _data_=res_b)
                    except TimeoutError:
                        await self.run_app('TIMEOUT')

        except (ConnectionClosed, socket.error):
            logger.exception("连接断开")
            raise
        except CancelledError:
            logger.info("任务取消")
            raise
        finally:
            self.ws = None
            await self.run_app("DISCONNECTED")

    async def run(self):
        try:
            self.task = asyncio.current_task()
            self.lock = asyncio.Lock()
            # self.task = asyncio.create_task(self.serve())
            # await self.task
            await self.serve()
        except CancelledError:
            pass
        finally:
            logger.info("退出 websocket Client!")
            await self.run_app("EXIT")
            self.ws = None

    async def send(self, msg):
        if self.ws:
            async with self.lock:
                await self.ws.send(msg)
        else:
            logger.warning(f"连接已经断开，不能发送数据：{msg}")

    def close(self):
        if self.task is not None:
            self.task.cancel()

    def __del__(self):
        self.close()
