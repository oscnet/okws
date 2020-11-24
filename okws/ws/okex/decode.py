import base64
import datetime
import hmac
import json
import logging
import zlib
from collections.abc import Mapping

from interceptor.interceptor import add_response, Interceptor

"""用法:
    okex(), okex(cfg)    
    cfg: {apiKey,secret,password}
    如果有 apiKey，则当联接到 ws 服务器时，自动登录
    主要功能：
    * 将 ws 发送的 byte 数据转换到 utf-8
    * 将 json 字符串转换成 map 到 ctx['DATA']中，原始数据在 ctx['_data_'] 中
    * 如果有登录参数，则在联接到 ws 服务器时，自动登录
"""


def _get_timestamp():
    now = datetime.datetime.now()
    return now.timestamp()


def _inflate(data):
    decompress = zlib.decompressobj(-zlib.MAX_WBITS)  # see above
    inflated = decompress.decompress(data)
    inflated += decompress.flush()
    return inflated


class _decode(Interceptor):
    def __init__(self, cfg):
        self.cfg = cfg

    async def enter(self, ctx):
        if ctx["_signal_"] == "CONNECTED":
            if self.cfg.get('password','')!='':
                # 登录
                await logon(ctx, self.cfg)

        elif ctx["_signal_"] == "ON_DATA":
            try:
                buff = _inflate(ctx["_data_"]).decode("utf-8")
                msg = json.loads(buff)
                ctx["DATA"] = msg
            except Exception:
                logging.exception(f"不能解析收到的交易所信息:{buff}")
                ctx["DATA"] = {}

    async def leave(self, ctx):
        if ctx.get("response"):
            resp = []
            for res in ctx["response"]:
                if isinstance(res, Mapping):
                    resp.append(json.dumps(res))
                elif isinstance(res, str):
                    resp.append(res)
            ctx["response"] = resp


def decode(cfg={}):
    return _decode(cfg)


# 一些工具函数


async def logon(ctx, ccxt_param):
    api_key = ccxt_param.get("apiKey") or ccxt_param.get("apikey")
    passphrase = ccxt_param["password"]
    secret_key = ccxt_param["secret"]
    await login(ctx, api_key, passphrase, secret_key)


async def login(ctx, api_key, passphrase, secret_key):
    timestamp = str(_get_timestamp())
    message = timestamp + "GET" + "/users/self/verify"

    mac = hmac.new(
        bytes(secret_key, encoding="utf8"),
        bytes(message, encoding="utf-8"),
        digestmod="sha256",
    )
    d = mac.digest()
    sign = base64.b64encode(d)

    login_param = {
        "op": "login",
        "args": [api_key, passphrase, timestamp, sign.decode("utf-8")],
    }

    await ctx["_server_"].send(json.dumps(login_param))

    # add_response(ctx, login_param)


def subscribe(ctx, channels):
    if type(channels) == list:
        cmd = {"op": "subscribe", "args": channels}
    elif type(channels) == str:
        cmd = {"op": "subscribe", "args": [channels]}
    else:
        raise ValueError("channels 错误")

    add_response(ctx, cmd)


def unsubscribe(ctx, channels):
    if type(channels) == list:
        cmd = {"op": "unsubscribe", "args": channels}
    elif type(channels) == str:
        cmd = {"op": "unsubscribe", "args": [channels]}
    else:
        raise ValueError("channels 错误")

    add_response(ctx, cmd)


def on_channel(ctx, channel):
    return (ctx.get("_signal_") == "ON_DATA") and (
        ctx["DATA"].get("table") == channel
    )
