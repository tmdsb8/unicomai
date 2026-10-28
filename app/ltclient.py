"""联通视频云拉流客户端。

协议说明：连接 wss://<wsHost>/h5player/live，发送 _paramStr_=_paramStr_ 加密参数后，
服务端以二进制帧推送裸 H.265 码流（含 G711A 音频，按帧交错）。
"""
import base64
import json
import random
import string
import ssl
import time

import websocket

PRODUCT_KEY = "3bd0c1bc-f50"
CHANNEL = "720p"
SECRET = "html5_open_api_check_secret"


def _rand_digits(n: int) -> str:
    return "".join(random.choice(string.digits) for _ in range(n))


def _js_escape(s: str) -> str:
    """等价于 JS 的 escape()：仅保留 ASCII 字母数字与 @*_+-./，其余 %XX 编码。"""
    keep = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789@*_+-./")
    out = []
    for ch in s:
        if ch in keep:
            out.append(ch)
        else:
            cp = ord(ch)
            if cp < 256:
                out.append("%%%02X" % cp)
            else:
                out.append("%%u%04X" % cp)
    return "".join(out)


def encrypt_param(plaintext: str) -> str:
    runes = list(plaintext)
    half = len(runes) // 2
    swapped = "".join(runes[half:]) + "".join(runes[:half])
    escaped = _js_escape(swapped)
    b64 = base64.b64encode(escaped.encode("utf-8")).decode("ascii")
    prefix = base64.b64encode(("%010d" % random.randint(1000000000, 9999999999)).encode())[:10].decode()
    return prefix + b64


def build_param_msg(token: str, device_id: str, channel_no: str = "",
                    share_id: str = "", relay_server: str = "", device_name: str = "") -> str:
    payload = {
        "requestTime": str(int(time.time() * 1000)),
        "productKey": PRODUCT_KEY,
        "deviceId": device_id,
        "channelNo": channel_no,
        "token": token,
        "hasAudio": "true",
        "region": "",
        "isPermanentStorage": "false",
        "channel": CHANNEL,
        "deviceName": device_name,
        "clientId": "WEBCLIENT_H5_" + _rand_digits(22) + str(int(time.time() * 1000)),
        "shareId": share_id,
        "relayServer": relay_server,
        "isSDCardPlayback": "false",
        "preConnect": "false",
        "releaseVersion": "H5PlayerServer_220719_B1072_4a25458_xml2json",
        "isSupportWASM": "1",
    }
    return encrypt_param(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))


def open_stream(ws_host: str, token: str, device_id: str, channel_no: str = "",
                share_id: str = "", relay_server: str = "", device_name: str = "",
                timeout: int = 15):
    """建立到摄像头的 WebSocket 连接，返回已订阅的 ws 连接对象。"""
    url = "wss://%s/h5player/live" % ws_host
    ws = websocket.create_connection(
        url,
        header=["User-Agent: ChinaUnicom/12.1200 (Android 16)"],
        sslopt={"cert_reqs": ssl.CERT_NONE},
        timeout=timeout,
    )
    msg = "_paramStr_=" + build_param_msg(token, device_id, channel_no, share_id, relay_server, device_name)
    ws.send(msg)
    return ws
