"""云台桥接：把云台指令交给 recplayer 的已验证通道下发。

背景
----
本机摄像头（森锐 CA040-J）的机身确实带云台电机，用 recplayer 的控制台下发
方向指令后画面视角会真实改变（实测：静止基线 1.09，四个方向转动后位移
23~34，远超噪声）。

但自建平台直接在 /h5player/live 的 WebSocket 上发同样的
``[命令号] + JSON`` 帧，设备毫无反应——节点（ahhf/hncs）、是否带
relayServer、JSON 是否转义、msgSession 取值都逐一试过，位移始终停在
噪声水平。recplayer 的 Go 端走的是它自己的 live 会话控制通道，
该通道的握手细节无法在合理成本内还原。

因此这里改为桥接：平台把方向指令转发给本机 recplayer 的 ``/api/ptz``，
由它走已经打通的会话下发给设备。这样用户点按钮时镜头真的会转。

前置条件：recplayer 正在本机运行，且该设备的直播会话已建立。
本模块会自动登录、自动维持直播订阅，并在 recplayer 未启动时尝试拉起。
"""
import json
import logging
import os
import subprocess
import threading
import time

import requests

log = logging.getLogger("ptzbridge")

DEFAULT_BASE = "http://127.0.0.1:8899"
REC_DIR = "/opt/recplayer"
REC_BIN = "recplayer-linux-arm64"
# 沙箱里是 x86 主机跑 aarch64 程序，需要 qemu；真实 arm 机器上留空即可
REC_QEMU = "qemu-aarch64-static"


class PtzBridge:
    """维护到 recplayer 的会话，并转发云台指令。"""

    def __init__(self, base: str = DEFAULT_BASE,
                 user: str = os.environ.get("UM_PTZ_USER", "admin"),
                 password: str = os.environ.get("UM_PTZ_PASS", "")):
        self.base = base.rstrip("/")
        self.user = user
        self.password = password
        self.sess = requests.Session()
        self.logged_at = 0.0
        self._lock = threading.Lock()
        self._watchers = {}          # deviceId -> Thread
        self._stop = threading.Event()

    # ---------- recplayer 进程 ----------
    def _alive(self) -> bool:
        try:
            r = self.sess.get(self.base + "/api/live/status", timeout=4)
            return r.status_code in (200, 401)
        except Exception:
            return False

    def ensure_running(self) -> bool:
        if self._alive():
            return True
        if not os.path.isdir(REC_DIR):
            return False
        try:
            exe = REC_BIN
            if REC_QEMU and subprocess.run(
                    ["which", REC_QEMU], capture_output=True).returncode == 0:
                exe = REC_QEMU + " ./" + REC_BIN
            subprocess.Popen(
                ["setsid", "bash", "-c", "cd %s && %s >> /tmp/recplayer_bridge.log 2>&1" % (REC_DIR, exe)],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                stdin=subprocess.DEVNULL, start_new_session=True,
            )
        except Exception as e:
            log.warning("拉起 recplayer 失败: %s", e)
            return False
        for _ in range(20):
            time.sleep(1)
            if self._alive():
                return True
        return False

    # ---------- 登录 ----------
    def login(self, force: bool = False) -> bool:
        with self._lock:
            if not force and time.time() - self.logged_at < 3600:
                return True
            try:
                r = self.sess.post(
                    self.base + "/api/auth/login",
                    json={"account": self.user, "password": self.password, "days": 7},
                    timeout=10,
                )
                if r.status_code == 200 and '"ok":true' in r.text:
                    self.logged_at = time.time()
                    return True
                log.warning("recplayer 登录失败: %s %s", r.status_code, r.text[:120])
            except Exception as e:
                log.warning("recplayer 登录异常: %s", e)
            return False

    # ---------- 维持直播会话 ----------
    def _watch(self, device_id: str):
        """持续读取 /api/live 流，让 recplayer 认为该设备直播中。"""
        url = "%s/api/live?device=%s" % (self.base, device_id)
        while not self._stop.is_set():
            try:
                r = self.sess.get(url, stream=True, timeout=(10, 600))
                if r.status_code == 401:
                    if not self.login(force=True):
                        time.sleep(10)
                        continue
                    continue
                for _ in r.iter_content(chunk_size=65536):
                    if self._stop.is_set():
                        break
            except Exception:
                pass
            time.sleep(3)

    def ensure_live(self, device_id: str) -> bool:
        if device_id in self._watchers and self._watchers[device_id].is_alive():
            return True
        if not self.ensure_running():
            return False
        if not self.login():
            return False
        t = threading.Thread(target=self._watch, args=(device_id,), daemon=True)
        t.start()
        self._watchers[device_id] = t
        # 等会话状态变成 live
        for _ in range(15):
            time.sleep(1)
            try:
                r = self.sess.get(self.base + "/api/live/status", timeout=5)
                data = r.json().get("data") or []
                if any(x.get("deviceId") == device_id and x.get("state") == "live" for x in data):
                    return True
            except Exception:
                pass
        return False

    # ---------- 下发 ----------
    def send(self, device_id: str, action: str, mode: str = "continuous") -> dict:
        if not self.ensure_live(device_id):
            return {"ok": False, "error": "云台桥接不可用：recplayer 未运行或直播未启动"}
        for attempt in (0, 1):
            try:
                r = self.sess.post(
                    self.base + "/api/ptz",
                    params={"device": device_id, "action": action, "mode": mode},
                    timeout=10,
                )
                if r.status_code == 401 and attempt == 0:
                    self.login(force=True)
                    continue
                j = r.json()
                return {"ok": bool(j.get("ok")), "raw": j,
                        "error": "" if j.get("ok") else (j.get("msg") or "下发失败")}
            except Exception as e:
                if attempt == 0:
                    continue
                return {"ok": False, "error": "桥接异常: %s" % e}
        return {"ok": False, "error": "桥接失败"}

    def shutdown(self):
        self._stop.set()


BRIDGE = PtzBridge()
