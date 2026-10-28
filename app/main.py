"""联通 AI 摄像头监控平台 —— 后端服务。

功能
----
- 直播：原始 H.265 通过 /api/live/{dev} 以 mp4 分片流输出，前端用 MSE 播放
- 录像：分片落盘 + 自动转 mp4 + 自动备份
- AI：抽帧检测人/移动物 -> 20s 视频 + 3 张截图
- 存储：可插拔云盘（local / webdav / s3 / baidu）
"""
import asyncio
import functools
import hashlib
import hmac
import json
import logging
import os
import queue
import secrets
import subprocess
import sys
import threading
import time

from fastapi import (FastAPI, HTTPException, Request, WebSocket,
                     WebSocketDisconnect)
from fastapi.responses import (FileResponse, HTMLResponse, JSONResponse,
                               Response, StreamingResponse)
from fastapi.staticfiles import StaticFiles

import storage
import engine as engine_mod
from engine import all_engines, get_engine
from events import get_event_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("app")

DATA_DIR = os.path.join(os.environ.get("UNICOMAI_HOME", "/opt/unicomai"), "data")
DEVICES_PATH = os.path.join(DATA_DIR, "devices.json")
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

os.makedirs(DATA_DIR, exist_ok=True)

app = FastAPI(title="联通 AI 摄像头监控平台")


# ---------------- 设备配置 ----------------
def load_devices():
    if os.path.exists(DEVICES_PATH):
        try:
            return json.load(open(DEVICES_PATH))
        except Exception:
            return []
    # 首次启动：可从 GO_UnicomMonitor 的 video.json 导入；
    # 设 UM_LEGACY_DEVICES="" 关闭导入（新部署无需旧程序时推荐）
    src = os.environ.get("UM_LEGACY_DEVICES", "/opt/unicommonitor/video.json")
    devs = []
    if src and os.path.exists(src):
        try:
            for v in json.load(open(src)):
                devs.append({
                    "name": v.get("name", "摄像头"),
                    "deviceId": v.get("deviceId"),
                    "wsHost": v.get("wsHost", ""),
                    "token": v.get("token", ""),
                    "channelNo": v.get("channelNo", ""),
                    "shareId": v.get("shareId", ""),
                    "relayServer": v.get("relayServer", ""),
                    "autoRecord": True,
                    "aiEnabled": True,
                })
        except Exception as e:
            log.warning("导入设备失败: %s", e)
    json.dump(devs, open(DEVICES_PATH, "w"), ensure_ascii=False, indent=2)
    return devs


def save_devices(devs):
    json.dump(devs, open(DEVICES_PATH, "w"), ensure_ascii=False, indent=2)


DEVICES = load_devices()


@app.on_event("startup")
def _startup():
    storage.start_worker()
    for d in DEVICES:
        if d.get("enabled", True):
            start_device(d)
    log.info("已启动 %d 台设备", len(DEVICES))
    # 历史 HEVC 录像与事件片段在后台慢慢转成 H.264，不影响拉流
    def _migrate():
        engine_mod.migrate_legacy(os.path.join(DATA_DIR, "recordings"), "recording")
        engine_mod.migrate_legacy(os.path.join(DATA_DIR, "events"), "event")
    threading.Thread(target=_migrate, daemon=True, name="migrate").start()
    # 每 5 分钟巡检一次，补转漏掉的历史片段（进程被杀留下的半截段）
    threading.Thread(target=engine_mod.janitor_loop, daemon=True, name="tc-janitor").start()


def start_device(d):
    cfg = dict(d)
    cfg["recordDir"] = os.path.join(DATA_DIR, "recordings")
    eng = get_engine(cfg)
    eng.record_enabled = d.get("autoRecord", True)
    eng.start()
    if d.get("aiEnabled", True):
        get_event_engine(eng, d.get("ai")).start()
    return eng


# ---------------- 登录认证 ----------------
AUTH_FILE = os.path.join(DATA_DIR, "auth.json")
LOCK_THRESHOLD = 3          # 连续失败 3 次锁定
LOCK_SECONDS = 600          # 锁 10 分钟
_AUTH_LOCK = threading.Lock()

PUBLIC_PATHS = {"/api/auth/status", "/api/auth/login", "/api/auth/setup",
                "/api/health"}


def _load_auth():
    try:
        with open(AUTH_FILE) as f:
            return json.load(f)
    except Exception:
        return None


def _save_auth(a):
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp = AUTH_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(a, f)
    os.replace(tmp, AUTH_FILE)


def _hash_pw(password: str, salt_hex: str) -> str:
    return hashlib.pbkdf2_hmac("sha256", password.encode(),
                               bytes.fromhex(salt_hex), 60000).hex()


def _new_session(a) -> str:
    """会话令牌：HMAC(密钥, 用户名)，改密码/登出时轮换。"""
    return hmac.new(bytes.fromhex(a["secret"]), a["user"].encode(),
                    hashlib.sha256).hexdigest()


def _auth_state():
    """返回 needSetup / authed / locked / lockRemaining 的公共状态。"""
    a = _load_auth()
    if not a:
        return {"needSetup": True, "authed": False, "locked": False,
                "lockRemaining": 0, "user": ""}
    locked = a.get("lockUntil", 0) > time.time()
    return {"needSetup": False, "authed": False, "locked": locked,
            "lockRemaining": int(a["lockUntil"] - time.time()) if locked else 0,
            "user": a.get("user", "")}


@app.post("/api/auth/setup")
async def auth_setup(req: Request):
    body = await req.json()
    user = str(body.get("user") or "admin").strip() or "admin"
    password = str(body.get("password") or "")
    if len(password) < 6:
        raise HTTPException(400, "密码至少 6 位")
    with _AUTH_LOCK:
        if _load_auth():
            raise HTTPException(403, "已初始化过，请直接登录")
        salt = secrets.token_hex(16)
        secret = secrets.token_hex(32)
        a = {"user": user, "salt": salt, "pw": _hash_pw(password, salt),
             "secret": secret, "failed": 0, "lockUntil": 0}
        a["session"] = _new_session(a)
        _save_auth(a)
    resp = JSONResponse({"ok": True})
    resp.set_cookie("sid", a["session"], httponly=True, samesite="lax",
                    max_age=30 * 86400)
    return resp


@app.post("/api/auth/login")
async def auth_login(req: Request):
    body = await req.json()
    user = str(body.get("user") or "").strip()
    password = str(body.get("password") or "")
    with _AUTH_LOCK:
        a = _load_auth()
        if not a:
            raise HTTPException(400, "尚未初始化账号")
        if a.get("lockUntil", 0) > time.time():
            raise HTTPException(423, "账号已锁定，请 %d 分钟后再试" % (
                int((a["lockUntil"] - time.time()) / 60) + 1))
        if hmac.compare_digest(_hash_pw(password, a["salt"]), a["pw"]) \
                and user == a["user"]:
            a["failed"] = 0
            a["session"] = _new_session(a)   # 每次登录轮换，旧会话失效
            _save_auth(a)
        else:
            a["failed"] = a.get("failed", 0) + 1
            msg = "用户名或密码错误（%d/%d）" % (a["failed"], LOCK_THRESHOLD)
            if a["failed"] >= LOCK_THRESHOLD:
                a["lockUntil"] = time.time() + LOCK_SECONDS
                a["failed"] = 0
                msg = "连续失败 %d 次，账号锁定 %d 分钟" % (
                    LOCK_THRESHOLD, LOCK_SECONDS // 60)
            _save_auth(a)
            raise HTTPException(401, msg)
    resp = JSONResponse({"ok": True})
    resp.set_cookie("sid", a["session"], httponly=True, samesite="lax",
                    max_age=30 * 86400)
    return resp


@app.post("/api/auth/password")
async def auth_password(req: Request):
    """修改账号名/密码。需已登录（中间件校验会话）+ 当前密码验证。

    成功后轮换会话密钥：其他已登录设备全部失效，本次会话下发新 Cookie。
    """
    body = await req.json()
    old_pw = str(body.get("oldPassword") or "")
    new_user = str(body.get("newUser") or "").strip()
    new_pw = str(body.get("newPassword") or "")
    with _AUTH_LOCK:
        a = _load_auth()
        if not a:
            raise HTTPException(400, "尚未初始化账号")
        if not hmac.compare_digest(_hash_pw(old_pw, a["salt"]), a["pw"]):
            raise HTTPException(401, "当前密码不正确")
        if new_pw:
            if len(new_pw) < 6:
                raise HTTPException(400, "新密码至少 6 位")
            a["salt"] = secrets.token_hex(16)
            a["pw"] = _hash_pw(new_pw, a["salt"])
        if new_user:
            a["user"] = new_user
        a["secret"] = secrets.token_hex(32)   # 踢掉其他已登录设备
        a["failed"] = 0
        a["lockUntil"] = 0
        a["session"] = _new_session(a)
        _save_auth(a)
    resp = JSONResponse({"ok": True, "user": a["user"]})
    resp.set_cookie("sid", a["session"], httponly=True, samesite="lax",
                    max_age=30 * 86400)
    return resp


@app.post("/api/auth/logout")
async def auth_logout(req: Request):
    a = _load_auth()
    if a:
        a["session"] = ""
        _save_auth(a)
    resp = JSONResponse({"ok": True})
    resp.delete_cookie("sid")
    return resp


@app.get("/api/auth/status")
async def auth_status(request: Request):
    st = _auth_state()
    a = _load_auth()
    st["authed"] = bool(a and a.get("session")
                        and request.cookies.get("sid") == a["session"])
    return st


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    path = request.url.path
    if path.startswith("/api") and path not in PUBLIC_PATHS:
        a = _load_auth()
        if a:
            # 未初始化账号时放行（首次进入会引导设置密码）；
            # session 为空（已登出）视为未认证，空 cookie 不算匹配
            sid = request.cookies.get("sid", "")
            if not a.get("session") or not sid or sid != a["session"]:
                return JSONResponse({"detail": "未登录或会话已过期"}, status_code=401)
    return await call_next(request)


# ---------------- WebRTC 低延迟直播 ----------------
from aiortc import RTCPeerConnection          # noqa: E402
import webrtc_bridge                          # noqa: E402

_webrtc_sessions = set()                      # 防 GC：引用必须长期持有


@app.post("/api/webrtc/offer")
async def webrtc_offer(req: Request):
    body = await req.json()
    device_id = body.get("deviceId") or (
        DEVICES[0]["deviceId"] if DEVICES else "")
    d = next((x for x in DEVICES if x["deviceId"] == device_id), None)
    if not d:
        raise HTTPException(404, "设备不存在")
    eng = get_engine(dict(d, recordDir=os.path.join(DATA_DIR, "recordings")))
    sess = webrtc_bridge.WebRTCSession(eng)
    _webrtc_sessions.add(sess)

    @sess.pc.on("connectionstatechange")
    async def _on_state():
        logging.info("[webrtc] %s -> %s", device_id, sess.pc.connectionState)
        if sess.pc.connectionState in ("failed", "closed", "disconnected"):
            await sess.close()
            _webrtc_sessions.discard(sess)

    try:
        return await sess.handle_offer(body["sdp"], body.get("type", "offer"))
    except Exception as e:
        await sess.close()
        _webrtc_sessions.discard(sess)
        raise HTTPException(500, "WebRTC 协商失败: %s" % e)


# ---------------- 页面 ----------------
@app.get("/", response_class=HTMLResponse)
def index():
    # 前端迭代频繁：禁止缓存，避免用户浏览器拿着旧版 JS 播不了新后端
    return FileResponse(os.path.join(STATIC_DIR, "index.html"),
                        headers={"Cache-Control": "no-cache, no-store, must-revalidate"})


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


# ---------------- 设备与状态 ----------------
@app.get("/api/devices")
def api_devices():
    out = []
    alive = _reap_viewers()      # 顺手清理断开的观看，观众数才准
    for d in DEVICES:
        eng = get_engine(dict(d, recordDir=os.path.join(DATA_DIR, "recordings")))
        st = eng.status()
        evt = get_event_engine(eng)
        st["ai"] = evt.status()
        st["autoRecord"] = d.get("autoRecord", True)
        st["aiEnabled"] = d.get("aiEnabled", True)
        st["ptz"] = d.get("ptz", True)     # 设备是否有云台电机
        st["ptzMode"] = "hardware" if d.get("ptz", True) else "digital"
        st["liveViewers"] = alive
        out.append(st)
    return out


@app.post("/api/devices/{device_id}/record")
async def toggle_record(device_id: str, request: Request):
    body = await request.json()
    for d in DEVICES:
        if d["deviceId"] == device_id:
            d["autoRecord"] = bool(body.get("enabled", True))
            save_devices(DEVICES)
            eng = get_engine(dict(d, recordDir=os.path.join(DATA_DIR, "recordings")))
            eng.record_enabled = d["autoRecord"]
            return {"ok": True, "autoRecord": d["autoRecord"]}
    raise HTTPException(404, "设备不存在")


@app.post("/api/devices/{device_id}/ai")
async def toggle_ai(device_id: str, request: Request):
    body = await request.json()
    for d in DEVICES:
        if d["deviceId"] == device_id:
            d["aiEnabled"] = bool(body.get("enabled", True))
            save_devices(DEVICES)
            eng = get_engine(dict(d, recordDir=os.path.join(DATA_DIR, "recordings")))
            evt = get_event_engine(eng, d.get("ai"))
            evt.cfg["enabled"] = d["aiEnabled"]
            if d["aiEnabled"]:
                evt.start()
            else:
                evt.stop()
            return {"ok": True, "aiEnabled": d["aiEnabled"]}
    raise HTTPException(404, "设备不存在")


@app.post("/api/ptz/{device_id}")
async def ptz_control(device_id: str, request: Request):
    """云台控制：left / right / up / down / stop / reset。

    指令复用拉流的那条 WebSocket —— 设备同一时刻只接受一个会话，
    为云台另开连接会把直播和录制一起挤掉。
    """
    d = next((x for x in DEVICES if x["deviceId"] == device_id), None)
    if not d:
        raise HTTPException(404, "设备不存在")
    raw = await request.body()
    body = json.loads(raw) if raw else {}
    action = str(body.get("action") or "").strip()
    if d.get("ptz", True) is False:
        # 机身没有电机，硬件指令发过去也不会有任何位移（实测各方向画面零变化），
        # 直接告诉前端走数字云台，避免误判为“按键失灵”。
        return {"ok": True, "mode": "digital", "action": action,
                "note": "该机型无云台电机，已切换数字云台"}
    eng = get_engine(dict(d, recordDir=os.path.join(DATA_DIR, "recordings")))
    res = await asyncio.get_event_loop().run_in_executor(None, eng.send_ptz, action)
    if not res.get("ok"):
        raise HTTPException(400, res.get("error", "下发失败"))
    res["mode"] = "hardware"
    res.setdefault("backend", res.get("backend", "direct"))
    res.pop("raw", None)
    return res


@app.get("/api/wslog/{device_id}")
def api_wslog(device_id: str, limit: int = 40):
    """设备上行控制帧留档（云台应答、中继状态等），用于排查指令是否生效。"""
    d = next((x for x in DEVICES if x["deviceId"] == device_id), None)
    if not d:
        raise HTTPException(404, "设备不存在")
    eng = get_engine(dict(d, recordDir=os.path.join(DATA_DIR, "recordings")))
    items = list(eng.ctrl_log)[-limit:]
    return {"count": len(items), "items": items}


# ---------------- 语音对讲 ----------------
_TALK_LOCK = threading.Lock()      # 设备喇叭同一时刻只接受一个对讲方


@app.websocket("/ws/talk/{device_id}")
async def ws_talk(ws: WebSocket, device_id: str):
    """浏览器 -> 摄像头语音对讲。

    协议与官方 webPlayer 完全一致：浏览器采集麦克风降采样为
    PCM16LE 8kHz 单声道，二进制帧（≤4.8KB）经本通道转发到设备
    的拉流 WebSocket（帧头 0x01）；文本 "stop" 或断开时发 0x02 停止。
    """
    a = _load_auth()
    if a and (not a.get("session") or ws.cookies.get("sid", "") != a["session"]):
        await ws.close(code=4401)
        return
    d = next((x for x in DEVICES if x["deviceId"] == device_id), None)
    if not d:
        await ws.close(code=4404)
        return
    await ws.accept()
    eng = get_engine(dict(d, recordDir=os.path.join(DATA_DIR, "recordings")))
    if not _TALK_LOCK.acquire(blocking=False):
        await ws.send_text(json.dumps({"error": "其他人正在对讲，请稍后再试"}))
        await ws.close(code=4409)
        return
    loop = asyncio.get_event_loop()
    stopped = False
    try:
        while True:
            msg = await ws.receive()
            if msg.get("type") == "websocket.disconnect":
                break
            data = msg.get("bytes")
            if data:
                res = await loop.run_in_executor(None, eng.send_talk, data)
                if not res.get("ok"):
                    await ws.send_text(json.dumps({"error": res.get("error")}))
            elif msg.get("text") == "stop":
                stopped = True
                await loop.run_in_executor(None, eng.stop_talk)
                await ws.send_text(json.dumps({"ok": True}))
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logging.warning("[talk] 会话异常: %s", e)
    finally:
        if not stopped:
            try:
                await loop.run_in_executor(None, eng.stop_talk)
            except Exception:
                pass
        _TALK_LOCK.release()


@app.post("/api/ptz_raw/{device_id}")
async def ptz_raw(device_id: str, request: Request):
    """调试接口：按原始字节往设备会话发一帧，用于协议探测。"""
    d = next((x for x in DEVICES if x["deviceId"] == device_id), None)
    if not d:
        raise HTTPException(404, "设备不存在")
    raw = await request.body()
    body = json.loads(raw) if raw else {}
    eng = get_engine(dict(d, recordDir=os.path.join(DATA_DIR, "recordings")))
    return await asyncio.get_event_loop().run_in_executor(None, eng.send_raw, body)


@app.post("/api/devices/{device_id}/ai/config")
async def set_ai_config(device_id: str, request: Request):
    """调整 AI 判定参数：灵敏度、抽帧间隔、冷却、事件时长、截图张数。"""
    body = await request.json()
    d = next((x for x in DEVICES if x["deviceId"] == device_id), None)
    if not d:
        raise HTTPException(404, "设备不存在")
    eng = get_engine(dict(d, recordDir=os.path.join(DATA_DIR, "recordings")))
    evt = get_event_engine(eng, d.get("ai"))

    allow = {
        "interval": (float, 0.3, 30.0),
        "personConf": (float, 0.01, 0.95),
        "objConf": (float, 0.01, 0.95),
        "motionTh": (float, 0.0001, 0.5),
        "cooldown": (int, 0, 3600),
        "preSeconds": (int, 0, 60),
        "postSeconds": (int, 5, 120),
        "shotCount": (int, 0, 9),
    }
    for k, (typ, lo, hi) in allow.items():
        if k in body:
            try:
                v = typ(body[k])
            except Exception:
                continue
            evt.cfg[k] = max(lo, min(hi, v))

    # 持久化到设备配置
    d["ai"] = {k: evt.cfg.get(k) for k in
               ("interval", "personConf", "objConf", "motionTh",
                "cooldown", "preSeconds", "postSeconds", "shotCount")}
    save_devices(DEVICES)
    return {"ok": True, "config": d["ai"]}


# ---------------- 直播（MSE） ----------------
def _ffmpeg_live_cmd(audio_fd: int):
    """H.265 -> H.264，输出 fMP4 分片流供浏览器 MSE 播放。

    音频：设备同一条 WebSocket 上以 0x62 标签帧推 G711A（8kHz 单声道），
    从 audio_fd 送进 ffmpeg 合成 AAC 音轨。
    注意：本机 ffmpeg 未编译 libx264，使用 libopenh264。
    """
    return [
        "ffmpeg", "-v", "error",
        "-fflags", "nobuffer", "-flags", "low_delay",
        "-f", "hevc", "-i", "pipe:0",
        "-f", "alaw", "-ar", "8000", "-ac", "1", "-i", "pipe:%d" % audio_fd,
        "-map", "0:v:0", "-map", "1:a:0",
        # 缩到 720p：原始 2304×1296 软编每个进程吃 ~2GB 内存，
        # 容器 8GB 上限下多路并发直接触发 OOM（ffmpeg 被 SIGKILL → 直播黑屏）
        "-vf", "scale=-2:720",
        "-c:v", "libopenh264", "-b:v", "1500k", "-g", "20",
        "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-b:a", "32k",
        # muxdelay=0：压掉 MP4 封装器的固有前置延迟（实时监控要求）
        "-muxdelay", "0", "-muxpreload", "0",
        "-f", "mp4", "-movflags", "frag_keyframe+empty_moov+default_base_moof",
        "pipe:1",
    ]


MAX_LIVE_VIEWERS = 6          # 每个观众一条独立转码进程，必须限流
_live_viewers = {}            # viewer_id -> {"ts":..., "q":..., "proc":...}
_live_lock = threading.Lock()


def _reap_viewers(force=False):
    """回收僵尸观看：30 秒没有推进的会话，连带退订队列、杀掉转码进程。

    async 生成器被 GC 时不保证执行 finally，只靠 finally 会漏，
    所以这里定期从外部强制收尾。返回当前有效数量。
    """
    now = time.time()
    dead = []
    with _live_lock:
        for k, v in list(_live_viewers.items()):
            if now - v["ts"] > 30:
                dead.append(_live_viewers.pop(k))
        alive = len(_live_viewers)
    for v in dead:
        q = v.get("q")
        if q is not None:
            try:
                v["eng"].unsubscribe(q)
            except Exception:
                pass
        p = v.get("proc")
        logging.warning("[reap] 回收观看 %s（静默 %.0fs，proc=%s）",
                        v.get("vid"), now - v["ts"], "有" if p else "无")
        if p is not None:
            try:
                p.kill()
                p.wait(timeout=2)     # 不 wait 会留下 <defunct> 僵尸
            except Exception:
                pass
    return alive


def _viewer_acquire():
    """占用一个观看名额，满了返回 None。"""
    if _reap_viewers() >= MAX_LIVE_VIEWERS:
        return None
    with _live_lock:
        vid = "%d_%d" % (int(time.time() * 1000), len(_live_viewers))
        while vid in _live_viewers:
            vid += "_x"
        _live_viewers[vid] = {"ts": time.time(), "q": None, "proc": None,
                              "eng": None, "vid": vid}
        return vid


def _viewer_bind(vid, **kw):
    with _live_lock:
        v = _live_viewers.get(vid)
        if v is None:
            return False
        for k, val in kw.items():
            v[k] = val
        v["ts"] = time.time()
        return True


def _viewer_touch(vid):
    """刷新活跃时间，返回该名额是否仍然有效（被回收过则返回 False）。"""
    with _live_lock:
        v = _live_viewers.get(vid)
        if v is None:
            return False
        v["ts"] = time.time()
        return True


def _viewer_release(vid):
    with _live_lock:
        _live_viewers.pop(vid, None)


def _reaper_loop():
    """后台定时回收：不依赖请求触发，断开的会话最多 10 秒内收干净。"""
    while True:
        time.sleep(10)
        try:
            _reap_viewers()
        except Exception:
            pass


threading.Thread(target=_reaper_loop, daemon=True, name="live-reaper").start()


@app.get("/api/live/{device_id}")
async def live(device_id: str):
    d = next((x for x in DEVICES if x["deviceId"] == device_id), None)
    if not d:
        raise HTTPException(404, "设备不存在")
    vid = _viewer_acquire()
    if vid is None:
        raise HTTPException(503, "同时在线的观看已经满了，请稍后再试")
    eng = get_engine(dict(d, recordDir=os.path.join(DATA_DIR, "recordings")))

    # 音频馈送：G711A 从独立管道（fd3）进 ffmpeg。单独线程负责写，
    # 队列超时就补 A-law 静音（0xD5），保证 ffmpeg 的音频输入永不枯竭、
    # 视频不会因等音频而卡住。
    a_r, a_w = os.pipe()
    audio_q = eng.subscribe_audio()
    _viewer_bind(vid, afd=a_w)
    os.set_blocking(a_w, False)
    a_stop = threading.Event()

    def _feed_audio():
        SILENCE = b"\xd5" * 320          # A-law 静音帧
        while not a_stop.is_set():
            try:
                chunk = audio_q.get(timeout=0.04)
            except Exception:
                chunk = None
            if chunk is None:
                if time.time() - (eng.last_audio_at or 0) > 2.0:
                    chunk = SILENCE      # 设备断音时补静音，防 ffmpeg 阻塞
            if chunk is None:
                continue
            try:
                os.write(a_w, chunk)
            except BlockingIOError:
                pass                     # 管道满：丢音频总比卡视频强
            except OSError:
                break

    threading.Thread(target=_feed_audio, daemon=True,
                     name="afeed-" + vid).start()

    async def gen():
        q = eng.subscribe()
        proc = None
        aligned = False
        last_yield = time.time()
        boot = eng.bootstrap_stream()   # 冷启动：用缓冲里的参数集+IDR 立即开播
        _viewer_bind(vid, eng=eng, q=q)   # 让外部回收时能一起收尾
        pending = b""   # 写进 ffmpeg 但没写完的尾巴，必须拼回下一块，丢字节会打碎 fMP4
        logging.info("[live] 会话开始 %s boot=%dB", vid, len(boot or b""))
        try:
            while True:
                # 阻塞队列必须放进线程池，否则会堵死整个事件循环
                try:
                    chunk = await asyncio.get_event_loop().run_in_executor(
                        None, functools.partial(_q_get, q, 5.0))
                except Exception:
                    chunk = None
                if chunk is None:
                    if time.time() - last_yield > 20:
                        logging.warning("[live] 引擎 20s 无帧，结束会话 %s", vid)
                        break
                    continue

                if not aligned:
                    if boot:
                        chunk = boot + chunk
                        boot = b""
                        aligned = True
                    else:
                        seg = eng.align_to_idr(chunk)
                        if not seg:
                            continue
                        chunk = seg
                        aligned = True

                if proc is None:
                    proc = subprocess.Popen(_ffmpeg_live_cmd(a_r), stdin=subprocess.PIPE,
                                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                            pass_fds=(a_r,),      # 音频管道
                                            bufsize=0)
                    # stdin/stdout 都设非阻塞，配合 select 才不会拖住事件循环
                    for _f in (proc.stdin, proc.stdout):
                        try:
                            os.set_blocking(_f.fileno(), False)
                        except Exception:
                            pass
                    _viewer_bind(vid, proc=proc)
                try:
                    # 必须非阻塞写：公网网关消费慢时 ffmpeg 会暂停读 stdin，
                    # 同步 write 会把整个事件循环一起卡死（曾导致全站无响应）。
                    # 部分写入的剩余部分记进 pending，下一轮拼在块头重试——
                    # 绝不能从码流中间丢字节（会打出 Invalid NAL，浏览器 MSE 卡死）。
                    data = pending + chunk if pending else chunk
                    pending = b""
                    wrote = await asyncio.get_event_loop().run_in_executor(
                        None, functools.partial(_write_pipe, proc.stdin, data, 1.5))
                    if wrote < len(data):
                        pending = bytes(data[wrote:])
                        # 积压超过 8MB 说明下游真的死了，丢弃整个观看会话
                        if len(pending) > 8 * 1024 * 1024:
                            logging.warning("[live] ffmpeg stdin 积压 >8MB，结束会话 %s", vid)
                            break
                except Exception as e:
                    logging.warning("[live] 写 ffmpeg stdin 异常: %s (%s)", e, vid)
                    break
                if wrote == 0 or wrote < len(data):
                    # 一直写不进去说明下游基本断流了，结束这个会话
                    if time.time() - last_yield > 25:
                        logging.warning("[live] 下游 25s 无进展，结束会话 %s", vid)
                        break

                buf = await asyncio.get_event_loop().run_in_executor(
                    None, functools.partial(_read_avail, proc.stdout))
                if buf:
                    last_yield = time.time()
                    if not _viewer_touch(vid):
                        logging.warning("[live] 名额已被回收，结束会话 %s", vid)
                        break      # 名额已被回收，说明这个连接早断了
                    yield buf
                if proc.poll() is not None:
                    logging.warning("[live] ffmpeg 提前退出 code=%s (%s)", proc.returncode, vid)
                    break
        finally:
            eng.unsubscribe(q)
            a_stop.set()
            eng.unsubscribe_audio(audio_q)
            for fd in (a_r, a_w):
                try:
                    os.close(fd)
                except OSError:
                    pass
            if proc:
                try:
                    err = b""
                    try:
                        err = os.read(proc.stderr.fileno(), 65536) or b""
                    except Exception:
                        pass
                    proc.kill()
                    proc.wait(timeout=2)
                    if proc.returncode not in (0, None, -9, 9):
                        logging.warning("[live] ffmpeg rc=%s stderr: %s",
                                        proc.returncode, err[-500:].decode("utf-8", "ignore"))
                except Exception:
                    pass
            _viewer_release(vid)

    return StreamingResponse(gen(), media_type="video/mp4",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


def _q_get(q, timeout):
    try:
        return q.get(timeout=timeout)
    except Exception:
        return None


def _write_pipe(fp, data, timeout=1.5):
    """把数据写进子进程 stdin，最多等 timeout 秒，返回实际写入字节数。

    用 select 等「可写」再写，绝不阻塞调用线程。超时返回已写入字节数；
    未写完的部分由调用方记入 pending 下一轮续写（码流字节绝不能丢）。
    """
    import select
    view = memoryview(data)
    total = 0
    end = time.time() + timeout
    fd = fp.fileno()
    while total < len(data):
        remain = end - time.time()
        if remain <= 0:
            break
        try:
            _, w, _ = select.select([], [fd], [], remain)
        except Exception:
            break
        if not w:
            break
        try:
            n = os.write(fd, view[total:])
            if n <= 0:
                break
            total += n
        except BlockingIOError:
            continue
        except Exception:
            break
    return total


def _read_avail(fp, size=131072, timeout=0.05):
    """有多少读多少，最多等 timeout 秒。用 os.read 是因为 fp.read(n) 会
    一直等到凑够 n 字节，管道上会白等甚至卡住。"""
    import select
    try:
        r, _, _ = select.select([fp], [], [], timeout)
        if r:
            return os.read(fp.fileno(), size) or b""
    except (OSError, ValueError):
        pass
    except Exception:
        pass
    return b""


@app.get("/api/_diag/{device_id}")
def _diag(device_id: str):
    """临时诊断：看看环形缓冲里有几块、参数集、IDR 分布、冷启动字节数。"""
    d = next((x for x in DEVICES if x["deviceId"] == device_id), None)
    if not d:
        raise HTTPException(404, "设备不存在")
    eng = get_engine(dict(d, recordDir=os.path.join(DATA_DIR, "recordings")))
    with eng.lock:
        items = list(eng.ring)
    types = {}
    idr_blocks = 0
    blocks = []
    for ts, payload in items:
        seen = []
        for st, en, t in eng.split_nals(payload):
            types[t] = types.get(t, 0) + 1
            seen.append(t)
        if 19 in seen or 20 in seen:
            idr_blocks += 1
        blocks.append({"ts": round(ts, 1), "len": len(payload), "nals": seen[:14]})
    boot = eng.bootstrap_stream()
    return {
        "ring_blocks": len(items),
        "idr_blocks": idr_blocks,
        "nal_types": types,
        "params": {"vps": len(eng.vps), "sps": len(eng.sps), "pps": len(eng.pps)},
        "boot_len": len(boot),
        "boot_head": boot[:24].hex(),
        "blocks": blocks[-12:],
    }


@app.get("/api/ptz/selftest/{device_id}")
async def ptz_selftest(device_id: str):
    """云台自检：依次让四个方向各转 3 秒，比较画面位移，判断机身有没有电机。"""
    return await asyncio.get_event_loop().run_in_executor(None, _ptz_selftest, device_id)


def _ptz_selftest(device_id: str):
    import cv2
    import numpy as np
    import random as _rnd
    d = next((x for x in DEVICES if x["deviceId"] == device_id), None)
    if not d:
        raise HTTPException(404, "设备不存在")
    eng = get_engine(dict(d, recordDir=os.path.join(DATA_DIR, "recordings")))

    def gray():
        raw = eng.snapshot_ring(time.time() - 6, time.time())
        if not raw:
            return None
        p = subprocess.run(
            ["ffmpeg", "-v", "error", "-f", "hevc", "-i", "pipe:0", "-frames:v", "1",
             "-f", "image2pipe", "-vcodec", "png", "pipe:1"],
            input=raw, capture_output=True, timeout=25)
        if not p.stdout:
            return None
        img = cv2.imdecode(np.frombuffer(p.stdout, np.uint8), cv2.IMREAD_GRAYSCALE)
        return img

    def diff(a, b):
        if a is None or b is None:
            return -1.0
        if a.shape != b.shape:
            b = cv2.resize(b, (a.shape[1], a.shape[0]))
        return float(cv2.absdiff(a, b).mean())

    a = gray()
    time.sleep(3)
    b = gray()
    baseline = diff(a, b)

    dirs = {}
    backend = "bridge"
    for name in ("left", "right", "up", "down"):
        s1 = gray()
        r = eng.send_ptz(name)
        backend = r.get("backend", backend)
        time.sleep(3)
        s2 = gray()
        eng.send_ptz("stop")
        dirs[name] = round(diff(s1, s2), 2)
        time.sleep(0.6)

    moved = max(dirs.values()) > max(baseline * 2.5, 6.0)
    return {
        "baseline": round(baseline, 2),
        "dirs": dirs,
        "moved": moved,
        "backend": backend,
        "verdict": "motor" if moved else "fixed",
        "note": ("检测到画面明显位移，机身有云台电机" if moved
                 else "四个方向画面位移都在噪声范围内，机身应为固定镜头"),
    }


@app.get("/api/detect/{device_id}")
async def detect_once(device_id: str):
    """对当前画面做一次即时检测，返回目标框（不写文件、不触发事件）。

    界面上的「叠加检测框」用它，够直观地看到 AI 在认什么。
    """
    return await asyncio.get_event_loop().run_in_executor(None, _detect_sync, device_id)


def _detect_sync(device_id: str):
    import cv2
    import numpy as np
    d = next((x for x in DEVICES if x["deviceId"] == device_id), None)
    if not d:
        raise HTTPException(404, "设备不存在")
    eng = get_engine(dict(d, recordDir=os.path.join(DATA_DIR, "recordings")))
    raw = eng.snapshot_ring(time.time() - 6, time.time())
    if not raw:
        raise HTTPException(503, "暂无画面")
    p = subprocess.run(
        ["ffmpeg", "-v", "error", "-f", "hevc", "-i", "pipe:0", "-frames:v", "1",
         "-f", "image2pipe", "-vcodec", "png", "pipe:1"],
        input=raw, capture_output=True, timeout=25)
    if not p.stdout:
        raise HTTPException(503, "解码失败")
    img = cv2.imdecode(np.frombuffer(p.stdout, np.uint8), cv2.IMREAD_COLOR)
    if img is None:
        raise HTTPException(503, "解码失败")
    evt = get_event_engine(eng)
    dets = evt.detector.detect(img, conf_thres=min(evt.cfg["personConf"], evt.cfg["objConf"]))
    h, w = img.shape[:2]
    objs = [{"name": x[6], "conf": round(float(x[4]), 3),
             "box": [int(x[0]), int(x[1]), int(x[2]), int(x[3])]} for x in dets]
    return {"w": w, "h": h, "ts": time.time(), "objects": objs, "count": len(objs)}


@app.get("/api/snapshot/{device_id}")
async def snapshot(device_id: str):
    return await asyncio.get_event_loop().run_in_executor(None, _snapshot_sync, device_id)


def _snapshot_sync(device_id: str):
    """返回当前实时画面单帧（JPEG），供界面预览与 AI 叠加显示。"""
    d = next((x for x in DEVICES if x["deviceId"] == device_id), None)
    if not d:
        raise HTTPException(404, "设备不存在")
    eng = get_engine(dict(d, recordDir=os.path.join(DATA_DIR, "recordings")))
    raw = eng.snapshot_ring(time.time() - 6, time.time())
    if not raw:
        raise HTTPException(503, "暂无画面")
    p = subprocess.run(
        ["ffmpeg", "-v", "error", "-f", "hevc", "-i", "pipe:0", "-frames:v", "1",
         "-f", "image2pipe", "-vcodec", "mjpeg", "-q:v", "4", "pipe:1"],
        input=raw, capture_output=True, timeout=25)
    if not p.stdout:
        raise HTTPException(503, "解码失败")
    return HTMLResponse(content=p.stdout, media_type="image/jpeg")


# ---------------- 事件 ----------------
@app.get("/api/timeline/days")
def api_timeline_days():
    """有录像的日期列表（倒序），供时间轨道的日期选择器用。"""
    root = os.path.join(DATA_DIR, "recordings")
    days = {}
    if os.path.isdir(root):
        for dev in os.listdir(root):
            dpath = os.path.join(root, dev)
            if not os.path.isdir(dpath):
                continue
            for day in os.listdir(dpath):
                if len(day) != 8 or not day.isdigit():
                    continue
                fp = os.path.join(dpath, day)
                if not os.path.isdir(fp):
                    continue
                n = size = 0
                for f in os.listdir(fp):
                    if f.endswith(".mp4"):
                        try:
                            size += os.path.getsize(os.path.join(fp, f))
                            n += 1
                        except OSError:
                            pass
                if not n:
                    continue
                key = "%s-%s-%s" % (day[:4], day[4:6], day[6:])
                d = days.setdefault(key, {"date": key, "files": 0, "size": 0, "devices": []})
                d["files"] += n
                d["size"] += size
                if dev not in d["devices"]:
                    d["devices"].append(dev)
    return sorted(days.values(), key=lambda x: x["date"], reverse=True)


@app.get("/api/timeline")
def api_timeline(date: str = "", device: str = ""):
    """某一天的录像时间轴：录像覆盖区间 + 事件点位。

    切片文件名就是该段的起始时刻（HHMMSS.mp4），因此不需要逐个 ffprobe：
    同一目录按时间排序后，一段的结束就是下一段的开始；最后一段用文件
    修改时间兜底（转码完成时间，误差在分钟内，拖进度条足够用）。
    """
    if not date:
        date = time.strftime("%Y-%m-%d")
    try:
        day = date.replace("-", "")
        base_ts = time.mktime(time.strptime(day, "%Y%m%d"))
    except Exception:
        raise HTTPException(400, "日期格式应为 YYYY-MM-DD")

    root = os.path.join(DATA_DIR, "recordings")
    segments = []
    devs = [device] if device else (os.listdir(root) if os.path.isdir(root) else [])
    for dev in devs:
        dpath = os.path.join(root, dev, day)
        if not os.path.isdir(dpath):
            continue
        files = []
        for f in os.listdir(dpath):
            if not f.endswith(".mp4"):
                continue
            stem = f[:-4].split("-")[0]
            if len(stem) != 6 or not stem.isdigit():
                continue
            try:
                st = os.stat(os.path.join(dpath, f))
            except OSError:
                continue
            files.append((stem, f, st))
        files.sort()
        for i, (stem, f, st) in enumerate(files):
            start = base_ts + int(stem[:2]) * 3600 + int(stem[2:4]) * 60 + int(stem[4:])
            if i + 1 < len(files):
                nx = files[i + 1][0]
                end = base_ts + int(nx[:2]) * 3600 + int(nx[2:4]) * 60 + int(nx[4:])
                if end <= start:
                    end = start + 60
            else:
                end = min(st.st_mtime, start + 3600)
                if end <= start + 5:
                    end = start + 60
            segments.append({
                "file": os.path.relpath(os.path.join(dpath, f), DATA_DIR),
                "device": dev,
                "name": f,
                "start": start,
                "end": end,
                "dur": end - start,
                "size": st.st_size,
            })

    # 当天事件
    events = []
    lo, hi = base_ts, base_ts + 86400
    for eng in all_engines():
        if device and eng.device_id != device:
            continue
        evt = get_event_engine(eng)
        for e in evt.events:
            ts = float(e.get("ts") or 0)
            if lo <= ts < hi:
                events.append({
                    "id": e.get("id"),
                    "ts": ts,
                    "time": time.strftime("%H:%M:%S", time.localtime(ts)),
                    "reason": e.get("reason", ""),
                    "targets": e.get("targets", []),
                    "video": e.get("video", ""),
                    "shots": e.get("shots", []),
                    "deviceId": e.get("deviceId", ""),
                })

    segments.sort(key=lambda x: x["start"])
    events.sort(key=lambda x: x["ts"])
    return {"date": date, "dayStart": base_ts, "segments": segments, "events": events}


@app.get("/api/events")
def api_events(limit: int = 100, device: str = ""):
    res = []
    for eng in all_engines():
        if device and eng.device_id != device:
            continue
        evt = get_event_engine(eng)
        res.extend(evt.list_events(limit))
    res.sort(key=lambda x: x.get("ts", 0), reverse=True)
    return res[:limit]


@app.get("/api/events/{event_id}/meta")
def api_event_meta(event_id: str):
    p = os.path.join(DATA_DIR, "events", event_id, "meta.json")
    if not os.path.exists(p):
        raise HTTPException(404, "事件不存在")
    return json.load(open(p))


@app.get("/data/{path:path}")
def data_file(path: str):
    full = os.path.normpath(os.path.join(DATA_DIR, path))
    if not full.startswith(os.path.realpath(DATA_DIR)) and not full.startswith(DATA_DIR):
        raise HTTPException(403, "非法路径")
    if not os.path.exists(full):
        raise HTTPException(404, "文件不存在")
    return FileResponse(full)


# ---------------- 播放（录像与事件共用） ----------------
def _probe_duration_sec(path, src_fmt=None):
    """用 ffprobe 取媒体时长（秒），失败返回 0。"""
    cmd = ["ffprobe", "-v", "error"]
    if src_fmt:
        cmd += ["-f", src_fmt]
    cmd += ["-show_entries", "format=duration", "-of",
            "default=noprint_wrappers=1:nokey=1", path]
    try:
        r = subprocess.run(cmd, capture_output=True, timeout=15, text=True)
        return float(r.stdout.strip() or 0)
    except Exception:
        return 0.0


def _prune_playcache(cache_dir, keep=60, max_age=7 * 86400):
    """播放缓存清理：最多保留 keep 个文件，且删除 7 天未访问的。"""
    try:
        files = []
        now = time.time()
        for name in os.listdir(cache_dir):
            p = os.path.join(cache_dir, name)
            if not os.path.isfile(p):
                continue
            try:
                st = os.stat(p)
            except OSError:
                continue
            if now - st.st_atime > max_age:
                os.remove(p)
                continue
            files.append((st.st_atime, st.st_size, p))
        files.sort()
        total = sum(s for _, s, _ in files)
        while len(files) > keep or total > 2 * 1024 ** 3:
            _, s, p = files.pop(0)
            try:
                os.remove(p)
                total -= s
            except OSError:
                pass
    except Exception:
        pass


@app.get("/api/play")
def api_play(f: str, t: float = 0):
    """播放录像/事件视频。

    平台产生的视频一律是 H.264 + faststart 的 MP4，浏览器原生就能播，
    这里直接把文件交给 FileResponse（支持 Range，可拖动进度条、秒开）。
    只有还没转完码的历史 HEVC 文件会返回 409，前端轮询后自动重试。
    """
    path = os.path.realpath(os.path.join(DATA_DIR, f))
    allow = (os.path.realpath(os.path.join(DATA_DIR, "recordings")),
             os.path.realpath(os.path.join(DATA_DIR, "events")))
    if not path.startswith(allow) or not os.path.isfile(path):
        raise HTTPException(404, "文件不存在")

    if path.endswith(".mp4") and _is_h264(path):
        return FileResponse(path, media_type="video/mp4",
                            headers={"Cache-Control": "max-age=3600",
                                     "Accept-Ranges": "bytes"})

    st = engine_mod.transcode_state(path)
    if st in (None, "failed"):
        engine_mod.enqueue_transcode(path)
        st = "queued"
    return JSONResponse(status_code=409, content={
        "status": st,
        "message": "该录像正在转码为 H.264，完成后自动可播",
    })


_CODEC_CACHE = {}


def _is_h264(path):
    """带缓存的编码探测：同一个文件（大小+修改时间不变）只探一次。"""
    try:
        st = os.stat(path)
        key = (path, st.st_size, int(st.st_mtime))
        hit = _CODEC_CACHE.get(key)
        if hit is not None:
            return hit
        ok = engine_mod.is_h264(path)
        if len(_CODEC_CACHE) > 800:
            _CODEC_CACHE.clear()
        _CODEC_CACHE[key] = ok
        return ok
    except Exception:
        return False


@app.get("/api/transcode")
def api_transcode():
    """转码队列状态，前端用来提示「还有几个在转」。"""
    return engine_mod.transcode_stats()


@app.get("/api/recordings/play")
def recordings_play(f: str):
    """兼容旧链接，转给通用播放接口。"""
    return api_play(f)


@app.get("/api/recordings")
def api_recordings(limit: int = 200):
    root = os.path.join(DATA_DIR, "recordings")
    items = []
    if os.path.isdir(root):
        for dirpath, _, files in os.walk(root):
            for f in files:
                if f.endswith((".mp4", ".hevc")):
                    p = os.path.join(dirpath, f)
                    try:
                        st = os.stat(p)
                    except OSError:
                        continue
                    state = engine_mod.transcode_state(p)
                    items.append({
                        "file": os.path.relpath(p, DATA_DIR),
                        "size": st.st_size,
                        "mtime": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(st.st_mtime)),
                        "ts": st.st_mtime,
                        "device": os.path.basename(os.path.dirname(os.path.dirname(p))),
                        # 只有 H.264 的 mp4 才能网页直接播；其余要给「转码中」提示
                        "ready": f.endswith(".mp4") and state not in ("queued", "running"),
                        "state": state or ("ready" if f.endswith(".mp4") else "queued"),
                    })
    items.sort(key=lambda x: x["ts"], reverse=True)
    return items[:limit]


@app.post("/api/recordings/backup")
async def backup_all(request: Request):
    body = await request.json() if await request.body() else {}
    files = body.get("files")
    root = os.path.join(DATA_DIR, "recordings")
    n = 0
    if not files:
        for dirpath, _, fs in os.walk(root):
            for f in fs:
                if f.endswith((".mp4", ".hevc")):
                    storage.backup_queue.put_nowait((os.path.join(dirpath, f), "recording"))
                    n += 1
    else:
        for rel in files:
            p = os.path.join(DATA_DIR, rel)
            if os.path.exists(p):
                storage.backup_queue.put_nowait((p, "recording"))
                n += 1
    return {"queued": n}


# ---------------- 云盘 ----------------
@app.get("/api/storage")
def api_storage():
    cfg = storage.load_cfg()
    safe = dict(cfg)
    c = dict(safe.get("config", {}))
    for k in ("password", "secretKey", "accessKey", "refreshToken", "appKey",
              "authorization", "cookie", "accessToken"):
        if k in c and c[k]:
            c[k] = "******"
    safe["config"] = c
    safe["recent"] = storage.recent_uploads(30)
    safe["queue"] = storage.backup_queue.qsize()
    safe["drivers"] = list(storage.DRIVERS.keys())
    return safe


@app.post("/api/storage")
async def api_storage_save(request: Request):
    body = await request.json()
    old = storage.load_cfg()
    new = dict(old)
    for k in ("driver", "enabled", "backupRecordings", "uploadEvents", "keepLocal"):
        if k in body:
            new[k] = body[k]
    if "config" in body:
        merged = dict(old.get("config", {}))
        for k, v in body["config"].items():
            if v == "******":       # 掩码值表示不修改
                continue
            merged[k] = v
        new["config"] = merged
    storage.save_cfg(new)
    return {"ok": True}


@app.post("/api/storage/test")
async def api_storage_test(request: Request):
    body = await request.json() if await request.body() else {}
    cfg = storage.load_cfg()
    if body:
        cfg = dict(cfg)
        old = dict(cfg.get("config", {}))
        if "driver" in body:
            cfg["driver"] = body["driver"]
        if body.get("config"):
            merged = dict(old)
            for k, v in body["config"].items():
                if v == "******":       # 掩码值代表“沿用已保存的密钥”
                    continue
                merged[k] = v
            cfg["config"] = merged
        cfg["enabled"] = True           # 测试时即使没启用也允许连一次
    ok, msg = storage.test_driver(cfg)
    return {"ok": ok, "message": msg}


if os.environ.get("UM_DIAG") == "1":
    import diag_route
    diag_route.register(app, sys.modules[__name__])


@app.get("/api/health")
def health():
    return {"ok": True, "devices": len(DEVICES), "time": time.strftime("%Y-%m-%d %H:%M:%S")}



# ---------------- 摄像头接入凭据（多设备管理） ----------------
def _mask(tok: str) -> str:
    if len(tok) > 14:
        return tok[:8] + "****" + tok[-4:]
    return "****" if tok else ""


def _stop_engine(dev_id: str):
    """停掉指定设备的拉流引擎与事件检测，并清出缓存。"""
    eng = engine_mod._engines.get(dev_id)
    if not eng:
        return
    try:
        get_event_engine(eng).stop()
    except Exception:
        pass
    eng.stop()
    engine_mod._engines.pop(dev_id, None)
    try:
        events.get_event_engines_map().pop(dev_id, None)
    except Exception:
        pass


@app.get("/api/device/access")
def device_access_get():
    return {"devices": [{
        "name": d.get("name", ""),
        "deviceId": d.get("deviceId", ""),
        "wsHost": d.get("wsHost", ""),
        "relayServer": d.get("relayServer", ""),
        "channelNo": d.get("channelNo", ""),
        "tokenMasked": _mask(d.get("token", "")),
        "tokenSet": bool(d.get("token")),
    } for d in DEVICES]}


def _probe_device(cfg: dict) -> str:
    """用给定凭据开一条临时拉流会话测试，返回错误说明；空串表示通过。

    注意：摄像头同一时刻只允许一个会话，探测会短暂顶掉正在运行的
    直播会话，引擎几秒内会自动重连回来。
    """
    import ltclient
    try:
        ws = ltclient.open_stream(
            cfg.get("wsHost", ""), cfg.get("token", ""), cfg.get("deviceId", ""),
            cfg.get("channelNo", ""), "", cfg.get("relayServer", ""),
            cfg.get("name", ""))
        ws.settimeout(10)
        got = False
        t0 = time.time()
        while time.time() - t0 < 8:
            try:
                data = ws.recv()
            except Exception as e:
                return "连接后读取失败：%s（多半是 token 无效或已过期）" % e
            if isinstance(data, (bytes, bytearray)) and len(data) > 100:
                got = True
                break
        try:
            ws.close()
        except Exception:
            pass
        if not got:
            return "连接成功但 8 秒内没有收到音视频数据，请核对 deviceId/token"
    except Exception as e:
        return "连接失败：%s" % e
    return ""


@app.post("/api/device/access/test")
async def device_access_test(request: Request):
    body = await request.json()
    for k in ("deviceId", "token", "wsHost"):
        if not (body.get(k) or "").strip():
            return {"ok": False, "error": "缺少必填项：%s" % k}
    err = await asyncio.get_event_loop().run_in_executor(None, _probe_device, body)
    return {"ok": not err, "error": err}


def _restart_engine(d: dict):
    """按新配置重启一台设备的引擎（原引擎存在则先停）。"""
    _stop_engine(d.get("deviceId"))
    start_device(d)


@app.post("/api/device/access")
async def device_access_save(request: Request):
    """新增或更新一台摄像头（以 deviceId 为键）。

    token 留空 = 沿用该设备已保存的 token；新增设备 token 必填。
    """
    global DEVICES
    body = await request.json()
    for k in ("deviceId", "wsHost"):
        if not (body.get(k) or "").strip():
            return {"ok": False, "error": "缺少必填项：%s" % k}
    dev_id = body["deviceId"].strip()
    old = next((x for x in DEVICES if x.get("deviceId") == dev_id), None)
    token = (body.get("token") or "").strip() or (old or {}).get("token", "")
    if not token:
        return {"ok": False, "error": "token 为空（新设备必须填写）"}

    cfg = {
        "name": (body.get("name") or "摄像头").strip(),
        "deviceId": dev_id,
        "wsHost": body["wsHost"].strip(),
        "token": token,
        "channelNo": (body.get("channelNo") or "").strip(),
        "shareId": "",
        "relayServer": (body.get("relayServer") or "").strip(),
    }
    if old:
        for k in ("autoRecord", "aiEnabled", "ai", "ptz", "ptzBackend",
                  "ptzFallback", "ringSeconds", "maxRingBytes", "segBytes",
                  "segSeconds", "recHeight", "recBitrate", "recordDir"):
            if k in old:
                cfg[k] = old[k]

    err = await asyncio.get_event_loop().run_in_executor(None, _probe_device, cfg)
    if err:
        return {"ok": False, "error": err}

    if old:
        DEVICES[DEVICES.index(old)] = cfg
    else:
        DEVICES.append(cfg)
    save_devices(DEVICES)
    await asyncio.get_event_loop().run_in_executor(None, _restart_engine, cfg)
    return {"ok": True, "deviceId": dev_id, "total": len(DEVICES)}


@app.delete("/api/device/access/{device_id}")
async def device_access_delete(device_id: str):
    """删除一台摄像头：停引擎并移出配置（录像/AI 事件文件保留）。"""
    global DEVICES
    idx = next((i for i, x in enumerate(DEVICES)
                if x.get("deviceId") == device_id), None)
    if idx is None:
        return {"ok": False, "error": "设备不存在"}
    await asyncio.get_event_loop().run_in_executor(None, _stop_engine, device_id)
    DEVICES.pop(idx)
    save_devices(DEVICES)
    return {"ok": True, "total": len(DEVICES)}
