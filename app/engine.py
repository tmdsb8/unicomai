"""设备流引擎：单条 WebSocket 拉流，供直播/录制/AI 检测共享。

设计要点
--------
联通摄像头同一时刻只允许一个会话拉流，多进程会互相挤掉。因此本模块为每台设备
维护唯一一条连接，在本进程内复用：
  - 原始 H.265 字节流 -> 分片落盘（录制）、推给直播客户端、送 AI 检测
  - 内存环形缓冲保留最近 N 秒数据，命中事件后从中截取前 N 秒作为事件视频

录像格式
--------
摄像机出的是 H.265/HEVC 裸流，浏览器、手机相册、多数云盘预览都不认。
切片关闭后立即转码成 H.264 的 MP4（720p，faststart），原文件删除，
这样本地/云盘/网页三处都能直接点开即播。
"""
import hashlib
import json
import logging
import os
import queue
import random
import subprocess
import threading
import time
import urllib.parse
from collections import deque

import ltclient

# 单帧私有头长度；首包内含 URL 编码的元数据，需跳过直到首个起始码
FRAME_HDR = 16
START_CODE = b"\x00\x00\x00\x01"


class DeviceEngine:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.name = cfg.get("name", "camera")
        self.device_id = cfg.get("deviceId", "")
        self.ws_host = cfg.get("wsHost", "")
        self.token = cfg.get("token", "")
        self.channel_no = cfg.get("channelNo", "")
        self.share_id = cfg.get("shareId", "")
        self.relay = cfg.get("relayServer", "")

        self.lock = threading.Lock()
        self.ptz_lock = threading.Lock()   # 云台帧单独串行，避免与重连撞车
        self.ws = None
        self.connected = False
        self.last_error = ""
        self.started_at = None
        self.total_bytes = 0
        self.last_data_at = 0.0

        # 环形缓冲：保存最近 ring_seconds 秒的原始字节（含帧边界）
        self.ring_seconds = cfg.get("ringSeconds", 45)
        self.ring = deque()          # [(ts, bytes)]
        self.vps = b""               # 最近一次 VPS(32)
        self.sps = b""               # 最近一次 SPS(33)
        self.pps = b""               # 最近一次 PPS(34)
        self.ring_bytes = 0
        self.max_ring_bytes = cfg.get("maxRingBytes", 80 * 1024 * 1024)

        # 订阅者：队列用于直播转发（每个消费者独立队列，满了就丢最旧）
        self.subscribers = set()
        self.sub_lock = threading.Lock()

        # 音频（G711A 8kHz 单声道）：设备以 0x62 标签帧推送，320B/帧、25帧/秒。
        # 与视频分开缓存/分发；audio_ring 供直播冷启动预填 ffmpeg 音频输入。
        self.audio_subscribers = set()
        self.audio_ring = deque()          # [(ts, g711a_bytes)]
        self.audio_seconds = 3.0
        self.audio_bytes_total = 0
        self.audio_frames = 0
        self.last_audio_at = 0.0

        # 录制
        self.ctrl_log = deque(maxlen=80)   # 设备上行控制帧留档
        self.record_enabled = cfg.get("autoRecord", True)
        self.record_dir = cfg.get("recordDir", os.path.join(os.environ.get("UNICOMAI_HOME", "/opt/unicomai"), "data/recordings"))
        self.seg_bytes = cfg.get("segBytes", 50 * 1024 * 1024)
        self.seg_seconds = cfg.get("segSeconds", 300)     # 单段最长 5 分钟
        self.rec_height = cfg.get("recHeight", 720)       # 转码后高度
        self.rec_bitrate = cfg.get("recBitrate", "1200k")
        self._rec_fp = None
        self._rec_size = 0
        self._rec_path = ""
        self._rec_start = 0.0
        self._rec_afp = None            # 音频伴生文件（G711A 裸流）
        self._rec_a_path = ""

        self._stop = threading.Event()
        self._thread = None

        # 统计
        self.fps_window = deque(maxlen=60)

    # ---------------- 生命周期 ----------------
    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="eng-" + self.device_id, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._close()
        self._close_record()

    def _close(self):
        with self.lock:
            ws, self.ws = self.ws, None
            self.connected = False
        if ws:
            try:
                ws.close()
            except Exception:
                pass

    def _close_record(self):
        if self._rec_fp:
            try:
                self._rec_fp.close()
            except Exception:
                pass
            self._rec_fp = None
            self._rec_size = 0
        if self._rec_afp:
            try:
                self._rec_afp.close()
            except Exception:
                pass
            self._rec_afp = None

    # ---------------- 主循环 ----------------
    def _run(self):
        backoff = 2
        while not self._stop.is_set():
            try:
                ws = ltclient.open_stream(
                    self.ws_host, self.token, self.device_id,
                    self.channel_no, self.share_id, self.relay, self.name,
                )
                ws.settimeout(20)
            except Exception as e:
                self.last_error = "连接失败: %s" % e
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue

            with self.lock:
                self.ws = ws
                self.connected = True
                self.started_at = self.started_at or time.time()
                self.last_error = ""

            first_packet = True
            backoff = 2
            try:
                while not self._stop.is_set():
                    try:
                        data = ws.recv()
                    except Exception as e:
                        self.last_error = "读取中断: %s" % e
                        break
                    if isinstance(data, str):
                        self._log_ctrl(data)
                        continue
                    if not isinstance(data, (bytes, bytearray)):
                        continue
                    if len(data) < FRAME_HDR:
                        # 短帧基本都是控制信令（云台应答、中继状态），留档便于排查
                        self._log_ctrl(data)
                        continue
                    b0, b1 = data[0], data[1]
                    if b0 == 0x00 and b1 == 0x62:
                        # FLV 式 tag：0x62 = 音频（G711A 8kHz 单声道）。
                        # 16 字节私有头 + 320 字节 A-law 数据，25 帧/秒。
                        self._on_audio(bytes(data[FRAME_HDR:]))
                    elif b0 == 0x00 and b1 == 0x63:
                        payload = bytes(data[FRAME_HDR:])
                        # 包体前面可能挂着 URL 编码的 JSON 元数据
                        # （如 %7B%22sync%22%3A1%2C%22ipcamTime%22...%7D），
                        # 必须剥到首个起始码，否则会混进码流污染解码器。
                        idx = payload.find(START_CODE)
                        if idx < 0:
                            continue
                        if idx > 0:
                            payload = payload[idx:]
                        self._on_video(payload)
                    else:
                        # 0x04/0x61/0x25/0x46 等：控制应答与元数据，留档
                        self._log_ctrl(data)
            finally:
                self._close()

            if not self._stop.is_set():
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def _log_ctrl(self, data):
        """记录设备上行控制帧（云台应答等），供 /api/wslog 排查。"""
        try:
            if isinstance(data, str):
                rec = {"ts": time.time(), "cmd": None, "dir": "text", "text": data[:400]}
            else:
                raw = bytes(data)
                cmd = raw[0] if raw else None
                try:
                    txt = urllib.parse.unquote(raw[1:].decode("utf-8", "replace"))
                except Exception:
                    txt = raw[:80].hex()
                rec = {"ts": time.time(), "cmd": cmd, "dir": "bin", "len": len(raw),
                       "text": txt[:400]}
            self.ctrl_log.append(rec)
        except Exception:
            pass

    # ---------------- 数据处理 ----------------
    @staticmethod
    def split_nals(payload: bytes):
        """按起始码拆分 NAL，返回 [(start, end, nal_type)]。"""
        pos = []
        idx = 0
        n = len(payload)
        while True:
            i = payload.find(START_CODE, idx)
            if i < 0 or i + 6 > n:
                break
            pos.append((i, (payload[i + 4] >> 1) & 0x3F))
            idx = i + 4
        out = []
        for k, (st, t) in enumerate(pos):
            en = pos[k + 1][0] if k + 1 < len(pos) else n
            out.append((st, en, t))
        return out

    def _cache_params(self, payload: bytes):
        """缓存最近一次参数集，供中途接入时补发。"""
        for st, en, t in self.split_nals(payload):
            if t == 32:
                self.vps = payload[st:en]
            elif t == 33:
                self.sps = payload[st:en]
            elif t == 34:
                self.pps = payload[st:en]

    def params_prefix(self) -> bytes:
        return self.vps + self.sps + self.pps

    def align_to_idr(self, payload: bytes):
        """从给定数据里找到 IDR 起点，并在前面补上缓存的参数集。

        返回可直接送解码器的字节流；不含 IDR 时返回空。
        """
        self._cache_params(payload)
        for st, en, t in self.split_nals(payload):
            if t in (19, 20):
                return self.params_prefix() + payload[st:]
        return b""

    def bootstrap_stream(self) -> bytes:
        """从环形缓冲里取出「参数集 + 最近一个 IDR」起始的码流，用于直播冷启动。

        摄像机 GOP 可能很长（数十秒才一个 IDR），实时等 IDR 会让首帧等待过久；
        缓冲里通常已包含最近一次 IDR，直接取用可立即开播。

        注意：返回值从 IDR 处截断，该分片内 IDR 之前的参数集不会随之带出，
        因此必须无条件在前面补上缓存参数集，否则解码器会报
        「PPS id out of range / SPS does not exist」。
        """
        with self.lock:
            items = list(self.ring)
        if not items:
            return b""
        # 从后往前找最近的 IDR 所在分片
        for ts, payload in reversed(items):
            for st, en, t in self.split_nals(payload):
                if t in (19, 20):
                    return self.params_prefix() + payload[st:]
        # 缓冲内无 IDR：退回缓存参数集（有待后续 IDR 补齐）
        return self.params_prefix()

    @staticmethod
    def _has_params(data: bytes) -> bool:
        idx = 0
        while True:
            i = data.find(START_CODE, idx)
            if i < 0 or i + 6 > len(data):
                return False
            t = (data[i + 4] >> 1) & 0x3F
            if t in (33, 34):
                return True
            idx = i + 4

    def _on_video(self, payload: bytes):
        now = time.time()
        self.total_bytes += len(payload)
        self.last_data_at = now
        self.fps_window.append(now)

        # 缓存参数集（VPS/SPS/PPS 只在 GOP 开头出现一次）
        self._cache_params(payload)

        # 环形缓冲
        self.ring.append((now, payload))
        self.ring_bytes += len(payload)
        while self.ring and (now - self.ring[0][0] > self.ring_seconds or self.ring_bytes > self.max_ring_bytes):
            _, old = self.ring.popleft()
            self.ring_bytes -= len(old)

        # 录制落盘
        if self.record_enabled:
            self._write_record(payload)

        # 分发给直播订阅者
        with self.sub_lock:
            dead = []
            for q in self.subscribers:
                try:
                    q.put_nowait(payload)
                except queue.Full:
                    try:
                        q.get_nowait()
                        q.put_nowait(payload)
                    except Exception:
                        dead.append(q)
            for q in dead:
                self.subscribers.discard(q)

    def _on_audio(self, payload: bytes):
        """收到一帧 G711A 音频：进环形缓冲并分发给音频订阅者。"""
        if not payload:
            return
        now = time.time()
        self.audio_bytes_total += len(payload)
        self.audio_frames += 1
        self.last_audio_at = now
        self.audio_ring.append((now, payload))
        while self.audio_ring and now - self.audio_ring[0][0] > self.audio_seconds:
            self.audio_ring.popleft()
        # 录像音频伴生流：与视频切片同步开合，转码时合成 AAC 音轨
        if self.record_enabled and self._rec_fp is not None:
            try:
                if self._rec_afp is None:
                    self._rec_a_path = self._rec_path[:-5] + ".alaw"
                    self._rec_afp = open(self._rec_a_path, "wb")
                self._rec_afp.write(payload)
            except Exception:
                if self._rec_afp:
                    try:
                        self._rec_afp.close()
                    except Exception:
                        pass
                self._rec_afp = None
        with self.sub_lock:
            dead = []
            for q in self.audio_subscribers:
                try:
                    q.put_nowait(payload)
                except queue.Full:
                    try:
                        q.get_nowait()
                        q.put_nowait(payload)
                    except Exception:
                        dead.append(q)
            for q in dead:
                self.audio_subscribers.discard(q)

    def recent_audio(self, seconds: float = 1.0) -> bytes:
        """取最近 N 秒的 G711A 音频（直播冷启动预填 ffmpeg 输入用）。"""
        cutoff = time.time() - seconds
        return b"".join(p for ts, p in self.audio_ring if ts >= cutoff)

    def subscribe_audio(self) -> queue.Queue:
        q = queue.Queue(maxsize=200)
        with self.sub_lock:
            self.audio_subscribers.add(q)
        return q

    def unsubscribe_audio(self, q: queue.Queue):
        with self.sub_lock:
            self.audio_subscribers.discard(q)

    # ---------------- 语音对讲 ----------------
    TALK_DATA = b"\x01"     # 与官方 webPlayer 对齐：0x01=对讲音频, 0x02=停止
    TALK_STOP = b"\x02"

    def send_talk(self, pcm16: bytes) -> dict:
        """把浏览器采集的 PCM16LE 8kHz 单声道数据发给摄像头喇叭。

        官方 webPlayer 的做法：麦克风降采样到 8kHz/16bit，攒 4800 字节
        （300ms）后以 [0x01]+数据 发在同一条 WebSocket 上；停止时发 [0x02]。
        """
        with self.lock:
            ws = self.ws
        if ws is None:
            return {"ok": False, "error": "设备未连接"}
        if not pcm16 or len(pcm16) > 19200:
            return {"ok": False, "error": "音频块大小非法"}
        try:
            with self.ptz_lock:
                ws.send(self.TALK_DATA + bytes(pcm16))
            return {"ok": True, "bytes": len(pcm16)}
        except Exception as e:
            return {"ok": False, "error": "发送失败: %s" % e}

    def stop_talk(self) -> dict:
        with self.lock:
            ws = self.ws
        if ws is None:
            return {"ok": False, "error": "设备未连接"}
        try:
            with self.ptz_lock:
                ws.send(self.TALK_STOP)
            return {"ok": True}
        except Exception as e:
            return {"ok": False, "error": "发送失败: %s" % e}

    def _write_record(self, payload: bytes):
        """把原始 H.265 码流按「时长或体积」切片落盘。

        切片闭包时立刻丢给转码队列 —— 裸流本身不可播，转成 H.264 MP4 才好用。
        """
        try:
            now = time.time()
            if self._rec_fp is None:
                d = os.path.join(self.record_dir, self.device_id, time.strftime("%Y%m%d"))
                os.makedirs(d, exist_ok=True)
                self._rec_path = os.path.join(d, time.strftime("%H%M%S") + ".hevc")
                self._rec_fp = open(self._rec_path, "wb")
                self._rec_size = 0
                self._rec_start = now
                # 段首补一次参数集，让每个切片都能独立解码（否则开头几秒花屏）
                pre = self.params_prefix()
                if pre:
                    self._rec_fp.write(pre)
                    self._rec_size += len(pre)
            self._rec_fp.write(payload)
            self._rec_size += len(payload)
            if self._rec_size >= self.seg_bytes or (now - self._rec_start) >= self.seg_seconds:
                self._rec_fp.close()
                self._rec_fp = None
                if self._rec_afp:            # 音频伴生流与视频切片同步收口
                    try:
                        self._rec_afp.close()
                    except Exception:
                        pass
                self._rec_afp = None
                threading.Thread(target=enqueue_transcode, args=(self._rec_path,),
                                 daemon=True).start()
        except Exception:
            self._close_record()

    # ---------------- 对外能力 ----------------
    @staticmethod
    def extract_aligned_gop(payload: bytes) -> bytes:
        """截取「最近一组参数集 + IDR」开始的码流。

        H.265 解码必须从 VPS(32)/SPS(33)/PPS(34) 加 IDR(19/20) 处开始，
        否则会报 "PPS id out of range" 或 "Could not find ref with POC"。
        返回可直接送解码器的字节流；找不到返回空。
        """
        start = len(payload)
        idx = 0
        n = len(payload)
        while True:
            i = payload.find(START_CODE, idx)
            if i < 0 or i + 6 > n:
                break
            nal_type = (payload[i + 4] >> 1) & 0x3F
            if nal_type in (32, 33, 34, 19, 20):
                start = i          # 不断前移到最近的参数集/IDR 起点
            idx = i + 4
        if start >= len(payload):
            return b""
        seg = payload[start:]
        # 校验：段内必须同时含 IDR 与 SPS/PPS，才是可解码起点
        has_idr = has_pps = False
        idx = 0
        while True:
            i = seg.find(START_CODE, idx)
            if i < 0 or i + 6 > len(seg):
                break
            t = (seg[i + 4] >> 1) & 0x3F
            if t in (19, 20):
                has_idr = True
            if t in (33, 34):
                has_pps = True
            idx = i + 4
        return seg if (has_idr and has_pps) else b""

    def subscribe(self) -> queue.Queue:
        q = queue.Queue(maxsize=200)
        with self.sub_lock:
            self.subscribers.add(q)
        return q

    def unsubscribe(self, q: queue.Queue):
        with self.sub_lock:
            self.subscribers.discard(q)

    def snapshot_ring(self, start_ts: float, end_ts: float) -> bytes:
        """截取时间窗口内的原始字节，用于生成事件视频。

        截出来的片段起点通常是 P 帧，若不带参数集解码器会直接报错，
        因此开头补上缓存的 VPS/SPS/PPS；同时把窗口起点对齐到首个 IDR，
        避免开头出现花屏（依赖帧缺失）。
        """
        with self.lock:
            items = list(self.ring)
        picked = [p for ts, p in items if start_ts <= ts <= end_ts]
        if not picked:
            return b""
        # 优先从窗口内的 IDR 开始，画面才干净
        begin = 0
        for i, payload in enumerate(picked):
            if any(t in (19, 20) for _, _, t in self.split_nals(payload)):
                begin = i
                break
        body = b"".join(picked[begin:])
        return self.params_prefix() + body

    # ---------------- 云台 ----------------
    PTZ_DIRS = {"left": 1, "right": 2, "up": 3, "down": 4, "stop": 0}

    def send_ptz(self, action: str) -> dict:
        """向摄像头下发云台指令。

        首选走 recplayer 桥接（ptzbridge），因为本机设备只对那条已打通的
        会话响应方向指令；直连 WebSocket 的帧设备不认（见 ptzbridge 模块说明）。
        桥接不可用时才回落到直连帧，保证老行为不会退化。
        """
        mode = self.cfg.get("ptzBackend", "bridge")
        if mode == "bridge":
            try:
                from ptzbridge import BRIDGE
                res = BRIDGE.send(self.device_id, action)
                if res.get("ok"):
                    res["action"] = action
                    res["backend"] = "bridge"
                    return res
                self.last_error = "云台桥接失败: %s" % res.get("error")
            except Exception as e:
                self.last_error = "云台桥接异常: %s" % e
            if not self.cfg.get("ptzFallback", True):
                return {"ok": False, "error": self.last_error}
        return self._send_ptz_direct(action)

    def _send_ptz_direct(self, action: str) -> dict:
        """直接在拉流的 WebSocket 上发云台帧（备用通道）。

        帧首字节是命令号，其后是 URL 编码的 JSON：
          0x07 + {"msgSession":<随机>,"channelNo":"","value":1..4}  左/右/上/下
          0x06 + {"msgSession":<随机>,"channelNo":"","value":0}     停止
          0x08 + {"msgSession":<随机>,"channelNo":""}               复位
        """
        with self.lock:
            ws = self.ws
        if ws is None:
            return {"ok": False, "error": "设备未连接"}

        sess = random.randint(10000000, 999999999)
        if action == "reset":
            frame = b"\x08" + self._ptz_payload({"msgSession": sess, "channelNo": ""})
        else:
            value = self.PTZ_DIRS.get(action)
            if value is None:
                return {"ok": False, "error": "未知方向: %s" % action}
            tag = b"\x07" if value else b"\x06"
            frame = tag + self._ptz_payload(
                {"msgSession": sess, "channelNo": "", "value": value})

        try:
            with self.ptz_lock:          # 与重连/心跳错开，避免帧被截断
                ws.send(frame)
            self.ctrl_log.append({"ts": time.time(), "cmd": "TX", "dir": "out",
                                  "len": len(frame), "text": frame[1:120].decode("latin-1")})
        except Exception as e:
            return {"ok": False, "error": "下发失败: %s" % e}
        return {"ok": True, "action": action, "session": sess, "bytes": len(frame),
                "backend": "direct"}

    @staticmethod
    def _ptz_payload(obj: dict) -> bytes:
        txt = json.dumps(obj, separators=(",", ":"), ensure_ascii=False)
        return urllib.parse.quote(txt, safe="").encode()

    def send_raw(self, spec: dict) -> dict:
        """调试用：按原始规格往当前 WebSocket 发一帧。

        spec 支持两种写法：
          {"cmd": 7, "obj": {...}}   -> [cmd] + urlencode(json)
          {"hex": "07257b..."}       -> 原样发送
        用于在不重启服务的前提下穷举云台协议变体。
        """
        with self.lock:
            ws = self.ws
        if ws is None:
            return {"ok": False, "error": "设备未连接"}
        try:
            if spec.get("hex"):
                frame = bytes.fromhex(spec["hex"])
            elif spec.get("raw"):
                frame = spec["raw"].encode("latin-1")
            else:
                cmd = int(spec.get("cmd", 7))
                obj = spec.get("obj") or {}
                frame = bytes([cmd]) + self._ptz_payload(obj)
            with self.ptz_lock:
                ws.send(frame)
            return {"ok": True, "bytes": len(frame), "preview": frame[:60].decode("latin-1")}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def status(self) -> dict:
        now = time.time()
        fps = 0
        if len(self.fps_window) > 1:
            span = self.fps_window[-1] - self.fps_window[0]
            if span > 0:
                fps = round((len(self.fps_window) - 1) / span, 1)
        return {
            "name": self.name,
            "deviceId": self.device_id,
            "connected": self.connected,
            "lastError": self.last_error,
            "fps": fps,
            "totalMB": round(self.total_bytes / 1024 / 1024, 1),
            "idleSec": round(now - self.last_data_at, 1) if self.last_data_at else None,
            "uptimeSec": int(now - self.started_at) if self.started_at else 0,
            "recordEnabled": self.record_enabled,
            "liveViewers": len(self.subscribers),
            "ringMB": round(self.ring_bytes / 1024 / 1024, 1),
            "audioConnected": (time.time() - self.last_audio_at) < 3 if self.last_audio_at else False,
            "audioKB": round(self.audio_bytes_total / 1024, 1),
        }


# ---------------- 转码队列：HEVC -> H.264 ----------------
log = logging.getLogger("engine")

_TC_Q = queue.Queue()
_TC_SEEN = set()
_TC_STATE = {}                 # path -> queued / running / ready / failed
_TC_KIND = {}                  # path -> recording / event
_TC_LOCK = threading.Lock()
_TC_WORKERS = 2                # 容器内存上限 8GB，4 路并发会和直播转码一起触发 OOM


def transcode_state(path):
    with _TC_LOCK:
        return _TC_STATE.get(path)


def transcode_stats():
    with _TC_LOCK:
        vals = list(_TC_STATE.values())
    return {
        "queued": vals.count("queued"),
        "running": vals.count("running"),
        "ready": vals.count("ready"),
        "failed": vals.count("failed"),
    }


def probe_vcodec(path):
    """探测视频编码名，失败返回空串。"""
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=codec_name", "-of", "default=nw=1:nk=1", path],
            capture_output=True, timeout=20, text=True)
        out = (r.stdout or "").strip()
        return out.splitlines()[0] if out else ""
    except Exception:
        return ""


def is_h264(path):
    return probe_vcodec(path) == "h264"


def enqueue_transcode(path, kind="recording"):
    """把待转码文件排进队列（同路径只排一次）。

    kind=recording 转完会顺带进云盘备份队列；kind=event 只转换不上传
    （事件在生成时已经单独上传过了）。
    """
    if not path or not os.path.exists(path):
        return False
    with _TC_LOCK:
        if path in _TC_SEEN:
            return False
        _TC_SEEN.add(path)
        _TC_STATE[path] = "queued"
        _TC_KIND[path] = kind
    _TC_Q.put(path)
    return True


def _tc_worker():
    while True:
        path = _TC_Q.get()
        with _TC_LOCK:
            _TC_STATE[path] = "running"
        ok = False
        try:
            with _TC_LOCK:
                kind = _TC_KIND.get(path, "recording")
            ok = transcode_to_h264(path, kind)
        except Exception as e:
            log.warning("转码异常 %s: %s", path, e)
        finally:
            with _TC_LOCK:
                _TC_STATE[path] = "ready" if ok else "failed"
                if not ok:
                    _TC_SEEN.discard(path)      # 失败的下次巡检再试
            _TC_Q.task_done()


def _ensure_workers():
    for i in range(_TC_WORKERS):
        t = threading.Thread(target=_tc_worker, name="tc-%d" % i, daemon=True)
        t.start()


_ensure_workers()


def _mp4_duration(path):
    try:
        r = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration",
                            "-of", "default=nw=1:nk=1", path],
                           capture_output=True, timeout=30, text=True)
        return float((r.stdout or "0").strip() or 0)
    except Exception:
        return 0.0


def _acquire_tc_lock(path):
    """给待转码文件加进程级排他锁，防止孤儿进程重复写同一个临时文件。"""
    try:
        import fcntl
        key = hashlib.md5(path.encode("utf-8")).hexdigest()[:16]
        fp = open("/tmp/um_tc_%s.lock" % key, "w")
        fcntl.flock(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fp
    except OSError:
        return False          # 已有别人在转
    except Exception:
        return None           # 拿不到锁也能继续（环境不支持）


def transcode_to_h264(path, kind="recording"):
    """把 HEVC 裸流 / HEVC 封装的 MP4 转成浏览器和云盘都认的 H.264 MP4。

    输出 720p、faststart（moov 前置），网页可以直接边下边播、拖动进度条。
    .hevc 转换成功后删原文件；HEVC 的 MP4 是原地替换（先写临时文件，验完再替换），
    中途失败不会破坏素材。
    """
    if not os.path.exists(path):
        return False
    lock = _acquire_tc_lock(path)
    if lock is False:
        return False
    try:
        return _transcode_impl(path, kind)
    finally:
        if lock:
            try:
                lock.close()
            except Exception:
                pass


def _transcode_impl(path, kind):
    if path.endswith(".hevc"):
        src_fmt = ["-f", "hevc"]
        out_path = path[:-5] + ".mp4"
        tmp = out_path + ".part"
    elif path.endswith(".mp4"):
        if is_h264(path):
            return True
        src_fmt = []
        out_path = path
        tmp = path[:-4] + ".h264part.tmp"
    else:
        return False

    # 音频伴生文件（G711A 裸流）：存在且足够大才合成音轨
    a_path = path[:-5] + ".alaw" if path.endswith(".hevc") else ""
    has_audio = bool(a_path and os.path.exists(a_path)
                     and os.path.getsize(a_path) > 8000)

    cmd = ["ffmpeg", "-v", "error", "-y"] + src_fmt + ["-i", path]
    if has_audio:
        cmd += ["-f", "alaw", "-ar", "8000", "-ac", "1", "-i", a_path]
    cmd += ["-map", "0:v:0"]
    if has_audio:
        cmd += ["-map", "1:a:0", "-c:a", "aac", "-b:a", "32k"]
    else:
        cmd += ["-an"]
    cmd += ["-c:v", "libopenh264", "-b:v", "1200k", "-g", "60",
            "-vf", "scale=-2:720",
            "-pix_fmt", "yuv420p",
            "-movflags", "+faststart",
            "-f", "mp4", tmp]
    ok = False
    try:
        r = subprocess.run(cmd, capture_output=True, timeout=3600)
        if r.returncode == 0 and os.path.exists(tmp) and os.path.getsize(tmp) > 10 * 1024:
            # 落地前先验一遍，避免把半成品覆盖到原文件上（会直接丢素材）
            vcodec = probe_vcodec(tmp)
            vdur = _mp4_duration(tmp)
            if vcodec == "h264" and vdur > 0.5:
                os.replace(tmp, out_path)
                ok = True
            else:
                log.warning("转码产物无效 %s codec=%s dur=%s", path, vcodec, vdur)
        else:
            log.warning("转码失败 %s rc=%s %s", path, r.returncode,
                        (r.stderr or b"")[-300:].decode("utf-8", "ignore"))
    except Exception as e:
        log.warning("转码异常 %s: %s", path, e)
    finally:
        if not ok and os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass

    if not ok:
        return False

    if out_path != path:
        try:
            os.remove(path)
        except OSError:
            pass
        if a_path:                       # 音轨已合成，伴生文件一并清理
            try:
                os.remove(a_path)
            except OSError:
                pass
    if kind == "recording":
        try:
            from storage import backup_queue, load_cfg
            if load_cfg().get("backupRecordings", True):
                backup_queue.put_nowait((out_path, "recording"))
        except Exception:
            pass
    prune_recordings(out_path)
    return True


def migrate_legacy(root=os.path.join(os.environ.get("UNICOMAI_HOME", "/opt/unicomai"), "data/recordings"), kind="recording"):
    """启动时把历史 HEVC 文件排进转码队列（跳过还在写入的）。"""
    n = 0
    try:
        now = time.time()
        for dirpath, _, files in os.walk(root):
            for f in sorted(files):
                # 上次进程被杀留下的半成品，直接清掉重来
                if f.endswith((".part", ".h264part.tmp")):
                    fp = os.path.join(dirpath, f)
                    try:
                        if now - os.stat(fp).st_mtime > 600:
                            os.remove(fp)
                    except OSError:
                        pass
                    continue
                if not f.endswith((".hevc", ".mp4")):
                    continue
                p = os.path.join(dirpath, f)
                try:
                    st = os.stat(p)
                except OSError:
                    continue
                if now - st.st_mtime < 90:
                    continue
                if f.endswith(".hevc") or probe_vcodec(p) == "hevc":
                    if enqueue_transcode(p, kind):
                        n += 1
    except Exception as e:
        log.warning("历史录像扫描失败: %s", e)
    if n:
        log.info("历史文件待转码 %d 个（%s）", n, root)
    return n


def janitor_loop(interval=300):
    """定时巡检：把漏掉的历史 HEVC（进程被杀、断电留下的段）补进转码队列。"""
    while True:
        time.sleep(interval)
        _HOME_DATA = os.path.join(os.environ.get("UNICOMAI_HOME", "/opt/unicomai"), "data")
        for root, kind in ((os.path.join(_HOME_DATA, "recordings"), "recording"),
                           (os.path.join(_HOME_DATA, "events"), "event")):
            try:
                migrate_legacy(root, kind)
            except Exception:
                pass


def prune_recordings(ref_path: str, keep_days=7, keep_bytes=60 * 1024 ** 3):
    """清理旧的录像文件：超过保留天数，或总量超过上限时从最旧的删起。

    以 ref_path 所在根目录为准，避免误删别处文件。
    """
    try:
        root = os.path.join(os.environ.get("UNICOMAI_HOME", "/opt/unicomai"), "data/recordings")
        now = time.time()
        items = []
        total = 0
        for dirpath, _, files in os.walk(root):
            for f in files:
                if not f.endswith((".hevc", ".mp4")):
                    continue
                p = os.path.join(dirpath, f)
                try:
                    st = os.stat(p)
                except OSError:
                    continue
                # 正在写入的切片不要动（最近 60 秒内修改过）
                if now - st.st_mtime < 60:
                    total += st.st_size
                    continue
                if now - st.st_mtime > keep_days * 86400:
                    try:
                        os.remove(p)
                        continue
                    except OSError:
                        pass
                items.append((st.st_mtime, st.st_size, p))
                total += st.st_size
        if total <= keep_bytes:
            return
        items.sort()
        for _, size, p in items:
            if total <= keep_bytes:
                break
            try:
                os.remove(p)
                total -= size
            except OSError:
                pass
    except Exception:
        pass


def post_process_segment(path: str):
    """兼容旧调用：把切片丢进转码队列。"""
    return enqueue_transcode(path)



_engines = {}


def get_engine(cfg: dict) -> DeviceEngine:
    dev = cfg.get("deviceId", "")
    if dev not in _engines:
        _engines[dev] = DeviceEngine(cfg)
    return _engines[dev]


def all_engines():
    return list(_engines.values())
