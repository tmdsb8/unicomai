"""事件引擎：定期抽帧做 AI 判定，命中后从环形缓冲截取视频 + 截图。

流程
----
1. 按 interval 秒抽一帧（从直播订阅队列取最新数据，用 ffmpeg 解码成图）
2. YOLO 判定人 / 移动物
3. 命中且不在冷却期 -> 记录事件时间点
4. 等待 post_seconds 秒（让环形缓冲补齐"事件后"画面）
5. 从环形缓冲截取 [t-hit-pre, t+post] 的码流 -> ffmpeg 转 mp4
6. 抽取 3 张截图（事件前、事件中、事件后）
7. 打包上传
"""
import json
import logging
import os
import subprocess
import threading
import time

import cv2
import numpy as np

from detector import Detector

log = logging.getLogger("events")

DATA_DIR = os.path.join(os.environ.get("UNICOMAI_HOME", "/opt/unicomai"), "data")
EVENT_DIR = os.path.join(DATA_DIR, "events")
INDEX_PATH = os.path.join(DATA_DIR, "events.json")


class EventEngine:
    def __init__(self, engine, cfg: dict):
        self.engine = engine
        self.cfg = {
            "enabled": True,
            "interval": 1.0,          # 抽帧间隔（秒）——1 秒足够快，又不至于把 CPU 拉满
            "personConf": 0.30,
            "objConf": 0.45,
            "motionTh": 0.004,
            "cooldown": 60,           # 冷却期（秒），避免同一动作反复触发
            "preSeconds": 8,          # 事件视频含事件前多少秒
            "postSeconds": 12,        # 事件视频含事件后多少秒（合计 20s）
            "shotCount": 3,
        }
        self.cfg.update(cfg or {})

        self.detector = Detector()
        self._stop = threading.Event()
        self._thread = None
        self.last_hit_at = 0.0
        self.last_reason = ""
        self.stats = {"frames": 0, "hits": 0, "saved": 0, "errors": 0}
        self._processing = False
        self.last_judge = {}
        self._motion_streak = 0
        os.makedirs(EVENT_DIR, exist_ok=True)
        self._load_index()

    # ---------- 索引 ----------
    def _load_index(self):
        self.events = []
        if os.path.exists(INDEX_PATH):
            try:
                self.events = json.load(open(INDEX_PATH))
            except Exception:
                self.events = []

    def _save_index(self):
        with open(INDEX_PATH, "w") as f:
            json.dump(self.events[-500:], f, ensure_ascii=False, indent=1)

    # ---------- 生命周期 ----------
    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="events-" + self.engine.device_id, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()

    # ---------- 主循环 ----------
    def _run(self):
        if not self.detector.available:
            log.error("AI 模型不可用，事件检测未启动")
            return
        while not self._stop.is_set():
            t0 = time.time()
            try:
                if self.cfg.get("enabled", True):
                    self._tick()
            except Exception as e:
                self.stats["errors"] += 1
                log.warning("检测异常: %s", e)
            elapsed = time.time() - t0
            self._stop.wait(max(0.2, self.cfg["interval"] - elapsed))

    def _grab_frame(self):
        """从环形缓冲取最近 1.5 秒码流，解码出最后一帧。

        早期版本走订阅队列，队列慢时一次要等好几秒；改成直接读环形缓冲后，
        每次抽帧稳定在 0.2 秒左右，2 秒一帧的节奏才真正跑得起来。
        """
        import time as _t
        try:
            ring = list(self.engine.ring)
        except Exception:
            return None
        if not ring:
            return None
        now = ring[-1][0]
        buf = []
        total = 0
        for ts, data in reversed(ring):
            buf.append(data)
            total += len(data)
            if now - ts > 1.5 or total > 700 * 1024:
                break
        if not buf:
            return None
        data = b"".join(reversed(buf))
        # 从最近的参数集/IDR 起解码，避免花屏或直接失败
        seg = self.engine.extract_aligned_gop(data)
        if seg:
            data = seg
        try:
            proc = subprocess.run(
                ["ffmpeg", "-v", "error", "-f", "hevc", "-i", "pipe:0",
                 "-frames:v", "1", "-f", "image2pipe", "-vcodec", "png", "pipe:1"],
                input=data, capture_output=True, timeout=15,
            )
            if proc.returncode != 0 or not proc.stdout:
                return None
            arr = np.frombuffer(proc.stdout, np.uint8)
            return cv2.imdecode(arr, cv2.IMREAD_COLOR)
        except Exception:
            return None

    def _tick(self):
        if not self.engine.connected:
            return
        img = self._grab_frame()
        if img is None:
            return
        self.stats["frames"] += 1

        hit, reason, dets, motion = self.detector.judge(
            img,
            person_conf=self.cfg["personConf"],
            obj_conf=self.cfg["objConf"],
            motion_th=self.cfg["motionTh"],
        )
        # 画面持续变化也算事件：人在远处走过时 YOLO 常常框不出来，
        # 但帧差会一直有反应，连续两帧超过阈值就记一次。
        if (not hit) and motion and motion >= self.cfg["motionTh"]:
            self._motion_streak += 1
        else:
            self._motion_streak = 0
        if (not hit) and self._motion_streak >= 2 and motion >= self.cfg["motionTh"] * 1.5:
            hit, reason = True, "motion:%.3f" % motion

        now = time.time()
        self.last_judge = {
            "at": time.strftime("%H:%M:%S"),
            "ts": now,
            "hit": bool(hit),
            "reason": reason,
            "motion": round(float(motion or 0), 4),
            "objects": [{"name": x[6], "conf": round(float(x[4]), 3)} for x in dets][:8],
            "count": len(dets),
            "streak": self._motion_streak,
            "shape": list(img.shape[:2]),
        }
        if hit and now - self.last_hit_at >= self.cfg["cooldown"]:
            if self._processing:
                return
            self.last_hit_at = now
            self.last_reason = reason
            self.stats["hits"] += 1
            self._processing = True
            threading.Thread(target=self._make_event, args=(now, reason, img, dets),
                             daemon=True, name="evt").start()

    # ---------- 事件生成 ----------
    def _make_event(self, hit_ts, reason, frame, dets):
        try:
            pre, post = self.cfg["preSeconds"], self.cfg["postSeconds"]
            target_span = pre + post
            # 等缓冲补齐「事件后」画面；若缓冲尚未积累足够时长，继续等
            time.sleep(post + 1)
            wait_until = time.time() + 15
            while time.time() < wait_until:
                covered = self._buf_span()
                if covered >= target_span:
                    break
                time.sleep(1)

            ts_label = time.strftime("%Y%m%d_%H%M%S", time.localtime(hit_ts))
            evt_id = "%s_%s" % (self.engine.device_id, ts_label)
            d = os.path.join(EVENT_DIR, evt_id)
            os.makedirs(d, exist_ok=True)

            # 1) 事件视频：直接转成 H.264 MP4，浏览器与云盘都能直接播
            raw = self.engine.snapshot_ring(hit_ts - pre, hit_ts + post)
            video_path = os.path.join(d, "clip.mp4")
            duration = 0.0
            if len(raw) > 200 * 1024:
                tmp = os.path.join(d, "_raw.hevc")
                with open(tmp, "wb") as f:
                    f.write(raw)
                subprocess.run(
                    ["ffmpeg", "-v", "error", "-y", "-f", "hevc", "-i", tmp,
                     "-t", "%.1f" % target_span,
                     "-map", "0:v:0", "-an",
                     "-c:v", "libopenh264", "-b:v", "1500k", "-g", "60",
                     "-vf", "scale=-2:720", "-pix_fmt", "yuv420p",
                     "-movflags", "+faststart", video_path],
                    capture_output=True, timeout=300,
                )
                try:
                    os.remove(tmp)
                except Exception:
                    pass
                if os.path.exists(video_path):
                    duration = _probe_duration(video_path)

            # 只记录「人」和达到阈值的关注类，避免把窗帘花纹认成的马桶也写进告警
            from detector import WATCH_CLASSES, STATIC_IGNORE
            notable = [x for x in dets
                       if x[5] == 0 or (x[5] in WATCH_CLASSES and x[5] not in STATIC_IGNORE
                                        and x[4] >= self.cfg["objConf"])]

            # 2) 三张截图：命中帧 + 从视频前/后段各抽一帧
            shots = []
            shot1 = os.path.join(d, "shot_1.jpg")
            cv2.imwrite(shot1, _draw_boxes(frame, notable), [cv2.IMWRITE_JPEG_QUALITY, 88])
            shots.append(shot1)
            if os.path.exists(video_path) and duration > 2:
                for idx, ratio in ((2, 0.35), (3, 0.8)):
                    sp = os.path.join(d, "shot_%d.jpg" % idx)
                    subprocess.run(
                        ["ffmpeg", "-v", "error", "-y", "-ss", "%.2f" % (duration * ratio),
                         "-i", video_path, "-frames:v", "1", "-q:v", "3", sp],
                        capture_output=True, timeout=60,
                    )
                    if os.path.exists(sp):
                        shots.append(sp)
            while len(shots) < 3:   # 兜底：复制命中帧补足 3 张
                sp = os.path.join(d, "shot_%d.jpg" % (len(shots) + 1))
                cv2.imwrite(sp, _draw_boxes(frame, notable), [cv2.IMWRITE_JPEG_QUALITY, 88])
                shots.append(sp)

            meta = {
                "id": evt_id,
                "device": self.engine.name,
                "deviceId": self.engine.device_id,
                "time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(hit_ts)),
                "ts": hit_ts,
                "reason": reason,
                "targets": [{"name": x[6], "conf": round(x[4], 3)} for x in notable][:8],
                "video": os.path.relpath(video_path, DATA_DIR) if os.path.exists(video_path) else "",
                "duration": round(duration, 1),
                "shots": [os.path.relpath(s, DATA_DIR) for s in shots],
                "uploaded": False,
            }
            json.dump(meta, open(os.path.join(d, "meta.json"), "w"), ensure_ascii=False, indent=2)

            self.events.append(meta)
            self._save_index()
            self.stats["saved"] += 1
            log.info("事件已生成 %s (%s, %.1fs)", evt_id, reason, duration)

            # 3) 上传到云盘
            try:
                from storage import backup_queue, load_cfg
                if load_cfg().get("uploadEvents", True):
                    if meta["video"]:
                        backup_queue.put_nowait((os.path.join(DATA_DIR, meta["video"]), "event_video"))
                    for s in shots:
                        backup_queue.put_nowait((os.path.join(DATA_DIR, s), "event_shot"))
                    meta["uploaded"] = True
            except Exception as e:
                log.warning("事件入队上传失败: %s", e)
        except Exception as e:
            self.stats["errors"] += 1
            log.warning("生成事件失败: %s", e)
        finally:
            self._processing = False

    # ---------- 查询 ----------
    def _buf_span(self):
        """当前环形缓冲覆盖的时间跨度（秒）。"""
        ring = self.engine.ring
        if len(ring) < 2:
            return 0.0
        return ring[-1][0] - ring[0][0]

    def list_events(self, limit=100):
        return sorted(self.events, key=lambda x: x.get("ts", 0), reverse=True)[:limit]

    def status(self):
        return {
            "enabled": self.cfg.get("enabled", True),
            "modelReady": self.detector.available,
            "stats": self.stats,
            "cooldown": self.cfg["cooldown"],
            "lastHit": time.strftime("%H:%M:%S", time.localtime(self.last_hit_at)) if self.last_hit_at else None,
            "lastReason": self.last_reason,
            "config": self.cfg,
            "lastJudge": self.last_judge,
        }


def _probe_duration(path):
    try:
        r = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration",
                            "-of", "default=nw=1:nk=1", path], capture_output=True, timeout=30)
        return float(r.stdout.decode().strip() or 0)
    except Exception:
        return 0.0


def _draw_boxes(img, dets):
    out = img.copy()
    for x1, y1, x2, y2, conf, cid, name in dets:
        color = (0, 0, 255) if cid == 0 else (0, 165, 255)
        cv2.rectangle(out, (int(x1), int(y1)), (int(x2), int(y2)), color, 3)
        label = "%s %.2f" % (name, conf)
        cv2.putText(out, label, (int(x1), max(18, int(y1) - 8)),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.7, color, 2)
    return out


_event_engines = {}


def get_event_engine(engine, cfg=None):
    dev = engine.device_id
    if dev not in _event_engines:
        _event_engines[dev] = EventEngine(engine, cfg)
    return _event_engines[dev]


def get_event_engines_map():
    """暴露内部缓存（换设备配置时清理旧引擎用）。"""
    return _event_engines
