"""WebRTC 低延迟直播桥。

链路：引擎 HEVC 订阅队列 -> ffmpeg 解码缩放为 YUV420P 30fps -> aiortc 轨道
（PyAV/libx264 硬路径编码 H.264）-> RTCPeerConnection 发给浏览器。
端到端延迟 ~0.2-0.5 秒（对比 MSE 方案 1~2 秒）。

注意：沙箱只有 HTTP 代理出口、无公网 UDP，WebRTC 只在「浏览器与本服务
同一局域网（含本机）」时可用；公网访问需要 TURN 服务器，当前环境不具备。
"""
import asyncio
import fractions
import logging
import os
import queue
import subprocess
import threading
import time

import av
from aiortc import RTCPeerConnection, RTCSessionDescription
from aiortc.mediastreams import VideoStreamTrack

W, H, FPS = 1280, 720, 30
_FRAME_SIZE = W * H * 3 // 2


def _ffmpeg_cmd():
    return [
        "ffmpeg", "-v", "error",
        "-fflags", "nobuffer", "-flags", "low_delay",
        "-f", "hevc", "-i", "pipe:0",
        "-vf", "scale=%d:%d" % (W, H),
        "-r", str(FPS),
        "-pix_fmt", "yuv420p",
        "-f", "rawvideo", "pipe:1",
    ]


class LiveWebRTCTrack(VideoStreamTrack):
    """把引擎码流转成 aiortc 视频轨道。"""

    def __init__(self, eng):
        super().__init__()
        self.eng = eng
        self.q = eng.subscribe()
        self._closed = False
        self._proc = subprocess.Popen(_ffmpeg_cmd(), stdin=subprocess.PIPE,
                                      stdout=subprocess.PIPE,
                                      stderr=subprocess.DEVNULL, bufsize=0)
        self._frames = queue.Queue(maxsize=6)
        self._reader = threading.Thread(target=self._read_frames, daemon=True)
        self._reader.start()
        self._pumper = threading.Thread(target=self._pump_frames, daemon=True)
        self._pumper.start()

    def _pump_frames(self):
        """引擎订阅队列 -> ffmpeg stdin（独立线程，阻塞写没问题）。"""
        try:
            while not self._closed:
                try:
                    chunk = self.q.get(timeout=5)
                except Exception:
                    continue
                if not chunk:
                    continue
                try:
                    self._proc.stdin.write(chunk)
                    self._proc.stdin.flush()
                except Exception:
                    return
        except Exception:
            pass

    def _read_frames(self):
        try:
            while not self._closed:
                buf = b""
                while len(buf) < _FRAME_SIZE:
                    d = os.read(self._proc.stdout.fileno(), _FRAME_SIZE - len(buf))
                    if not d:
                        return
                    buf += d
                try:
                    self._frames.put_nowait(buf)
                except queue.Full:
                    try:
                        self._frames.get_nowait()   # 丢旧帧保实时
                        self._frames.put_nowait(buf)
                    except Exception:
                        pass
        except Exception:
            pass

    async def recv(self):
        pts, time_base = await self.next_timestamp()
        raw = await asyncio.get_event_loop().run_in_executor(
            None, self._frames.get)
        frame = av.VideoFrame(W, H, format="yuv420p")
        y = W * H
        c = W * H // 4
        frame.planes[0].update(raw[:y])
        frame.planes[1].update(raw[y:y + c])
        frame.planes[2].update(raw[y + c:])
        frame.pts = pts
        frame.time_base = time_base
        return frame

    def stop(self):
        self._closed = True
        try:
            self.q and self.eng.unsubscribe(self.q)
        except Exception:
            pass
        try:
            self._proc.kill()
        except Exception:
            pass
        super().stop()


class WebRTCSession:
    def __init__(self, eng):
        self.eng = eng
        self.pc = RTCPeerConnection()
        self.track = LiveWebRTCTrack(eng)
        self.pc.addTrack(self.track)

    async def handle_offer(self, sdp: str, type_: str):
        await self.pc.setRemoteDescription(
            RTCSessionDescription(sdp=sdp, type=type_))
        answer = await self.pc.createAnswer()
        await self.pc.setLocalDescription(answer)
        return {"sdp": self.pc.localDescription.sdp,
                "type": self.pc.localDescription.type}

    async def close(self):
        try:
            await self.pc.close()
        except Exception:
            pass
