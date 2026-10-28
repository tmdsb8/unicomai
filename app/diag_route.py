"""临时诊断路由：仅在开启 UM_DIAG=1 时注册。"""
import asyncio, functools, os, subprocess, time


def register(app, main):
    @app.get("/api/_diag")
    async def _diag():
        eng = main.get_engine(dict(main.DEVICES[0], recordDir=os.path.join(main.DATA_DIR, "recordings")))
        q = eng.subscribe()
        proc = None; aligned = False; total = 0; fed = 0; chunks = 0; log = []
        t0 = time.time()
        try:
            while time.time() - t0 < 12:
                chunk = await asyncio.get_event_loop().run_in_executor(
                    None, functools.partial(main._q_get, q, 5.0))
                if chunk is None:
                    log.append("timeout"); continue
                chunks += 1
                if not aligned:
                    seg = eng.align_to_idr(chunk)
                    log.append("len=%d aligned=%d vps=%d sps=%d pps=%d" % (len(chunk), len(seg), len(eng.vps), len(eng.sps), len(eng.pps)))
                    if not seg:
                        continue
                    chunk = seg; aligned = True
                if proc is None:
                    proc = subprocess.Popen(main._ffmpeg_live_cmd(), stdin=subprocess.PIPE,
                                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0)
                try:
                    proc.stdin.write(chunk); proc.stdin.flush(); fed += len(chunk)
                except Exception as e:
                    log.append("write fail %s" % e); break
                buf = await asyncio.get_event_loop().run_in_executor(
                    None, functools.partial(main._read_avail, proc.stdout))
                total += len(buf)
                if proc.poll() is not None:
                    log.append("ffmpeg exit %s" % proc.returncode); break
        finally:
            eng.unsubscribe(q)
            err = b""
            if proc:
                try:
                    proc.kill(); err = proc.stderr.read()
                except Exception:
                    pass
        return {"chunks": chunks, "fed": fed, "out": total, "log": log,
                "stderr": err.decode("utf-8", "ignore")[:800]}
