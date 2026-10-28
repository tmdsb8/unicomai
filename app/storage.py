"""可插拔的云存储后端。

支持的驱动
----------
- local    : 仅存本地（默认，用于先跑通全流程）
- webdav   : 坚果云 / Nextcloud / 群晖 / 通用 WebDAV
- s3       : 兼容 S3 协议（AWS S3 / 阿里云 OSS / 腾讯云 COS / MinIO）
- baidu    : 百度网盘开放平台（需 AppKey/SecretKey，走 OAuth 刷新令牌）

设计：所有驱动实现 put(local_path, remote_path) -> bool，失败抛异常由上层重试。
"""
import base64
import hashlib
import hmac
import json
import os
import queue
import random
import socket
import threading
import time
import urllib.parse
from datetime import datetime, timezone

import requests

CFG_PATH = os.environ.get("UM_STORAGE_CFG", os.path.join(os.environ.get("UNICOMAI_HOME", "/opt/unicomai"), "data/storage.json"))

# ---- 网络健壮性 -----------------------------------------------------------
# yun.139.com 等域名的 DNS 会返回 AAAA 记录，而部署环境往往没有 IPv6 出口，
# requests 偶发先试 IPv6 直接抛 Errno 101 (Network is unreachable)。
# 全局把解析结果收敛到 IPv4（无 v4 时保持原样）。
_orig_getaddrinfo = socket.getaddrinfo

def _ipv4_first_getaddrinfo(*args, **kwargs):
    try:
        res = _orig_getaddrinfo(*args, **kwargs)
    except Exception:
        raise
    v4 = [r for r in res if r[0] == socket.AF_INET]
    return v4 if v4 else res

socket.getaddrinfo = _ipv4_first_getaddrinfo

def _request_with_retry(fn, tries=3, base_delay=1.0, **kw):
    """连接类错误（DNS/路由抖动、沙箱唤醒后网络未就绪）做指数退避重试。"""
    last = None
    for i in range(tries):
        try:
            return fn(**kw)
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            last = e
            if i < tries - 1:
                time.sleep(base_delay * (2 ** i))
    raise last


class StorageError(Exception):
    pass


# ---------------- 驱动实现 ----------------
class LocalDriver:
    name = "local"

    def __init__(self, cfg):
        self.root = cfg.get("root", os.path.join(os.environ.get("UNICOMAI_HOME", "/opt/unicomai"), "data/cloud_sim"))
        os.makedirs(self.root, exist_ok=True)

    def put(self, local_path, remote_path):
        dst = os.path.join(self.root, remote_path.lstrip("/"))
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        with open(local_path, "rb") as src, open(dst, "wb") as out:
            while True:
                chunk = src.read(1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)
        return True

    def test(self):
        p = os.path.join(self.root, ".probe")
        open(p, "w").write("ok")
        os.remove(p)
        return True, "本地模拟目录可写: " + self.root


class WebDAVDriver:
    name = "webdav"

    def __init__(self, cfg):
        self.base = cfg["url"].rstrip("/")
        self.user = cfg.get("username", "")
        self.pwd = cfg.get("password", "")
        self.session = requests.Session()
        self.session.auth = (self.user, self.pwd)
        self.session.headers["User-Agent"] = "UnicomAI/1.0"

    def _ensure_dirs(self, remote_path):
        parts = remote_path.strip("/").split("/")[:-1]
        cur = self.base
        for p in parts:
            cur = cur + "/" + urllib.parse.quote(p)
            try:
                self.session.request("MKCOL", cur, timeout=15)
            except Exception:
                pass

    def put(self, local_path, remote_path):
        self._ensure_dirs(remote_path)
        url = self.base + "/" + urllib.parse.quote(remote_path.lstrip("/"))
        with open(local_path, "rb") as f:
            r = self.session.put(url, data=f, timeout=180)
        if r.status_code not in (200, 201, 204):
            raise StorageError("WebDAV PUT %s: %s %s" % (r.status_code, url, r.text[:200]))
        return True

    def test(self):
        try:
            r = self.session.request("PROPFIND", self.base, timeout=15,
                                     headers={"Depth": "0"})
            if r.status_code in (200, 207):
                return True, "WebDAV 连接正常 (%s)" % r.status_code
            return False, "WebDAV 返回 %s" % r.status_code
        except Exception as e:
            return False, "WebDAV 连接失败: %s" % e


class S3Driver:
    """S3 兼容（AWS / OSS / COS / MinIO）。使用 SigV4 手写签名，避免额外 SDK 依赖。"""
    name = "s3"

    def __init__(self, cfg):
        self.endpoint = cfg["endpoint"].rstrip("/")   # 如 https://oss-cn-hangzhou.aliyuncs.com
        self.region = cfg.get("region", "us-east-1")
        self.bucket = cfg["bucket"]
        self.ak = cfg["accessKey"]
        self.sk = cfg["secretKey"]
        self.prefix = cfg.get("prefix", "")
        self.path_style = cfg.get("pathStyle", True)

    def _sign(self, method, host, path, query="", payload=b"", content_type=""):
        t = datetime.now(timezone.utc)
        amzdate = t.strftime("%Y%m%dT%H%M%SZ")
        date = t.strftime("%Y%m%d")
        payload_hash = hashlib.sha256(payload).hexdigest()
        headers = {"host": host, "x-amz-content-sha256": payload_hash, "x-amz-date": amzdate}
        if content_type:
            headers["content-type"] = content_type
        signed_headers = ";".join(sorted(headers))
        canonical_headers = "".join("%s:%s\n" % (k, headers[k]) for k in sorted(headers))
        canonical_request = "\n".join([method, path, query, canonical_headers, signed_headers, payload_hash])
        scope = "%s/%s/s3/aws4_request" % (date, self.region)
        sts = "\n".join(["AWS4-HMAC-SHA256", amzdate, scope,
                         hashlib.sha256(canonical_request.encode()).hexdigest()])
        k = ("AWS4" + self.sk).encode()
        for part in (date, self.region, "s3", "aws4_request"):
            k = hmac.new(k, part.encode(), hashlib.sha256).digest()
        sig = hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()
        headers["Authorization"] = ("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s"
                                    % (self.ak, scope, signed_headers, sig))
        if content_type:
            headers["Content-Type"] = content_type
        return headers

    def put(self, local_path, remote_path):
        key = (self.prefix.rstrip("/") + "/" + remote_path.lstrip("/")).lstrip("/")
        with open(local_path, "rb") as f:
            body = f.read()
        parsed = urllib.parse.urlparse(self.endpoint)
        if self.path_style:
            host = parsed.netloc
            path = "/%s/%s" % (self.bucket, urllib.parse.quote(key))
        else:
            host = "%s.%s" % (self.bucket, parsed.netloc)
            path = "/" + urllib.parse.quote(key)
        ctype = "application/octet-stream"
        headers = self._sign("PUT", host, path, "", body, ctype)
        url = "%s://%s%s" % (parsed.scheme, host, path)
        r = requests.put(url, data=body, headers=headers, timeout=300)
        if r.status_code not in (200, 201):
            raise StorageError("S3 PUT %s: %s" % (r.status_code, r.text[:200]))
        return True

    def test(self):
        try:
            parsed = urllib.parse.urlparse(self.endpoint)
            host = parsed.netloc if self.path_style else "%s.%s" % (self.bucket, parsed.netloc)
            path = "/%s/" % self.bucket if self.path_style else "/"
            headers = self._sign("GET", host, path, "list-type=2&max-keys=1")
            url = "%s://%s%s?list-type=2&max-keys=1" % (parsed.scheme, host, path)
            r = requests.get(url, headers=headers, timeout=20)
            if r.status_code == 200:
                return True, "S3 连接正常，桶 %s 可读" % self.bucket
            return False, "S3 返回 %s: %s" % (r.status_code, r.text[:200])
        except Exception as e:
            return False, "S3 连接失败: %s" % e


class BaiduPanDriver:
    """百度网盘开放平台：precreate + superfile2 分片上传（简化为单分片）。"""
    name = "baidu"
    API = "https://pan.baidu.com/rest/2.0/xpan/file"

    def __init__(self, cfg):
        self.app_key = cfg["appKey"]
        self.secret_key = cfg["secretKey"]
        self.refresh_token = cfg["refreshToken"]
        self.remote_dir = cfg.get("dir", "/apps/unicomAI")
        self._token = None
        self._token_exp = 0

    def _access_token(self):
        if self._token and time.time() < self._token_exp:
            return self._token
        r = requests.get("https://openapi.baidu.com/oauth/2.0/token", timeout=20, params={
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.app_key,
            "client_secret": self.secret_key,
        })
        data = r.json()
        if "access_token" not in data:
            raise StorageError("百度网盘取 token 失败: %s" % data)
        self._token = data["access_token"]
        self._token_exp = time.time() + int(data.get("expires_in", 3600)) - 120
        return self._token

    def put(self, local_path, remote_path):
        token = self._access_token()
        size = os.path.getsize(local_path)
        block = hashlib.md5(open(local_path, "rb").read()).hexdigest()
        remote = "%s/%s" % (self.remote_dir.rstrip("/"), remote_path.lstrip("/"))
        r = requests.post(self.API, params={"method": "precreate", "access_token": token}, timeout=30,
                          data={"path": remote, "size": size, "isdir": 0, "autoinit": 1,
                                "block_list": json.dumps([block])})
        info = r.json()
        if info.get("errno") not in (0, None):
            raise StorageError("百度网盘 precreate 失败: %s" % info)
        uploadid = info.get("uploadid")
        if not uploadid:
            return True  # 已存在
        url = "https://d.pcs.baidu.com/rest/2.0/pcs/superfile2"
        with open(local_path, "rb") as f:
            r2 = requests.post(url, params={
                "method": "upload", "access_token": token, "type": "tmpfile",
                "path": remote, "uploadid": uploadid, "partseq": 0,
            }, files={"file": f}, timeout=600)
        if r2.json().get("md5") is None:
            raise StorageError("百度网盘分片上传失败: %s" % r2.text[:200])
        r3 = requests.post(self.API, params={"method": "create", "access_token": token}, timeout=30,
                           data={"path": remote, "size": size, "isdir": 0, "uploadid": uploadid,
                                 "block_list": json.dumps([block])})
        if r3.json().get("errno") != 0:
            raise StorageError("百度网盘 create 失败: %s" % r3.text[:200])
        return True

    def test(self):
        try:
            self._access_token()
            return True, "百度网盘 token 有效"
        except Exception as e:
            return False, str(e)


# ---------------- 国内运营商云盘 ----------------
def _md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def _sha256_file(path, size=None):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(1024 * 1024)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def _encode_uri_component(s: str) -> str:
    """对齐 JS encodeURIComponent / Go url.QueryEscape 的差异。"""
    r = urllib.parse.quote(s, safe="")
    for a, b in (("%21", "!"), ("%27", "'"), ("%28", "("),
                 ("%29", ")"), ("%2A", "*")):
        r = r.replace(a, b)
    return r


class CmccDriver:
    """中国移动云盘（139 个人云，新版 /personal_new）。

    鉴权：浏览器登录 https://yun.139.com/ 后，开发者工具里任意一条
    hcy/... 请求的 Authorization 头，把 "Basic " 后面的那串填进来即可。
    上传：/file/create 拿分片地址 -> PUT 各分片 -> /file/complete 合并。
    """

    name = "cmcc"
    BASE = "https://yun.139.com"
    ROUTE = "https://user-njs.yun.139.com/user/route/qryRoutePolicy"
    PART = 100 * 1024 * 1024

    def __init__(self, cfg):
        tok = (cfg.get("authorization") or cfg.get("token") or "").strip()
        if tok.lower().startswith("basic "):
            tok = tok[6:].strip()
        self.token = tok
        # Basic token 形如 base64("pc:<手机号>:<凭证>|1|RCS|<ts>|<sig>")，
        # 路由查询等接口要求 accountName 带上这个手机号，否则报 05010001
        self.user = ""
        try:
            raw = base64.b64decode(tok + "=" * (-len(tok) % 4)).decode("utf-8", "ignore")
            m = __import__("re").match(r"^[a-z]+:([^:|]+):", raw)
            if m:
                self.user = m.group(1)
        except Exception:
            pass
        self.parent = str(cfg.get("dir") or "root").strip() or "root"
        if self.parent in ("/", "", "0", "-1"):
            self.parent = "root"   # 139 个人云的根目录 ID 就是字面量 root
        self._host = ""
        self._pid = ""

    # --- 签名 ---
    def _sign(self, body: str, ts: str, rand: str) -> str:
        enc = _encode_uri_component(body)
        b64 = base64.b64encode("".join(sorted(enc)).encode("utf-8")).decode()
        res = _md5_hex(b64) + _md5_hex(ts + ":" + rand)
        return _md5_hex(res).upper()

    def _headers(self, body: str, ts: str, rand: str, route=False):
        h = {
            "Accept": "application/json, text/plain, */*",
            "Authorization": "Basic " + self.token,
            "mcloud-channel": "1000101",
            "mcloud-client": "10701",
            "mcloud-sign": "%s,%s,%s" % (ts, rand, self._sign(body, ts, rand)),
            "mcloud-version": "7.14.0",
            "CMS-DEVICE": "default",
            "Origin": "https://yun.139.com",
            "Referer": "https://yun.139.com/w/",
            "x-DeviceInfo": "||9|7.14.0|chrome|120.0.0.0|||windows 10||zh-CN|||",
            "x-huawei-channelSrc": "10000034",
            "x-inner-ntwk": "2",
            "x-m4c-caller": "PC",
            "x-m4c-src": "10002",
            "x-SvcType": "1",
            "Content-Type": "application/json",
        }
        if route:
            h["Inner-Hcy-Router-Https"] = "1"
        else:
            h.update({
                "Caller": "web",
                "Mcloud-Route": "001",
                "X-Yun-Api-Version": "v1",
                "X-Yun-App-Channel": "10000034",
                "X-Yun-Channel-Source": "10000034",
                "X-Yun-Client-Info": "||13|7.14.0|chrome|120.0.0.0|||windows 10||zh-CN|||dW5kZWZpbmVk||",
                "X-Yun-Module-Type": "100",
                "X-Yun-Svc-Type": "1",
            })
        return h

    def _post_json(self, url, obj, route=False, timeout=60):
        body = json.dumps(obj, separators=(",", ":"), sort_keys=True,
                          ensure_ascii=False)
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        rand = "".join(random.choice("abcdefghijklmnopqrstuvwxyz0123456789")
                       for _ in range(16))
        r = _request_with_retry(
            requests.post, url=url, data=body.encode("utf-8"),
            headers=self._headers(body, ts, rand, route), timeout=timeout)
        try:
            data = r.json()
        except Exception:
            raise StorageError("移动云盘返回非 JSON: %s %s" % (r.status_code, r.text[:200]))
        if not data.get("success", False):
            raise StorageError("移动云盘: %s (%s)" % (data.get("message"), data.get("code")))
        return data

    def _personal_host(self):
        if self._host:
            return self._host
        if not self.token:
            raise StorageError("未填写移动云盘 Authorization")
        data = self._post_json(self.ROUTE, {
            "userInfo": {"userType": 1, "accountType": 1, "accountName": self.user},
            "modAddrType": 1,
        }, route=True)
        for item in (data.get("data", {}) or {}).get("routePolicyList", []) or []:
            if item.get("modName") == "personal" and item.get("httpsUrl"):
                self._host = item["httpsUrl"].rstrip("/")
                break
        if not self._host:
            raise StorageError("移动云盘未返回个人云接入点")
        return self._host

    def _resolve_parent(self):
        """目录填的是名字（如 /联通摄像头录像/）时，解析成它的 fileId。

        139 个人云的接口只认 ID（root 是根）。按名字在根目录里找；
        找不到就自动创建同名文件夹，不用用户去云盘里手动查 ID。
        """
        if self._pid:
            return self._pid
        pid = self.parent
        if pid != "root" and not pid.startswith(("F", "D")):
            host = self._personal_host()
            want = pid.strip("/")
            found = None
            cursor = ""
            for _ in range(10):   # 最多翻 10 页，一般根目录就一页
                body = {"parentFileId": "root", "pageSize": 100,
                        "orderBy": "updated_at", "orderDirection": "DESC",
                        "categories": ""}
                if cursor:
                    body["nextPageCursor"] = cursor
                r = self._post_json(host + "/file/list", body, timeout=30)
                data = r.get("data", {}) or {}
                for it in data.get("items") or []:
                    if it.get("name") == want:
                        found = it.get("fileId")
                        break
                if found or not data.get("nextPageCursor"):
                    break
                cursor = data["nextPageCursor"]
            if not found:
                r = self._post_json(host + "/file/create", {
                    "name": want, "type": "folder", "parentFileId": "root",
                    "fileRenameMode": "reject",
                }, timeout=30)
                found = (r.get("data", {}) or {}).get("fileId")
            if not found:
                raise StorageError("移动云盘: 无法定位目录 %r" % want)
            pid = found
        self._pid = pid
        return pid

    def put(self, local_path, remote_path):
        host = self._personal_host()
        parent = self._resolve_parent()
        size = os.path.getsize(local_path)
        # 云盘里没有多级目录概念时，用下划线把日期目录压进文件名
        name = remote_path.strip("/").replace("/", "_")
        digest = _sha256_file(local_path)
        part = self.PART
        count = max(1, (size + part - 1) // part)
        parts = [{"partNumber": i + 1,
                  "partSize": min(part, size - i * part),
                  "parallelHashCtx": {"partOffset": i * part}} for i in range(count)]

        data = self._post_json(host + "/file/create", {
            "contentHash": digest,
            "contentHashAlgorithm": "SHA256",
            "contentType": "application/octet-stream",
            "parallelUpload": False,
            "partInfos": parts[:100],
            "size": size,
            "parentFileId": parent,
            "name": name,
            "type": "file",
            "fileRenameMode": "auto_rename",
        })
        info = data.get("data", {}) or {}
        if info.get("exist"):
            return True
        upload_id = info.get("uploadId")
        file_id = info.get("fileId")
        urls = list(info.get("partInfos") or [])
        # 分片超过 100 个要分批再取地址
        for i in range(100, len(parts), 100):
            more = self._post_json(host + "/file/getUploadUrl", {
                "fileId": file_id,
                "uploadId": upload_id,
                "partInfos": parts[i:i + 100],
                "commonAccountInfo": {"account": self.user, "accountType": 1},
            })
            urls.extend((more.get("data", {}) or {}).get("partInfos") or [])

        with open(local_path, "rb") as f:
            for u in urls:
                idx = int(u.get("partNumber", 1)) - 1
                length = parts[idx]["partSize"] if idx < len(parts) else part
                f.seek(idx * part)
                chunk = f.read(length)
                r = _request_with_retry(
                    requests.put, url=u["uploadUrl"], data=chunk, timeout=600, headers={
                    "Content-Type": "application/octet-stream",
                    "Content-Length": str(len(chunk)),
                    "Origin": "https://yun.139.com",
                    "Referer": "https://yun.139.com/",
                })
                if r.status_code != 200:
                    raise StorageError("移动云盘分片上传失败: %s" % r.status_code)

        self._post_json(host + "/file/complete", {
            "contentHash": digest,
            "contentHashAlgorithm": "SHA256",
            "fileId": file_id,
            "uploadId": upload_id,
        })
        return True

    def test(self):
        try:
            host = self._personal_host()
            pid = self._resolve_parent()
            self._post_json(host + "/file/list", {
                "parentFileId": pid, "pageSize": 1,
                "orderBy": "updated_at", "orderDirection": "DESC",
                "categories": "",
            }, timeout=30)
            return True, "移动云盘连接正常（个人云 %s · 目录 %s -> %s…）" % (
                host.replace("https://", ""), self.parent, pid[:12])
        except Exception as e:
            return False, str(e)


class CtyunDriver:
    """天翼云盘（189）。

    鉴权：浏览器登录 https://cloud.189.cn/ 后，把任意请求的 Cookie 整条复制进来。
    上传走网页端 initMultiUpload / getMultiUploadUrls / commitMultiUploadFile 三段式分片。
    """

    name = "ctyun"

    def __init__(self, cfg):
        self.cookie = (cfg.get("cookie") or "").strip()
        self.dir = str(cfg.get("dir") or "-11")     # -11 是天翼云盘根目录
        self._cli = None

    def _client(self):
        if self._cli is not None:
            return self._cli
        if not self.cookie:
            raise StorageError("未填写天翼云盘 Cookie")
        try:
            from cloud189 import Cloud189
            from cloud189.store import MemoryStore
        except ImportError:
            raise StorageError("缺少依赖 cloud189，请执行 pip install cloud189")
        # token 用内存存储，SDK 会拿 Cookie 里的 sessionKey 去换 accessToken
        self._cli = Cloud189({"ssonCookie": self.cookie, "token": MemoryStore()})
        return self._cli

    def put(self, local_path, remote_path):
        cli = self._client()
        name = remote_path.strip("/").replace("/", "_") or "upload.bin"
        cli.upload(local_path, self.dir, rename=name)
        return True

    def test(self):
        try:
            cli = self._client()
            info = cli.get_disk_space_info()
            data = info.get("data", info) if isinstance(info, dict) else {}
            used = data.get("cloudUsedSize")
            total = data.get("cloudTotalSize")
            if used and total:
                return True, "天翼云盘连接正常，已用 %s / %s" % (
                    _human(int(used)), _human(int(total)))
            return True, "天翼云盘连接正常"
        except Exception as e:
            return False, "天翼云盘失败: %s" % e


class CuccDriver:
    """联通云盘（沃盘 / pan.wo.cn）。

    鉴权：登录 https://pan.wo.cn/ 后，F12 里找带 dispatcher 的请求，
    响应里的 token（UUID 形式）填进来即可。
    上传：8MB 分片 + 文件信息 AES-128-CBC 加密，POST 到 upload2C 节点。
    """

    name = "cucc"
    UPLOAD_URL = "https://tjupload.pan.wo.cn/openapi/client/upload2C"
    CHUNK = 8 * 1024 * 1024
    IV = b"wNSOYIB1k1DjY5lA"

    def __init__(self, cfg):
        self.token = (cfg.get("accessToken") or "").strip()
        self.dir = str(cfg.get("dir") or "0")

    def _encrypt_info(self, info: dict) -> str:
        from Crypto.Cipher import AES
        key = self.token[:16].encode("utf-8")
        if len(key) < 16:
            raise StorageError("联通云盘 Token 长度不足 16 位")
        raw = json.dumps(info, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        pad = 16 - len(raw) % 16
        raw += bytes([pad]) * pad
        return base64.b64encode(AES.new(key, AES.MODE_CBC, self.IV).encrypt(raw)).decode()

    @staticmethod
    def _file_type(name):
        ext = os.path.splitext(name)[1].lower().lstrip(".")
        if ext in ("jpg", "jpeg", "png", "bmp", "gif"):
            return "1"
        if ext in ("mp4", "mkv", "avi", "mov", "flv"):
            return "2"
        if ext in ("mp3", "wav", "flac"):
            return "3"
        if ext in ("doc", "docx", "pdf", "txt"):
            return "4"
        return "5"

    def put(self, local_path, remote_path):
        if not self.token:
            raise StorageError("未填写联通云盘 accessToken")
        name = remote_path.strip("/").replace("/", "_") or "upload.bin"
        size = os.path.getsize(local_path)
        total = max(1, (size + self.CHUNK - 1) // self.CHUNK)
        batch_no = time.strftime("%Y%m%d%H%M%S")
        unique_id = "%d_%s" % (int(time.time() * 1000),
                               "".join(random.choice("abcdefghijklmnopqrstuvwxyz")
                                       for _ in range(6)))
        headers = {
            "Origin": "https://pan.wo.cn",
            "Referer": "https://pan.wo.cn/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
        }
        with open(local_path, "rb") as f:
            for i in range(1, total + 1):
                chunk = f.read(self.CHUNK)
                info = self._encrypt_info({
                    "spaceType": "0",
                    "directoryId": self.dir,
                    "batchNo": batch_no,
                    "fileName": name,
                    "fileSize": size,
                    "fileType": self._file_type(name),
                })
                data = {
                    "uniqueId": unique_id, "accessToken": self.token,
                    "fileName": name, "psToken": "undefined",
                    "fileSize": str(size), "totalPart": str(total),
                    "channel": "wocloud", "directoryId": self.dir,
                    "fileInfo": info, "partSize": str(len(chunk)),
                    "partIndex": str(i),
                }
                r = requests.post(self.UPLOAD_URL, headers=headers, data=data,
                                  files={"file": (name, chunk, "application/octet-stream")},
                                  timeout=180)
                try:
                    res = r.json()
                except Exception:
                    raise StorageError("联通云盘返回异常: %s %s" % (r.status_code, r.text[:150]))
                if str(res.get("code")) != "0000":
                    raise StorageError("联通云盘: %s" % (res.get("msg") or res))
        return True

    def test(self):
        if not self.token:
            return False, "未填写联通云盘 accessToken"
        try:
            r = requests.get("https://pan.wo.cn/", timeout=15)
            ok = r.status_code < 500
            return (True, "联通云盘节点可达，Token 将在上传时校验") if ok \
                else (False, "联通云盘返回 %s" % r.status_code)
        except Exception as e:
            return False, "联通云盘连接失败: %s" % e


def _human(n):
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return "%.1f%s" % (n, unit)
        n /= 1024.0


DRIVERS = {
    "local": LocalDriver,
    "webdav": WebDAVDriver,
    "s3": S3Driver,
    "baidu": BaiduPanDriver,
    "cmcc": CmccDriver,
    "ctyun": CtyunDriver,
    "cucc": CuccDriver,
}


# ---------------- 配置读写 ----------------
_default_cfg = {
    "driver": "local",
    "enabled": True,
    "backupRecordings": True,       # 自动备份整段录像
    "uploadEvents": True,           # 上传事件视频与截图
    "keepLocal": True,              # 上传成功后是否保留本地副本
    "config": {"root": os.path.join(os.environ.get("UNICOMAI_HOME", "/opt/unicomai"), "data/cloud_sim")},
}


def load_cfg():
    if not os.path.exists(CFG_PATH):
        os.makedirs(os.path.dirname(CFG_PATH), exist_ok=True)
        json.dump(_default_cfg, open(CFG_PATH, "w"), indent=2, ensure_ascii=False)
        return dict(_default_cfg)
    try:
        cfg = json.load(open(CFG_PATH))
        for k, v in _default_cfg.items():
            cfg.setdefault(k, v)
        return cfg
    except Exception:
        return dict(_default_cfg)


def save_cfg(cfg):
    os.makedirs(os.path.dirname(CFG_PATH), exist_ok=True)
    json.dump(cfg, open(CFG_PATH, "w"), indent=2, ensure_ascii=False)


def get_driver(cfg=None):
    cfg = cfg or load_cfg()
    if not cfg.get("enabled"):
        return None
    cls = DRIVERS.get(cfg.get("driver"))
    if not cls:
        return None
    return cls(cfg.get("config", {}))


def test_driver(cfg=None):
    drv = get_driver(cfg)
    if drv is None:
        return False, "未启用云存储或驱动不支持"
    return drv.test()


# ---------------- 上传队列 ----------------
backup_queue = queue.Queue(maxsize=500)
upload_log = []
_ulock = threading.Lock()


def _log(item):
    with _ulock:
        upload_log.append(item)
        del upload_log[:-200]


def _remux_to_mp4(hevc_path):
    """把裸 H.265 录像转成 H.264 的 MP4（云盘/手机/网页都能直接放）。

    以前只是换封装（-c copy），出来的还是 HEVC，云盘预览和手机相册依旧认不出，
    所以这里统一重编码成 720p H.264。失败返回 None。
    """
    import subprocess
    out = hevc_path[:-5] + ".mp4"
    try:
        r = subprocess.run(
            ["ffmpeg", "-v", "error", "-y", "-f", "hevc", "-i", hevc_path,
             "-map", "0:v:0", "-an",
             "-c:v", "libopenh264", "-b:v", "1200k", "-g", "60",
             "-vf", "scale=-2:720", "-pix_fmt", "yuv420p",
             "-movflags", "+faststart", out],
            capture_output=True, timeout=1800)
        if r.returncode == 0 and os.path.exists(out) and os.path.getsize(out) > 1024:
            return out
    except Exception:
        pass
    try:
        if os.path.exists(out) and os.path.getsize(out) <= 1024:
            os.remove(out)
    except Exception:
        pass
    return None


def _worker():
    while True:
        local_path, kind = backup_queue.get()
        try:
            cfg = load_cfg()
            drv = get_driver(cfg)
            if drv is None:
                continue
            if kind == "recording" and not cfg.get("backupRecordings", True):
                continue
            if kind != "recording" and not cfg.get("uploadEvents", True):
                continue
            rel = os.path.relpath(local_path, os.environ.get("UNICOMAI_HOME", "/opt/unicomai") + "/data")
            # 裸 H.265 录像云盘上没法直接播，先转封装成 MP4 再上传
            up_path = local_path
            if local_path.endswith(".hevc"):
                converted = _remux_to_mp4(local_path)
                if converted:
                    up_path = converted
                rel = os.path.splitext(rel)[0] + ".mp4"
            remote = "%s/%s" % (time.strftime("%Y-%m-%d"), rel.replace(os.sep, "/"))
            ok, err, tries = False, "", 0
            for tries in range(1, 4):
                try:
                    drv.put(up_path, remote)
                    ok = True
                    break
                except Exception as e:
                    err = str(e)
                    time.sleep(2 * tries)
            if up_path != local_path:
                try:
                    os.remove(up_path)
                except Exception:
                    pass
            _log({"file": os.path.basename(local_path), "kind": kind,
                  "ok": ok, "tries": tries, "error": err,
                  "driver": cfg.get("driver"), "remote": remote,
                  "time": time.strftime("%Y-%m-%d %H:%M:%S")})
            if ok and not cfg.get("keepLocal", True) and kind == "recording":
                try:
                    os.remove(local_path)
                except Exception:
                    pass
        except Exception as e:
            _log({"file": os.path.basename(local_path), "kind": kind, "ok": False,
                  "error": str(e), "time": time.strftime("%Y-%m-%d %H:%M:%S")})
        finally:
            backup_queue.task_done()


def start_worker():
    t = threading.Thread(target=_worker, name="uploader", daemon=True)
    t.start()
    return t


def recent_uploads(n=50):
    with _ulock:
        return list(reversed(upload_log))[:n]
