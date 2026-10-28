# 联通 AI 摄像头监控平台（UnicomAI）

自建的联通看家摄像头（沃家总管）Web 监控平台，无需官方 App/插件：

- **实时直播**：H.265 拉流转 H.264，浏览器 MSE 直接播放，实时追帧（延迟 ~1s），
  可选 WebRTC 低延迟模式
- **声音**：G711A 音频随直播下行（AAC）；支持现场收音监听
- **语音对讲**：网页按住说话，音频经平台转发到摄像头喇叭（与官方播放器同协议）
- **云台控制**：方向/变焦/复位
- **自动录像**：切片落盘 → 自动转 720p H.264 MP4（带 AAC 音轨）→ 云盘备份
- **AI 检测**：YOLOv11 抽帧识别人/物，命中自动保存 20s 事件视频 + 3 张截图
- **多摄像头**：设备下拉切换，每台独立拉流/录像/AI/对讲
- **登录认证**：账号密码 + Cookie 会话，连续错 3 次锁定 10 分钟
- **云盘备份**：可插拔驱动（WebDAV / S3 / 百度网盘 / 移动云盘 / 本地）

## 目录结构

```
app/            后端（FastAPI）与前端（单页 index.html）
models/         YOLOv11n ONNX 模型（人/物检测）
bin/guard.sh    守护脚本（进程挂掉自动拉起）
data/           运行数据（录像、事件、配置；首次运行自动生成）
start.sh        一键启动
Dockerfile      容器部署
```

## 快速开始

### 0. 环境要求

- Python 3.10+
- ffmpeg（系统安装：`apt install ffmpeg` / `brew install ffmpeg`）
- 一台已开通「联通看家/沃家总管」的摄像头

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 启动

**本地 / 本地电脑：**

```bash
./start.sh                       # 默认端口 8800，数据存在本目录
PORT=9000 ./start.sh             # 自定义端口
```

**服务器（守护运行）：**

```bash
sudo mkdir -p /opt/unicomai && sudo cp -r . /opt/unicomai
UNICOMAI_HOME=/opt/unicomai nohup ./bin/guard.sh >/dev/null 2>&1 &
```

**Docker / NAS：**

```bash
docker compose up -d --build
```

### 3. 首次配置（网页完成，无需改配置文件）

1. 浏览器打开 `http://<IP>:8800` → 设置管理员账号密码
2. 「账号」页 → 「摄像头接入」→ 按卡片里的教程抓包获取 token，填入并「测试连接」→ 保存
3. 回到「实时直播」即可观看；声音/对讲按钮在直播画面左上角

### 如何获取摄像头 token

联通看家没有开放的登录 API，需要从官方网页抓一次 token：

1. Chrome 打开 [联通看家网页版](https://we.wo.cn/web/smart-club-pc-v2/?clientId=1001000001) 并登录
2. F12 → Network → 点对应摄像头直播
3. 找 `wss://…/h5player/live` 连接，参数 JSON 里有 `token` / `deviceId` / `wsHost` / `relayServer`

token 过期后重新抓一次，在「账号 → 摄像头接入」里更新即可（留空旧字段只改 token）。

## 配置说明

所有运行配置在 `data/` 下（首次运行自动生成，均已加入 .gitignore）：

| 文件 | 内容 |
|---|---|
| `auth.json` | 管理员账号（PBKDF2 加盐哈希） |
| `devices.json` | 摄像头凭据与开关（推荐网页里改） |
| `storage.json` | 云盘备份配置 |
| `recordings/` `events/` | 录像与 AI 事件（自动轮转：7 天 / 60GB） |

环境变量：`UNICOMAI_HOME`（数据根目录，默认 `/opt/unicomai`）、`PORT`（默认 8800）、
`UM_MODEL`（自定义模型路径）。

## 隐私与安全

- 本仓库**不含任何账号凭据**（token、密码、云盘密钥），首次运行时通过网页录入
- 直播/录像/配置接口均需登录；建议公网部署时加 HTTPS 反代（nginx/caddy）
- 摄像头会话单设备独占：平台与官方 App 同时预览会互相顶掉会话

## 常见问题

- **直播黑屏**：优先换 Chrome/Edge；页面会自动降级「兼容模式」
- **没声音**：点直播左上角「🔇 声音关」解除浏览器静音（浏览器要求点击后才出声）
- **对讲没反应**：确认浏览器已授权麦克风；对讲为半双工，说话时监控自动静音
- **云台不动**：机身桥接模式需要 recplayer（可选组件）；无 recplayer 时自动走直连帧
- **内存**：720p 转码单路约 300MB，8GB 内存可稳定跑 4~6 台设备
