# NapCatQQ 部署

这个目录用于在 `spider` 项目内运行 NapCatQQ，用一个 QQ 号登录后接收 QQ 群消息。

完整服务器部署流程见：

```text
../../hobby-box/docs/QQ群监控服务器部署说明.md
```

## 启动

先复制环境变量并修改 WebUI token：

```bash
cd /Users/gaoge/www/gougou/spider/napcat
cp .env.example .env
vim .env
```

启动：

```bash
./scripts/start.sh
```

WebUI 地址：

```text
http://127.0.0.1:6099/webui?token=你的_NAPCAT_WEBUI_TOKEN
```

进入 WebUI 后扫码登录 QQ 号，并在网络配置里启用 OneBot HTTP、WebSocket 或反向 WebSocket。

如果接入 `hobby-box` 的 QQ 群监控，推荐先用 HTTP 上报：

```text
上报地址：http://host.docker.internal:8080/callback/qq/onebot?token=你的_QQ_ONEBOT_CALLBACK_TOKEN
```

其中后端环境变量需要配置：

```text
QQ_ONEBOT_CALLBACK_TOKEN=你的_QQ_ONEBOT_CALLBACK_TOKEN
```

如果后端部署在服务器，把 `host.docker.internal:8080` 换成后端公网域名即可。

## 端口

- `6099`: NapCat WebUI
- `3000`: OneBot HTTP
- `3001`: OneBot WebSocket

## 群消息事件

账号加入 QQ 群后，NapCat 会通过 OneBot 事件上报群消息，核心字段大概是：

```json
{
  "post_type": "message",
  "message_type": "group",
  "group_id": 123456,
  "user_id": 10001,
  "raw_message": "消息内容"
}
```

如果要把消息接入后端，推荐使用反向 WebSocket，把事件推到自己的服务；这样不需要后端轮询。

## 停止

```bash
./scripts/stop.sh
```

## 数据目录

`data/` 存放 NapCat 配置、QQ 登录态和运行数据，已加入 `.gitignore`，不要提交。
