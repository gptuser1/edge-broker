# edge-broker

基于 Cloudflare Workers + Durable Objects 的轻量级边缘 Pub/Sub 消息代理。

## 特点

- ⚡ **实时推送**：WebSocket 实时消息推送，延迟极低
- 🎯 **单 DO 架构**：单个 Broker DO 管理所有主题，架构极简
- 💤 **Hibernatable WebSockets**：休眠模式大幅减少 DO 计算时间，节省成本
- 💾 **状态持久化**：消息持久化到 DO storage，重启不丢失
- ⏰ **自动清理**：Alarms API 自动清理过期消息，无需手动维护
- 📌 **保留消息**：新订阅者立即获取最新值（Retained Message）
- 🎛️ **多主题订阅**：单连接订阅多个主题，减少连接开销
- 🆔 **自定义 Client ID**：支持自定义客户端 ID，便于管理
- 🔌 **简单 API**：HTTP 发布 + WebSocket 订阅，易于集成

## 架构

```
Client (WebSocket)
    ↓
Worker (无状态，路由转发)
    ↓
Broker (DO) - 单实例管理所有主题、消息和连接
```

### 核心组件

- **Broker**：单个 DO 实例，管理所有主题的消息和 WebSocket 连接
- **广播机制**：直接在内存中向对应主题的所有连接推送消息，零延迟

## 项目结构

```
edge-broker/
├── src/
│   ├── index.ts                    # 主入口，路由转发
│   ├── types.ts                    # 类型定义
│   └── durable-objects/
│       └── broker.ts               # Broker DO
├── wrangler.toml                   # Wrangler 配置
├── tsconfig.json                   # TypeScript 配置
├── package.json
└── README.md
```

## API

### 发布消息

```bash
curl -X POST "https://your-worker.workers.dev/pub?service=my-topic" \
  -H "Content-Type: application/json" \
  -d '{"data": "hello world", "ttl": 180, "retain": false}'
```

**参数**：
- `service`：主题名称（URL 参数）
- `data`：消息内容，可以是任意 JSON
- `ttl`：消息过期时间（秒），默认 180
- `retain`：是否保留为最新消息，新订阅者会立即收到，默认 false

**响应**：
```json
{
  "success": true,
  "messageId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
}
```

### 订阅消息

```javascript
// 订阅单个主题
const ws = new WebSocket("wss://your-worker.workers.dev/sub?service=my-topic");

// 订阅多个主题（逗号分隔）
const ws = new WebSocket("wss://your-worker.workers.dev/sub?service=topic1,topic2,topic3");

// 使用自定义 clientId
const ws = new WebSocket("wss://your-worker.workers.dev/sub?service=my-topic&clientId=my-device-001");

ws.onopen = () => {
  console.log("Connected");
};

ws.onmessage = (event) => {
  const message = JSON.parse(event.data);
  console.log("Received:", message);
};

ws.onclose = () => {
  console.log("Disconnected");
};
```

**参数**：
- `service`：主题名称，多个主题用逗号分隔（URL 参数）
- `clientId`：自定义客户端 ID（可选）

**消息格式**：
```json
{
  "id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
  "data": "hello world",
  "timestamp": 1782133304088,
  "expiresAt": 1782133364088,
  "topic": "my-topic"
}
```

**连接成功后第一条消息**：
```json
{
  "success": true,
  "clientId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
  "topics": ["my-topic"]
}
```

### WebSocket 控制消息

连接建立后，可以通过发送 JSON 消息动态订阅或取消订阅主题：

```javascript
// 订阅新主题
ws.send(JSON.stringify({
  type: "subscribe",
  topic: "new-topic"
}));

// 取消订阅主题
ws.send(JSON.stringify({
  type: "unsubscribe",
  topic: "old-topic"
}));
```

## 开发与部署

### 前置要求

- Node.js 18+
- wrangler CLI (`npm install -g wrangler`)

### 安装依赖

```bash
npm install
```

### 本地开发

```bash
npm run dev
```

### 类型检查

```bash
npm run typecheck
```

### 部署

```bash
# 登录 Cloudflare
wrangler login

# 部署
npm run deploy
```

## 环境变量

| 变量名 | 默认值 | 说明 |
|---|---|---|
| `DEFAULT_TTL` | 180 | 消息默认过期时间（秒） |

## 技术栈

- **TypeScript** - 类型安全
- **Cloudflare Workers** - 边缘计算
- **Durable Objects** - 有状态的边缘计算
- **Hibernatable WebSockets** - 休眠模式 WebSocket，节省计算资源
- **Wrangler** - 开发部署工具

## License

MIT
