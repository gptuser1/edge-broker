# edge-hub-do

基于 Cloudflare Workers + Durable Objects 的实时 Pub/Sub 消息总线。

## 特点

- ⚡ **实时推送**：WebSocket 实时消息推送，延迟极低
- 🚀 **并行广播**：消息广播并行执行，大幅降低广播延迟
- 💤 **Hibernatable WebSockets**：休眠模式大幅减少 DO 计算时间，节省成本
- 🎯 **单实例保证**：Durable Objects 确保每个主题只有一个实例，状态一致
- 💾 **状态持久化**：消息和客户端状态持久化到 DO storage，重启不丢失
- ⏰ **自动清理**：Alarms API 自动清理过期消息，无需手动维护
- 🔄 **断线重连**：支持通过 clientId 恢复会话
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
TopicHub (DO) - 管理主题、消息队列、客户端列表
    ↓
ClientHub (DO) - 每个客户端一个实例，管理 WebSocket 连接
```

### 核心组件

- **TopicHub**：每个主题一个 DO 实例，维护消息队列和客户端 ID 集合
- **ClientHub**：每个客户端一个 DO 实例，管理 WebSocket 连接和多主题订阅
- **广播机制**：TopicHub 收到消息后，并行调用所有 ClientHub 推送

## 项目结构

```
edge-hub-do/
├── src/
│   ├── index.ts                    # 主入口，路由转发
│   ├── types.ts                    # 类型定义
│   └── durable-objects/
│       ├── topic-hub.ts            # TopicHub DO
│       └── client-hub.ts           # ClientHub DO
├── wrangler.toml                   # Wrangler 配置
├── tsconfig.json                   # TypeScript 配置
├── package.json
└── README.md
```

## API

### 发布消息

```bash
curl -X POST "https://your-worker.workers.dev/pub?service=my-service" \
  -H "Content-Type: application/json" \
  -d '{"data": "hello world", "ttl": 180, "retain": false}'
```

**参数**：
- `service`：主题/服务名称（URL 参数）
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
const ws = new WebSocket("wss://your-worker.workers.dev/sub?service=my-service");

// 订阅多个主题（逗号分隔）
const ws = new WebSocket("wss://your-worker.workers.dev/sub?service=topic1,topic2,topic3");

// 使用自定义 clientId
const ws = new WebSocket("wss://your-worker.workers.dev/sub?service=my-service&clientId=my-device-001");

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
- `service`：主题/服务名称，多个主题用逗号分隔（URL 参数）
- `clientId`：自定义客户端 ID（可选，用于断线重连）

**消息格式**：
```json
{
  "id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
  "data": "hello world",
  "timestamp": 1782133304088,
  "expiresAt": 1782133364088,
  "topic": "my-service"
}
```

**连接成功后第一条消息**：
```json
{
  "success": true,
  "clientId": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "topics": ["my-service"]
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

### 取消订阅 / 断开连接

```bash
curl -X POST "https://your-worker.workers.dev/stop-sub?service=my-service" \
  -H "Content-Type: application/json" \
  -d '{"clientId": "your-client-id"}'
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
