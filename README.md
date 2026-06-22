# edge-hub-do

基于 Cloudflare Workers + Durable Objects 的实时 Pub/Sub 消息总线。

## 特点

- ⚡ **实时推送**：WebSocket 实时消息推送，延迟极低
- 🎯 **单实例保证**：Durable Objects 确保每个主题只有一个实例，状态一致
- 💾 **状态持久化**：消息和客户端状态持久化到 DO storage，重启不丢失
- ⏰ **自动清理**：Alarms API 自动清理过期消息，无需手动维护
- 🔄 **断线重连**：支持通过 clientId 恢复会话
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
- **ClientHub**：每个客户端一个 DO 实例，持有 WebSocket 连接
- **广播机制**：TopicHub 收到消息后，遍历所有客户端 ID，逐个调用 ClientHub 推送

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
  -d '{"data": "hello world", "ttl": 180}'
```

**参数**：
- `service`：主题/服务名称（URL 参数）
- `data`：消息内容，可以是任意 JSON
- `ttl`：消息过期时间（秒），默认 180

**响应**：
```json
{
  "success": true,
  "messageId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
}
```

### 订阅消息

```javascript
const ws = new WebSocket("wss://your-worker.workers.dev/sub?service=my-service");

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
- `service`：主题/服务名称（URL 参数）

**消息格式**：
```json
{
  "id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
  "data": "hello world",
  "timestamp": 1782133304088,
  "expiresAt": 1782133364088
}
```

**连接成功后第一条消息**：
```json
{
  "success": true,
  "clientId": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
}
```

### 取消订阅

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
- **Wrangler** - 开发部署工具

## License

MIT
