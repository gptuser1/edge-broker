import type { Message, TopicState, PublishRequest, WsControlMessage } from "../types";

const DEFAULT_TTL_SEC = 180;
const MAX_MESSAGES_PER_TOPIC = 100;

export class Broker {
  private state: DurableObjectState;
  private env: Env;
  private topics: Map<string, TopicState> = new Map();
  private connections: Map<WebSocket, Set<string>> = new Map();
  private initialized = false;
  private initPromise?: Promise<void>;

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
  }

  private async ensureInitialized() {
    if (this.initialized) return;
    if (this.initPromise) {
      await this.initPromise;
      return;
    }

    this.initPromise = this.loadState();
    await this.initPromise;
    this.initialized = true;
  }

  private async loadState() {
    try {
      const stored = await this.state.storage.get<Record<string, TopicState>>("topics");
      if (stored) {
        this.topics = new Map(Object.entries(stored));
      }
    } catch (e) {
      // Ignore storage errors, start with empty state
    }
  }

  async fetch(request: Request): Promise<Response> {
    await this.ensureInitialized();

    const url = new URL(request.url);
    const path = url.pathname;

    if (path === "/pub") {
      return this.handlePublish(request);
    }

    if (path === "/sub") {
      return this.handleSubscribe(request);
    }

    if (path === "/stop-sub") {
      return this.handleUnsubscribe(request);
    }

    return new Response("Not found", { status: 404 });
  }

  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer) {
    if (typeof message !== "string") return;

    try {
      const data: WsControlMessage = JSON.parse(message);
      if (data.type === "subscribe" && data.topic) {
        this.subscribeToTopic(ws, data.topic);
      } else if (data.type === "unsubscribe" && data.topic) {
        this.unsubscribeFromTopic(ws, data.topic);
      }
    } catch (e) {
      // Ignore invalid messages
    }
  }

  async webSocketClose(ws: WebSocket, code: number, reason: string, wasClean: boolean) {
    this.cleanupConnection(ws);
  }

  async webSocketError(ws: WebSocket, error: unknown) {
    this.cleanupConnection(ws);
  }

  async alarm() {
    await this.ensureInitialized();

    const now = Date.now();
    let hasChanges = false;

    for (const [topic, state] of this.topics) {
      const before = state.messages.length;
      state.messages = state.messages.filter((m) => m.expiresAt > now);
      if (state.messages.length !== before) {
        hasChanges = true;
      }
    }

    if (hasChanges) {
      this.persistTopics();
    }

    this.resetAlarm();
  }

  private async handlePublish(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const topic = url.searchParams.get("service");

    if (!topic) {
      return new Response(JSON.stringify({ success: false, message: "Missing service parameter" }), {
        status: 400,
        headers: { "Content-Type": "application/json" },
      });
    }

    let body: PublishRequest;
    try {
      body = await request.json();
    } catch {
      return new Response(JSON.stringify({ success: false, message: "Invalid JSON body" }), {
        status: 400,
        headers: { "Content-Type": "application/json" },
      });
    }

    const ttl = body.ttl && body.ttl > 0 ? Math.max(body.ttl, 60) : DEFAULT_TTL_SEC;
    const now = Date.now();

    const message: Message = {
      id: crypto.randomUUID(),
      data: body.data,
      timestamp: now,
      expiresAt: now + ttl * 1000,
      topic,
    };

    // Get or create topic state
    const topicState = this.getTopicState(topic);
    topicState.messages.push(message);

    // Trim old messages
    if (topicState.messages.length > MAX_MESSAGES_PER_TOPIC) {
      topicState.messages = topicState.messages.slice(-MAX_MESSAGES_PER_TOPIC);
    }

    // Handle retain
    if (body.retain) {
      topicState.retained = {
        data: body.data,
        timestamp: now,
      };
    }

    this.persistTopics();
    this.resetAlarm();

    // Broadcast to all subscribers
    this.broadcastToTopic(topic, message);

    return new Response(
      JSON.stringify({
        success: true,
        messageId: message.id,
      }),
      {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }
    );
  }

  private async handleSubscribe(request: Request): Promise<Response> {
    const upgradeHeader = request.headers.get("Upgrade");
    if (upgradeHeader !== "websocket") {
      return new Response("Expected websocket", { status: 400 });
    }

    const url = new URL(request.url);
    const topicsParam = url.searchParams.get("service");
    const clientIdParam = url.searchParams.get("clientId");

    if (!topicsParam) {
      return new Response(JSON.stringify({ success: false, message: "Missing service parameter" }), {
        status: 400,
        headers: { "Content-Type": "application/json" },
      });
    }

    const topics = topicsParam.split(",").map((t) => t.trim()).filter(Boolean);
    const clientId = clientIdParam || crypto.randomUUID();

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    this.state.acceptWebSocket(server);

    // Initialize connection
    this.connections.set(server, new Set());

    // Send welcome message first
    const welcomeMsg = {
      success: true,
      clientId,
      topics: topics,
    };
    server.send(JSON.stringify(welcomeMsg));

    // Subscribe to requested topics (will send retained messages)
    for (const topic of topics) {
      this.subscribeToTopic(server, topic, false);
    }

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }

  private async handleUnsubscribe(request: Request): Promise<Response> {
    // Note: with single DO architecture, clients can just close the connection
    // This endpoint is kept for API compatibility
    return new Response(
      JSON.stringify({
        success: true,
        message: "Connection will be closed",
      }),
      {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }
    );
  }

  private subscribeToTopic(ws: WebSocket, topic: string, sendAck = true) {
    const topics = this.connections.get(ws);
    if (!topics) return;

    if (topics.has(topic)) return;

    topics.add(topic);

    // Send retained message if exists
    const topicState = this.topics.get(topic);
    if (topicState?.retained) {
      const retainedMsg: Message = {
        id: "retained",
        data: topicState.retained.data,
        timestamp: topicState.retained.timestamp,
        expiresAt: topicState.retained.timestamp + DEFAULT_TTL_SEC * 1000,
        topic,
      };
      ws.send(JSON.stringify(retainedMsg));
    }

    if (sendAck) {
      ws.send(JSON.stringify({ type: "subscribed", topic }));
    }
  }

  private unsubscribeFromTopic(ws: WebSocket, topic: string) {
    const topics = this.connections.get(ws);
    if (!topics) return;

    topics.delete(topic);

    ws.send(JSON.stringify({ type: "unsubscribed", topic }));
  }

  private broadcastToTopic(topic: string, message: Message) {
    const msgStr = JSON.stringify(message);

    for (const [ws, topics] of this.connections) {
      if (topics.has(topic)) {
        try {
          ws.send(msgStr);
        } catch (e) {
          // Connection might be dead, clean it up
          this.cleanupConnection(ws);
        }
      }
    }
  }

  private cleanupConnection(ws: WebSocket) {
    this.connections.delete(ws);
  }

  private getTopicState(topic: string): TopicState {
    let state = this.topics.get(topic);
    if (!state) {
      state = { messages: [] };
      this.topics.set(topic, state);
    }
    return state;
  }

  private persistTopics() {
    const plain: Record<string, TopicState> = {};
    for (const [k, v] of this.topics) {
      plain[k] = v;
    }
    this.state.storage.put("topics", plain);
  }

  private resetAlarm() {
    let earliestExpiry = Infinity;

    for (const state of this.topics.values()) {
      for (const msg of state.messages) {
        if (msg.expiresAt < earliestExpiry) {
          earliestExpiry = msg.expiresAt;
        }
      }
    }

    if (earliestExpiry !== Infinity) {
      this.state.storage.setAlarm(earliestExpiry);
    }
  }
}

interface Env {
  // Env is not used directly in DO, but kept for type consistency
}
