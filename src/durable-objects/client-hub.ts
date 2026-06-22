import type {
  Env,
  ClientState,
  InternalMsg,
  InternalReg,
  SubResponse,
  BaseResponse,
  WsControlMessage,
  Message,
} from "../types";

export class ClientHub implements DurableObject {
  private state: DurableObjectState;
  private env: Env;
  private clientState: ClientState;
  private readonly clientId: string;
  private topicsSet: Set<string>;

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
    this.clientId = state.id.toString();
    this.clientState = { topics: [], connected: false };
    this.topicsSet = new Set();

    this.state.blockConcurrencyWhile(async () => {
      const saved = await this.state.storage.get<ClientState>("client_state");
      if (saved) {
        this.clientState = saved;
        this.topicsSet = new Set(saved.topics);
      }
    });
  }

  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);
    const path = url.pathname;

    const jsonRes = (data: unknown, status = 200) =>
      new Response(JSON.stringify(data), {
        headers: { "Content-Type": "application/json" },
        status,
      });

    // WebSocket subscribe - supports multiple topics
    if (path === "/sub" && req.headers.get("Upgrade") === "websocket") {
      const topicsParam = url.searchParams.get("topics");
      const topics = topicsParam
        ? topicsParam
            .split(",")
            .map((t) => decodeURIComponent(t).trim())
            .filter((t) => t.length > 0)
        : [];

      return this.initWebSocket(topics);
    }

    // Push message from TopicHub
    if (path === "/push" && req.method === "POST") {
      const payload = await req.json<InternalMsg>();
      return this.pushMessage(payload);
    }

    // Manual unsubscribe / disconnect
    if (path === "/stop-sub" && req.method === "POST") {
      return this.handleDisconnect();
    }

    return jsonRes({ success: false, message: "Client internal endpoint not found" }, 404);
  }

  // Hibernatable WebSocket: handle incoming messages from client
  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer) {
    try {
      if (typeof message !== "string") return;

      const data = JSON.parse(message) as WsControlMessage;

      if (data.type === "subscribe" && data.topic) {
        await this.subscribeTopic(data.topic);
      } else if (data.type === "unsubscribe" && data.topic) {
        await this.unsubscribeTopic(data.topic);
      }
    } catch (err) {
      console.error("Failed to handle WS message:", (err as Error).message);
    }
  }

  // Hibernatable WebSocket: handle connection close
  async webSocketClose(ws: WebSocket, code: number, reason: string, wasClean: boolean) {
    await this.unregisterFromAllTopics();
    this.clientState.connected = false;
    await this.persistState();
  }

  // Hibernatable WebSocket: handle connection error
  async webSocketError(ws: WebSocket, error: unknown) {
    console.error("WebSocket error:", error);
    await this.unregisterFromAllTopics();
    this.clientState.connected = false;
    await this.persistState();
  }

  private initWebSocket(topics: string[]): Response {
    const [clientWs, serverWs] = Object.values(new WebSocketPair());

    // Use hibernatable WebSocket
    this.state.acceptWebSocket(serverWs);

    this.clientState.connected = true;
    this.persistState();

    // Register to all requested topics
    for (const topic of topics) {
      this.subscribeTopic(topic).catch((err) => {
        console.error("Failed to subscribe to topic:", topic, err);
      });
    }

    // Send clientId and subscribed topics as first message
    // We need to send via the WebSocket - but with hibernatable mode,
    // we can still send messages directly
    // Wait - with state.acceptWebSocket, we can't send directly?
    // Actually we can, let's try getting the WS from state
    // Actually, with hibernatable mode, we need to use state.getWebSockets()
    // Or we can send before accepting? No, acceptWebSocket must be called first

    // Let's send the welcome message
    // In hibernatable mode, we can still send via the serverWs reference
    // Actually no - after acceptWebSocket, the WS is managed by the runtime
    // But we can still use serverWs.send() in some implementations
    // Let's try a different approach: send before accepting? No, that doesn't work

    // Actually, looking at Cloudflare docs: after calling state.acceptWebSocket(ws),
    // you can still use ws.send() to send messages.
    // The hibernation just means the DO can sleep when there are no messages.

    // Wait, but we need to be careful. Let's send the welcome message.
    try {
      serverWs.send(
        JSON.stringify({
          success: true,
          clientId: this.clientId,
          topics: topics,
        } as SubResponse)
      );
    } catch (err) {
      console.error("Failed to send welcome message:", (err as Error).message);
    }

    return new Response(null, {
      status: 101,
      webSocket: clientWs,
      headers: {
        Upgrade: "websocket",
        Connection: "Upgrade",
      },
    });
  }

  private async pushMessage(payload: InternalMsg): Promise<Response> {
    const { msg, topic } = payload;

    if (!this.clientState.connected) {
      return new Response(JSON.stringify({ success: false, message: "Client disconnected" }), {
        status: 400,
        headers: { "Content-Type": "application/json" },
      });
    }

    // Get all active WebSocket connections
    const websockets = this.state.getWebSockets();

    if (websockets.length === 0) {
      // No active connections - client probably disconnected
      await this.unregisterFromAllTopics();
      this.clientState.connected = false;
      await this.persistState();
      return new Response(JSON.stringify({ success: false, message: "Client disconnected" }), {
        status: 400,
        headers: { "Content-Type": "application/json" },
      });
    }

    // Add topic to message if not already present
    const messageToSend: Message = {
      ...msg,
      topic: msg.topic || topic,
    };

    // Send to all WebSocket connections (should be 1 in most cases)
    for (const ws of websockets) {
      try {
        ws.send(JSON.stringify(messageToSend));
      } catch (err) {
        console.error("Failed to send message to client:", (err as Error).message);
      }
    }

    return new Response(JSON.stringify({ success: true }), {
      headers: { "Content-Type": "application/json" },
    });
  }

  private async handleDisconnect(): Promise<Response> {
    // Close all WebSocket connections
    const websockets = this.state.getWebSockets();
    for (const ws of websockets) {
      try {
        ws.close(1000, "Manual disconnect");
      } catch (err) {
        // Ignore - connection might already be closed
      }
    }

    await this.unregisterFromAllTopics();
    this.clientState = { topics: [], connected: false };
    this.topicsSet.clear();
    await this.persistState();

    return new Response(JSON.stringify({ success: true } as BaseResponse), {
      headers: { "Content-Type": "application/json" },
    });
  }

  private async subscribeTopic(topic: string): Promise<void> {
    if (this.topicsSet.has(topic)) return;

    try {
      const topicStub = this.env.TOPIC_DURABLE_OBJECT.get(
        this.env.TOPIC_DURABLE_OBJECT.idFromName(topic)
      );

      await topicStub.fetch(
        new Request("http://internal/reg", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "X-Topic": topic,
          },
          body: JSON.stringify({ clientId: this.clientId } as InternalReg),
        })
      );

      this.topicsSet.add(topic);
      await this.persistState();
    } catch (err) {
      console.error("Subscribe failed:", topic, (err as Error).message);
    }
  }

  private async unsubscribeTopic(topic: string): Promise<void> {
    if (!this.topicsSet.has(topic)) return;

    try {
      const topicStub = this.env.TOPIC_DURABLE_OBJECT.get(
        this.env.TOPIC_DURABLE_OBJECT.idFromName(topic)
      );

      await topicStub.fetch(
        new Request("http://internal/unreg", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "X-Topic": topic,
          },
          body: JSON.stringify({ clientId: this.clientId } as InternalReg),
        })
      );
    } catch (err) {
      console.error("Unsubscribe failed:", topic, (err as Error).message);
    }

    this.topicsSet.delete(topic);
    await this.persistState();
  }

  private async unregisterFromAllTopics(): Promise<void> {
    const unregisterPromises: Promise<void>[] = [];

    for (const topic of this.topicsSet) {
      unregisterPromises.push(this.unsubscribeTopic(topic));
    }

    await Promise.allSettled(unregisterPromises);
    this.topicsSet.clear();
  }

  private async persistState(): Promise<void> {
    this.clientState.topics = Array.from(this.topicsSet);
    await this.state.storage.put("client_state", this.clientState);
  }
}
