import type { Env, ClientState, InternalMsg, InternalReg, SubResponse, BaseResponse } from "../types";

export class ClientHub implements DurableObject {
  private state: DurableObjectState;
  private env: Env;
  private clientState: ClientState;
  private readonly clientId: string;

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
    this.clientId = state.id.toString();
    this.clientState = { topic: "", ws: null, connected: false };

    this.state.blockConcurrencyWhile(async () => {
      const saved = await this.state.storage.get<ClientState>("client_state");
      if (saved) this.clientState = saved;
    });
  }

  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);
    const path = url.pathname;
    const jsonRes = (data: unknown, status = 200) => new Response(JSON.stringify(data), {
      headers: { "Content-Type": "application/json" },
      status
    });

    if (path === "/sub" && req.headers.get("Upgrade") === "websocket") {
      const topic = url.searchParams.get("topic")!;
      return this.initWebSocket(topic);
    }

    if (path === "/push" && req.method === "POST") {
      const payload = await req.json<InternalMsg>();
      return this.pushMessage(payload);
    }

    if (path === "/stop-sub" && req.method === "POST") {
      return this.handleUnsubscribe();
    }

    return jsonRes({ success: false, message: "Client internal endpoint not found" }, 404);
  }

  private initWebSocket(topic: string): Response {
    if (this.clientState.connected) {
      return new Response(JSON.stringify({ success: false, message: "Already connected to a service" }), {
        status: 400,
        headers: { "Content-Type": "application/json" }
      });
    }

    const [clientWs, serverWs] = Object.values(new WebSocketPair());
    serverWs.accept();

    this.clientState = { topic, ws: serverWs, connected: true };
    this.state.storage.put("client_state", this.clientState);

    serverWs.addEventListener("close", () => this.unregisterFromTopic());
    serverWs.addEventListener("error", () => this.unregisterFromTopic());

    this.registerToTopic(topic);

    // Send clientId as first WS frame, 101 response with empty body
    serverWs.send(JSON.stringify({ success: true, clientId: this.clientId } as SubResponse));

    return new Response(null, {
      status: 101,
      webSocket: clientWs,
      headers: {
        Upgrade: "websocket",
        Connection: "Upgrade"
      }
    });
  }

  private async pushMessage(payload: InternalMsg): Promise<Response> {
    const { msg } = payload;
    if (!this.clientState.connected || !this.clientState.ws || this.clientState.ws.readyState !== WebSocket.OPEN) {
      await this.unregisterFromTopic();
      return new Response(JSON.stringify({ success: false, message: "Client disconnected" }), {
        status: 400,
        headers: { "Content-Type": "application/json" }
      });
    }
    this.clientState.ws.send(JSON.stringify(msg));
    return new Response(JSON.stringify({ success: true }), {
      headers: { "Content-Type": "application/json" }
    });
  }

  private async handleUnsubscribe(): Promise<Response> {
    await this.unregisterFromTopic();
    this.clientState.ws?.close(1000, "Manual unsubscribe");
    this.clientState = { topic: "", ws: null, connected: false };
    await this.state.storage.put("client_state", this.clientState);
    return new Response(JSON.stringify({ success: true } as BaseResponse), {
      headers: { "Content-Type": "application/json" }
    });
  }

  private async registerToTopic(topic: string) {
    try {
      const topicStub = this.env.TOPIC_DURABLE_OBJECT.get(this.env.TOPIC_DURABLE_OBJECT.idFromName(topic));
      await topicStub.fetch(new Request("http://internal/reg", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ clientId: this.clientId } as InternalReg)
      }));
    } catch (err) {
      console.error("Register failed:", (err as Error).message);
      this.unregisterFromTopic();
    }
  }

  private async unregisterFromTopic() {
    const targetTopic = this.clientState.topic;
    if (!targetTopic) return;
    try {
      const topicStub = this.env.TOPIC_DURABLE_OBJECT.get(this.env.TOPIC_DURABLE_OBJECT.idFromName(targetTopic));
      await topicStub.fetch(new Request("http://internal/unreg", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ clientId: this.clientId } as InternalReg)
      }));
    } catch (err) {
      console.error("Unregister failed:", (err as Error).message);
    }
    this.clientState.connected = false;
    await this.state.storage.put("client_state", this.clientState);
  }
}
