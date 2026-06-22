import type { Env, Message, PublishRequest, InternalReg, InternalMsg } from "../types";
import { randomUUID } from "node:crypto";

const DEFAULT_TTL_SEC = 180;

interface TopicState {
  messages: Message[];
  clientIds: string[];
}

export class TopicHub implements DurableObject {
  private state: DurableObjectState;
  private env: Env;
  private stateData: TopicState;
  private clientIdsSet: Set<string>;

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
    this.stateData = { messages: [], clientIds: [] };
    this.clientIdsSet = new Set();

    this.state.blockConcurrencyWhile(async () => {
      const saved = await this.state.storage.get<TopicState>("topic_state");
      if (saved) {
        this.stateData = saved;
        this.clientIdsSet = new Set(saved.clientIds);
      }
    });
  }

  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);
    const path = url.pathname;
    const jsonRes = (data: unknown, status = 200) => new Response(JSON.stringify(data), {
      headers: { "Content-Type": "application/json" },
      status
    });

    // Publish message from worker /pub
    if (req.method === "POST" && path === "/") {
      const body = await req.json<PublishRequest>();
      return this.handlePublish(body);
    }

    // Register client from ClientHub
    if (req.method === "POST" && path === "/reg") {
      const { clientId } = await req.json<InternalReg>();
      this.clientIdsSet.add(clientId);
      await this.persistState();
      return jsonRes({ success: true });
    }

    // Unregister client from ClientHub
    if (req.method === "POST" && path === "/unreg") {
      const { clientId } = await req.json<InternalReg>();
      this.clientIdsSet.delete(clientId);
      await this.persistState();
      return jsonRes({ success: true });
    }

    return jsonRes({ success: false, message: "Unknown topic internal route" }, 404);
  }

  // Alarm trigger for TTL cleanup
  async alarm() {
    const now = Date.now();
    this.stateData.messages = this.stateData.messages.filter(m => m.expiresAt > now);
    await this.persistState();
    await this.resetAlarm();
  }

  private async handlePublish(body: PublishRequest): Promise<Response> {
    const now = Date.now();
    const ttlMs = (body.ttl ?? DEFAULT_TTL_SEC) * 1000;
    const newMsg: Message = {
      id: randomUUID(),
      data: body.data,
      timestamp: now,
      expiresAt: now + ttlMs
    };

    this.stateData.messages.push(newMsg);
    await this.resetAlarm();
    await this.persistState();

    // Broadcast to all connected clients
    for (const cid of this.clientIdsSet) {
      const clientStub = this.env.CLIENT_DURABLE_OBJECT.get(this.env.CLIENT_DURABLE_OBJECT.idFromString(cid));
      clientStub.fetch(new Request("http://internal/push", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ msg: newMsg } as InternalMsg)
      })).catch(err => console.error("Push to client failed", cid, err));
    }

    return new Response(JSON.stringify({ success: true, messageId: newMsg.id }), {
      headers: { "Content-Type": "application/json" }
    });
  }

  private async resetAlarm() {
    if (this.stateData.messages.length === 0) {
      await this.state.storage.deleteAlarm();
      return;
    }
    const earliestExpire = Math.min(...this.stateData.messages.map(m => m.expiresAt));
    await this.state.storage.setAlarm(earliestExpire);
  }

  private async persistState() {
    this.stateData.clientIds = Array.from(this.clientIdsSet);
    await this.state.storage.put("topic_state", this.stateData);
  }
}
