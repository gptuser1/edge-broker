import type { Env, Message, PublishRequest, InternalReg, InternalMsg, RetainedMessage } from "../types";

const DEFAULT_TTL_SEC = 180;

interface TopicState {
  messages: Message[];
  clientIds: string[];
  retained?: RetainedMessage;
}

export class TopicHub implements DurableObject {
  private state: DurableObjectState;
  private env: Env;
  private stateData: TopicState;
  private clientIdsSet: Set<string>;
  private topicName: string = "";

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

    // Extract topic name from header if not set yet
    if (!this.topicName) {
      const topicHeader = req.headers.get("X-Topic");
      if (topicHeader) {
        this.topicName = topicHeader;
      }
    }

    const jsonRes = (data: unknown, status = 200) =>
      new Response(JSON.stringify(data), {
        headers: { "Content-Type": "application/json" },
        status,
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

      // Send retained message to new subscriber if exists
      if (this.stateData.retained) {
        const retainedMsg: Message = {
          id: "retained",
          data: this.stateData.retained.data,
          timestamp: this.stateData.retained.timestamp,
          expiresAt: Date.now() + DEFAULT_TTL_SEC * 1000,
          topic: this.topicName,
        };

        const clientStub = this.env.CLIENT_DURABLE_OBJECT.get(
          this.env.CLIENT_DURABLE_OBJECT.idFromString(clientId)
        );

        clientStub
          .fetch(
            new Request("http://internal/push", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ msg: retainedMsg, topic: this.topicName } as InternalMsg),
            })
          )
          .catch((err) => console.error("Push retained message failed", clientId, err));
      }

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
    this.stateData.messages = this.stateData.messages.filter((m) => m.expiresAt > now);
    await this.persistState();
    await this.resetAlarm();
  }

  private async handlePublish(body: PublishRequest): Promise<Response> {
    const now = Date.now();
    const ttlMs = (body.ttl ?? DEFAULT_TTL_SEC) * 1000;

    const newMsg: Message = {
      id: crypto.randomUUID(),
      data: body.data,
      timestamp: now,
      expiresAt: now + ttlMs,
      topic: this.topicName,
    };

    this.stateData.messages.push(newMsg);

    // Handle retained message
    if (body.retain) {
      this.stateData.retained = {
        data: body.data,
        timestamp: now,
      };
    }

    await this.resetAlarm();
    await this.persistState();

    // Broadcast to all connected clients - parallel
    const broadcastPromises: Promise<unknown>[] = [];

    for (const cid of this.clientIdsSet) {
      const clientStub = this.env.CLIENT_DURABLE_OBJECT.get(
        this.env.CLIENT_DURABLE_OBJECT.idFromString(cid)
      );

      const promise = clientStub
        .fetch(
          new Request("http://internal/push", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ msg: newMsg, topic: this.topicName } as InternalMsg),
          })
        )
        .catch((err) => {
          console.error("Push to client failed", cid, err);
          // Remove dead client
          this.clientIdsSet.delete(cid);
          this.persistState();
        });

      broadcastPromises.push(promise);
    }

    // Don't wait for all broadcasts to complete - fire and forget
    // But we track them for potential error handling
    Promise.allSettled(broadcastPromises).catch(() => {
      // Ignore - errors already handled individually
    });

    return new Response(JSON.stringify({ success: true, messageId: newMsg.id }), {
      headers: { "Content-Type": "application/json" },
    });
  }

  private async resetAlarm() {
    if (this.stateData.messages.length === 0) {
      await this.state.storage.deleteAlarm();
      return;
    }

    const earliestExpire = Math.min(...this.stateData.messages.map((m) => m.expiresAt));
    await this.state.storage.setAlarm(earliestExpire);
  }

  private async persistState() {
    this.stateData.clientIds = Array.from(this.clientIdsSet);
    await this.state.storage.put("topic_state", this.stateData);
  }
}
