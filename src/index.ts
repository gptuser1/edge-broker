import type { Env } from "./types";
import { TopicHub } from "./durable-objects/topic-hub";
import { ClientHub } from "./durable-objects/client-hub";

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url);
    const path = url.pathname;
    const service = url.searchParams.get("service");
    const clientIdParam = url.searchParams.get("clientId");

    const jsonRes = (data: unknown, status = 200) =>
      new Response(JSON.stringify(data), {
        headers: { "Content-Type": "application/json" },
        status,
      });

    // Root simple HTML homepage
    if (path === "/") {
      const html = `
        <!DOCTYPE html>
        <html lang="en">
        <head>
          <meta charset="UTF-8">
          <meta name="viewport" content="width=device-width, initial-scale=1.0">
          <title>Edge Hub DO - Pub/Sub Service</title>
          <style>
            body { font-family: Arial, sans-serif; max-width: 800px; margin: 2rem auto; padding: 0 1rem; }
            h1 { color: #2d3748; }
            .endpoint { margin: 1rem 0; padding: 1rem; background: #f7fafc; border-radius: 6px; }
            .code { font-family: monospace; background: #e2e8f0; padding: 0.25rem 0.5rem; border-radius: 3px; }
            .note { color: #718096; font-size: 0.9em; }
          </style>
        </head>
        <body>
          <h1>Edge Hub DO - Pub/Sub Service</h1>
          <p>Real-time Pub/Sub based on Cloudflare Durable Objects</p>
          
          <div class="endpoint">
            <h3>Publish Message</h3>
            <p>Method: POST</p>
            <p>URL: <span class="code">/pub?service=YOUR_SERVICE</span></p>
            <p>Body (JSON): <span class="code">{ "data": "your-message", "ttl": 180, "retain": false }</span></p>
            <p class="note">Set <code>retain: true</code> to keep the latest message for new subscribers.</p>
          </div>

          <div class="endpoint">
            <h3>Subscribe to Topics</h3>
            <p>Protocol: WebSocket</p>
            <p>URL: <span class="code">wss://your-domain/sub?service=topic1,topic2&clientId=optional-id</span></p>
            <p class="note">Multiple topics separated by comma. Custom clientId is optional.</p>
          </div>

          <div class="endpoint">
            <h3>Unsubscribe / Disconnect</h3>
            <p>Method: POST</p>
            <p>URL: <span class="code">/stop-sub?service=YOUR_SERVICE</span></p>
            <p>Body (JSON): <span class="code">{ "clientId": "your-client-id" }</span></p>
          </div>

          <div class="endpoint">
            <h3>WebSocket Control Messages</h3>
            <p>After connecting, you can send JSON messages to subscribe/unsubscribe:</p>
            <p><span class="code">{ "type": "subscribe", "topic": "new-topic" }</span></p>
            <p><span class="code">{ "type": "unsubscribe", "topic": "old-topic" }</span></p>
          </div>
        </body>
        </html>
      `;
      return new Response(html, {
        headers: { "Content-Type": "text/html; charset=UTF-8" },
      });
    }

    // Publish - requires service
    if (path === "/pub" && req.method === "POST") {
      if (!service) {
        return jsonRes({ success: false, message: "Missing 'service' query parameter" }, 400);
      }
      const topicStub = env.TOPIC_DURABLE_OBJECT.get(env.TOPIC_DURABLE_OBJECT.idFromName(service));
      // Forward request with topic name header
      const forwardReq = new Request(req.url, {
        method: req.method,
        headers: {
          ...Object.fromEntries(req.headers),
          "X-Topic": service,
        },
        body: req.body,
      });
      return topicStub.fetch(forwardReq);
    }

    // WebSocket subscribe - supports multiple topics and custom clientId
    if (path === "/sub") {
      if (!service) {
        return jsonRes({ success: false, message: "Missing 'service' query parameter" }, 400);
      }

      // Parse topics - support comma-separated or multiple service params
      const topics = service
        .split(",")
        .map((t) => t.trim())
        .filter((t) => t.length > 0);

      if (topics.length === 0) {
        return jsonRes({ success: false, message: "No valid topics specified" }, 400);
      }

      // Generate or use custom clientId
      let clientIdObj: DurableObjectId;
      if (clientIdParam) {
        // Custom clientId - use idFromName for stability
        clientIdObj = env.CLIENT_DURABLE_OBJECT.idFromName(clientIdParam);
      } else {
        // Auto-generated unique ID
        clientIdObj = env.CLIENT_DURABLE_OBJECT.newUniqueId();
      }

      const clientStub = env.CLIENT_DURABLE_OBJECT.get(clientIdObj);
      const topicsParam = topics.map((t) => encodeURIComponent(t)).join(",");

      const wsRequest = new Request(`http://internal/sub?topics=${topicsParam}`, {
        method: "GET",
        headers: {
          Upgrade: req.headers.get("Upgrade") ?? "",
          Connection: req.headers.get("Connection") ?? "",
          "Sec-WebSocket-Key": req.headers.get("Sec-WebSocket-Key") ?? "",
          "Sec-WebSocket-Version": req.headers.get("Sec-WebSocket-Version") ?? "",
          "Sec-WebSocket-Extensions": req.headers.get("Sec-WebSocket-Extensions") ?? "",
        },
      });

      return clientStub.fetch(wsRequest);
    }

    // Manual unsubscribe / disconnect
    if (path === "/stop-sub" && req.method === "POST") {
      try {
        const body = await req.json();
        const { clientId } = body as { clientId?: string };
        if (!clientId) {
          return jsonRes({ success: false, message: "Missing 'clientId' in request body" }, 400);
        }
        const clientStub = env.CLIENT_DURABLE_OBJECT.get(env.CLIENT_DURABLE_OBJECT.idFromString(clientId));
        return clientStub.fetch(req);
      } catch (e) {
        return jsonRes({ success: false, message: (e as Error).message }, 500);
      }
    }

    return jsonRes(
      { success: false, message: "Route not found. Available: /, /pub, /sub, /stop-sub" },
      404
    );
  },
};

export { TopicHub, ClientHub };
