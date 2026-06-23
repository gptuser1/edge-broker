import type { Env } from "./types";
import { Broker } from "./durable-objects/broker";

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url);
    const path = url.pathname;

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
          <title>Edge Broker - Pub/Sub Service</title>
          <style>
            body { font-family: Arial, sans-serif; max-width: 800px; margin: 2rem auto; padding: 0 1rem; }
            h1 { color: #2d3748; }
            .endpoint { margin: 1rem 0; padding: 1rem; background: #f7fafc; border-radius: 6px; }
            .code { font-family: monospace; background: #e2e8f0; padding: 0.25rem 0.5rem; border-radius: 3px; }
            .note { color: #718096; font-size: 0.9em; }
          </style>
        </head>
        <body>
          <h1>Edge Broker - Pub/Sub Service</h1>
          <p>Lightweight edge Pub/Sub message broker based on Cloudflare Durable Objects</p>
          
          <div class="endpoint">
            <h3>Publish Message</h3>
            <p>Method: POST</p>
            <p>URL: <span class="code">/pub?service=YOUR_TOPIC</span></p>
            <p>Body (JSON): <span class="code">{ "data": "your-message", "ttl": 180, "retain": false }</span></p>
            <p class="note">Set <code>retain: true</code> to keep the latest message for new subscribers.</p>
          </div>

          <div class="endpoint">
            <h3>Subscribe to Topics</h3>
            <p>Protocol: WebSocket</p>
            <p>URL: <span class="code">wss://your-domain/sub?service=topic1,topic2&clientId=optional-id</span></p>
            <p class="note">Multiple topics separated by comma. Single connection supports multiple topics.</p>
          </div>

          <div class="endpoint">
            <h3>WebSocket Control Messages</h3>
            <p>After connecting, you can send JSON messages to subscribe/unsubscribe:</p>
            <p><span class="code">{ "type": "subscribe", "topic": "new-topic" }</span></p>
            <p><span class="code">{ "type": "unsubscribe", "topic": "old-topic" }</span></p>
          </div>

          <div class="endpoint">
            <h3>Features</h3>
            <ul>
              <li>Single DO architecture - simple and fast</li>
              <li>Multi-topic subscription over single WebSocket</li>
              <li>Retained messages for new subscribers</li>
              <li>Hibernatable WebSockets for lower cost</li>
              <li>Automatic message expiration via Alarms API</li>
            </ul>
          </div>
        </body>
        </html>
      `;
      return new Response(html, {
        headers: { "Content-Type": "text/html; charset=UTF-8" },
      });
    }

    // Get broker DO stub (single instance)
    const brokerId = env.BROKER_DURABLE_OBJECT.idFromName("default");
    const brokerStub = env.BROKER_DURABLE_OBJECT.get(brokerId);

    // Forward all other requests to broker DO
    return brokerStub.fetch(req);
  },
};

export { Broker };
