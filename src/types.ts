// Client State
export interface ClientState {
  topic: string;
  ws: WebSocket | null;
  connected: boolean;
}

// Message stored in TopicHub
export interface Message {
  id: string;
  data: unknown;
  timestamp: number;
  expiresAt: number;
}

// Request body when publish
export interface PublishRequest {
  data: unknown;
  ttl?: number;
}

// Internal reg/unreg request between DOs
export interface InternalReg {
  clientId: string;
}

// Internal push message from TopicHub to ClientHub
export interface InternalMsg {
  msg: Message;
}

// Response sent to WS client on connect
export interface SubResponse {
  success: boolean;
  clientId: string;
}

// Base JSON response
export interface BaseResponse {
  success: boolean;
  message?: string;
}

// Env bindings for Worker
export interface Env {
  TOPIC_DURABLE_OBJECT: DurableObjectNamespace;
  CLIENT_DURABLE_OBJECT: DurableObjectNamespace;
}
