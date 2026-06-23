// Message stored per topic
export interface Message {
  id: string;
  data: unknown;
  timestamp: number;
  expiresAt: number;
  topic: string;
}

// Retained message per topic
export interface RetainedMessage {
  data: unknown;
  timestamp: number;
}

// Topic state
export interface TopicState {
  messages: Message[];
  retained?: RetainedMessage;
}

// Request body when publish
export interface PublishRequest {
  data: unknown;
  ttl?: number;
  retain?: boolean;
}

// Response sent to WS client on connect
export interface SubResponse {
  success: boolean;
  clientId: string;
  topics: string[];
}

// Base JSON response
export interface BaseResponse {
  success: boolean;
  message?: string;
}

// WS control message from client
export interface WsControlMessage {
  type: "subscribe" | "unsubscribe";
  topic: string;
}

// Env bindings for Worker
export interface Env {
  BROKER_DURABLE_OBJECT: DurableObjectNamespace;
}
