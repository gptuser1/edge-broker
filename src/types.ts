// Client State
export interface ClientState {
  topics: string[];
  connected: boolean;
}

// Message stored in TopicHub
export interface Message {
  id: string;
  data: unknown;
  timestamp: number;
  expiresAt: number;
  topic?: string;
}

// Retained message per topic
export interface RetainedMessage {
  data: unknown;
  timestamp: number;
}

// Request body when publish
export interface PublishRequest {
  data: unknown;
  ttl?: number;
  retain?: boolean;
}

// Internal reg/unreg request between DOs
export interface InternalReg {
  clientId: string;
}

// Internal push message from TopicHub to ClientHub
export interface InternalMsg {
  msg: Message;
  topic: string;
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
  TOPIC_DURABLE_OBJECT: DurableObjectNamespace;
  CLIENT_DURABLE_OBJECT: DurableObjectNamespace;
}
