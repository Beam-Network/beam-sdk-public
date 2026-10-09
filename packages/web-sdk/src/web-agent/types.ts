import type { Emitter } from "../core/events/index.js";
import type { BeamError } from "../core/errors/index.js";

export interface AgentRequestOptions {
  signal?: AbortSignal;
  timeoutMs?: number;
}
export interface WebAgentOptions {
  /** Full endpoint, e.g. wss://agent.example/v1/connect. ws is loopback-only. */
  url: string;
  /** Instance API credential only; never a Beam API key or agent credential. */
  getCredential: (signal: AbortSignal) => string | Promise<string>;
  /** Pin the enrolled identity for server-side control. Requires agentId and bootId; changes close the connection. */
  expectedAgentId?: string;
  timeoutMs?: number;
  iceServers?: RTCIceServer[];
  iceGatheringTimeoutMs?: number;
  mediaConnectionTimeoutMs?: number;
  webSocketFactory?: (url: string, protocol: string) => WebSocket;
  peerConnectionFactory?: (configuration: RTCConfiguration) => RTCPeerConnection;
}
export interface AgentSession {
  id: string;
  connected: boolean;
  /** Stable enrolled identity, when supplied by the Web Agent. */
  agentId?: string;
  /** Changes when the Web Agent process restarts, when supplied. */
  bootId?: string;
}
export interface AgentMembership {
  roomId: string;
  memberId: string;
  state: string;
}
export interface AgentChannel {
  id: string;
  name: string;
  kind: "message" | "media" | "stream";
  state: string;
  publish: boolean;
  subscribe: boolean;
  ready: boolean;
}
export interface AgentChannelScope {
  roomId: string;
  channelId: string;
}
export interface AgentBroadcast {
  id: string;
  updatedAt: string;
}
export interface AgentMessage {
  id: string;
  senderId: string;
  sequence: number;
  contentType: string;
  payload: Uint8Array;
  receivedAt: string;
}
export interface AgentPublication {
  id: string;
  sequence: number;
  online: number;
  accepted: number;
  delivered: number;
  failed: number;
  expired: number;
  skipped: number;
  duplicate: boolean;
  queued: boolean;
}
export type WebAgentEvents = {
  state: { state: "connecting" | "connected" | "disconnected" };
  availability: { connected: boolean };
  membership: AgentMembership;
  error: BeamError;
};
export type AgentSubscriptionEvents = {
  ready: undefined;
  message: AgentMessage;
  broadcasts: readonly AgentBroadcast[];
  error: BeamError;
  closed: undefined;
};
export type AgentMediaEvents = {
  state: { state: RTCPeerConnectionState };
  track: { track: MediaStreamTrack; stream: MediaStream };
  error: BeamError;
  closed: undefined;
};
export interface AgentPublishMediaOptions extends AgentRequestOptions {
  /** Defaults to false: caller-provided capture tracks remain caller-owned. */
  stopTracksOnClose?: boolean;
}

export interface AgentSubscription extends Emitter<AgentSubscriptionEvents> {
  readonly scope: Readonly<AgentChannelScope>;
  readonly kind: "message" | "media";
  readonly state: "preparing" | "ready" | "closed";
  readonly ready: Promise<void>;
  readonly broadcasts: readonly AgentBroadcast[];
  close(): Promise<void>;
}
export interface AgentMediaSession extends Emitter<AgentMediaEvents> {
  readonly stream: MediaStream;
  readonly state: RTCPeerConnectionState;
  /** Resolves when WebRTC connects, not merely when signaling returns an answer. */
  readonly ready: Promise<void>;
  close(): Promise<void>;
}
