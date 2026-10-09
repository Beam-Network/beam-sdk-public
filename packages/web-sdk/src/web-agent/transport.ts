import { assertNotSecretCredential } from "../config.js";
import {
  BeamAbortError,
  BeamAuthError,
  BeamConfigError,
  BeamError,
  BeamNetworkError,
  BeamTimeoutError,
} from "../core/errors/index.js";
import {
  asError,
  duration,
  identifier,
  inputId,
  object,
  PROTOCOL,
  protocolError,
  remoteError,
  text,
} from "./protocol.js";
import type { AgentRequestOptions, AgentSession, WebAgentOptions } from "./types.js";

interface Pending {
  receive: (value: unknown, error: unknown) => void;
  stop: (reason: BeamError) => void;
}
export interface WireResource {
  event(type: string, data: unknown, error: unknown): void;
  disconnected(reason: BeamError): void;
}

/** Internal transport: never exposes a generic command executor in package exports. */
export class AgentTransport {
  readonly resources = new Map<string, WireResource>();
  readonly #options: WebAgentOptions;
  readonly #url: string;
  readonly #pending = new Map<string, Pending>();
  #socket?: WebSocket;
  #attempt?: AbortController;
  #opening?: Promise<AgentSession>;
  #rejectOpening?: (error: BeamError) => void;
  #counter = 0;
  #connected = false;
  #session?: AgentSession;
  onState: (state: "connecting" | "connected" | "disconnected") => void = () => {};
  onEvent: (type: string, data: unknown) => void = () => {};
  onError: (error: BeamError) => void = () => {};

  constructor(options: WebAgentOptions) {
    this.#options = { ...options };
    let url: URL;
    try {
      url = new URL(options.url);
    } catch {
      throw new BeamConfigError("Provide a valid Web Agent WebSocket URL.");
    }
    const loopback = ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname);
    if (
      (url.protocol !== "wss:" && !(url.protocol === "ws:" && loopback)) ||
      url.username ||
      url.password ||
      url.search ||
      url.hash
    ) {
      throw new BeamConfigError(
        "Use wss (or ws on loopback), without URL credentials, query parameters, or fragments.",
      );
    }
    if (typeof options.getCredential !== "function")
      throw new BeamConfigError("Provide getCredential for the instance API credential.");
    if (options.expectedAgentId !== undefined) inputId(options.expectedAgentId);
    duration(options.timeoutMs, 30000);
    this.#url = url.href;
  }
  get connected(): boolean {
    return this.#connected;
  }

  connect(options: AgentRequestOptions = {}): Promise<AgentSession> {
    if (options.signal?.aborted) return Promise.reject(new BeamAbortError());
    if (this.#connected && this.#session) return Promise.resolve({ ...this.#session });
    if (this.#opening) return this.#opening;
    const timeout = duration(options.timeoutMs, this.#options.timeoutMs ?? 30000);
    const attempt = new AbortController();
    this.#attempt = attempt;
    this.#counter = 0;
    const promise = new Promise<AgentSession>((resolve, reject) => {
      this.#rejectOpening = reject;
      const abort = (): void => this.disconnect(new BeamAbortError());
      const timer = setTimeout(() => this.disconnect(new BeamTimeoutError("Web Agent connection timed out.")), timeout);
      const cleanup = (): void => {
        clearTimeout(timer);
        options.signal?.removeEventListener("abort", abort);
      };
      attempt.signal.addEventListener("abort", cleanup, { once: true });
      options.signal?.addEventListener("abort", abort, { once: true });
      void Promise.resolve()
        .then(() => this.#options.getCredential(attempt.signal))
        .then((credential) => {
          if (attempt.signal.aborted) return;
          assertNotSecretCredential(credential, "getCredential");
          if (typeof credential !== "string" || !credential || credential.length > 8192)
            throw new BeamConfigError("The instance API credential is missing or invalid.");
          const socket = this.#options.webSocketFactory?.(this.#url, PROTOCOL) ?? new WebSocket(this.#url, PROTOCOL);
          this.#socket = socket;
          socket.binaryType = "arraybuffer";
          socket.onopen = () => {
            if (this.#socket !== socket) return;
            try {
              socket.send(JSON.stringify({ type: "session.authenticate", requestId: "auth", token: credential }));
            } catch {
              this.disconnect(new BeamNetworkError("Web Agent authentication could not be sent."));
            } finally {
              credential = "";
              socket.onopen = null;
            }
          };
          socket.onerror = () => {
            if (this.#socket === socket) this.disconnect(new BeamNetworkError("Web Agent connection failed."));
          };
          socket.onclose = (event) => {
            if (this.#socket !== socket) return;
            this.disconnect(
              !this.#connected && event.code === 1008
                ? new BeamAuthError("Web Agent authentication was rejected.")
                : new BeamNetworkError("Web Agent disconnected; operations were not replayed."),
            );
          };
          socket.onmessage = (event) => {
            if (this.#socket !== socket) return;
            try {
              if (typeof event.data !== "string" || new TextEncoder().encode(event.data).length > 256 << 10)
                throw protocolError();
              const wire = object(JSON.parse(event.data));
              const type = text(wire.type);
              if (!this.#connected) {
                if (type !== "session.ready" || wire.requestId !== "auth") throw protocolError();
                const data = object(wire.data);
                if (data.protocol !== PROTOCOL) throw protocolError();
                const status = object(data.status);
                const agentId = status.agentId === undefined ? undefined : identifier(status.agentId);
                const bootId = status.bootId === undefined ? undefined : identifier(status.bootId);
                this.#checkIdentity(agentId, bootId);
                this.#session = {
                  id: identifier(data.sessionId),
                  connected: status.connected === true,
                  ...(agentId === undefined ? {} : { agentId }),
                  ...(bootId === undefined ? {} : { bootId }),
                };
                this.#connected = true;
                this.#rejectOpening = undefined;
                cleanup();
                resolve({ ...this.#session });
                this.onState("connected");
                return;
              }
              if (type === "response") {
                this.#pending.get(text(wire.requestId))?.receive(wire.data, wire.error);
              } else if (wire.resourceId !== undefined) {
                this.resources.get(identifier(wire.resourceId))?.event(type, wire.data, wire.error);
              } else {
                if (type === "connection.state" && this.#options.expectedAgentId !== undefined) {
                  const status = object(wire.data);
                  this.#checkIdentity(status.agentId, status.bootId);
                }
                this.onEvent(type, wire.data);
              }
            } catch (error) {
              this.disconnect(asError(error));
            }
          };
        })
        .catch((error) => {
          if (!attempt.signal.aborted)
            this.disconnect(
              error instanceof BeamError ? error : new BeamAuthError("Unable to obtain the instance API credential."),
            );
        });
    });
    this.#opening = promise;
    this.onState("connecting");
    void promise.then(
      () => {
        if (this.#opening === promise) this.#opening = undefined;
      },
      () => {
        if (this.#opening === promise) this.#opening = undefined;
      },
    );
    return promise;
  }

  #checkIdentity(agentId: unknown, bootId: unknown): void {
    if (
      this.#options.expectedAgentId !== undefined &&
      (agentId !== this.#options.expectedAgentId ||
        typeof bootId !== "string" ||
        !bootId ||
        bootId.length > 256 ||
        (this.#session !== undefined && bootId !== this.#session.bootId))
    )
      throw new BeamAuthError("Web Agent identity does not match the configured agent.");
  }

  disconnect(reason: BeamError = new BeamAbortError("Web Agent connection closed.")): void {
    const active = !!this.#socket || !!this.#attempt;
    this.#connected = false;
    this.#session = undefined;
    const socket = this.#socket;
    this.#socket = undefined;
    this.#attempt?.abort();
    this.#attempt = undefined;
    this.#rejectOpening?.(reason);
    this.#rejectOpening = undefined;
    this.#opening = undefined;
    if (socket) {
      socket.onopen = socket.onmessage = socket.onclose = socket.onerror = null;
      try {
        socket.close(1000);
      } catch {
        /* Already closed. */
      }
    }
    for (const pending of [...this.#pending.values()]) pending.stop(reason);
    this.#pending.clear();
    for (const resource of [...this.resources.values()]) resource.disconnected(reason);
    this.resources.clear();
    if (active) {
      if (!(reason instanceof BeamAbortError)) this.onError(reason);
      this.onState("disconnected");
    }
  }

  #send(type: string, fields: Record<string, unknown>, id: string): void {
    const socket = this.#socket;
    if (!socket || socket.readyState !== 1)
      throw new BeamNetworkError("Connect to the Web Agent before making requests.");
    const body = JSON.stringify({ type, requestId: id, ...fields });
    const size = new TextEncoder().encode(body).length;
    if (size > 256 << 10 || socket.bufferedAmount + size > 1 << 20)
      throw new BeamError("Web Agent request buffer is full.", { code: "resource_exhausted" });
    try {
      socket.send(body);
    } catch {
      throw new BeamNetworkError("Web Agent request could not be sent.");
    }
  }
  control(type: string, fields: Record<string, unknown>): void {
    if (!this.#connected) return;
    try {
      if (this.#counter >= 4090) throw new BeamNetworkError("Reconnect to renew the Web Agent session.");
      this.#send(type, fields, String(++this.#counter));
    } catch (error) {
      this.disconnect(asError(error));
    }
  }
  request<T>(
    type: string,
    fields: Record<string, unknown>,
    parse: (value: unknown) => T,
    options: AgentRequestOptions = {},
    lateCleanup?: (value: unknown) => void,
  ): Promise<T> {
    if (options.signal?.aborted) return Promise.reject(new BeamAbortError());
    if (!this.#connected)
      return Promise.reject(new BeamNetworkError("Connect to the Web Agent before making requests."));
    if (this.#pending.size >= 28 || this.#counter >= 4000)
      return Promise.reject(
        new BeamError("Web Agent request limit reached; wait for pending requests or reconnect to renew the session.", {
          code: "resource_exhausted",
        }),
      );
    const timeout = duration(options.timeoutMs, this.#options.timeoutMs ?? 30000);
    const id = String(++this.#counter);
    return new Promise<T>((resolve, reject) => {
      let cancelled = false;
      const cleanup = (): void => {
        clearTimeout(timer);
        options.signal?.removeEventListener("abort", abort);
        this.#pending.delete(id);
      };
      const stop = (reason: BeamError): void => {
        cleanup();
        reject(reason);
      };
      const cancel = (reason: BeamError): void => {
        if (cancelled) return;
        cancelled = true;
        clearTimeout(timer);
        options.signal?.removeEventListener("abort", abort);
        reject(reason);
        // Keep ownership of late responses. If cancellation remains uncertain,
        // ending this session ensures no abandoned media/subscription survives.
        timer = setTimeout(
          () => this.disconnect(new BeamTimeoutError("Web Agent cancellation was not confirmed.")),
          30000,
        );
        this.control("request.cancel", { targetRequestId: id });
      };
      const abort = (): void => cancel(new BeamAbortError());
      let timer = setTimeout(
        () => cancel(new BeamTimeoutError("Web Agent request timed out; its result may be uncertain.")),
        timeout,
      );
      this.#pending.set(id, {
        stop,
        receive: (value, error) => {
          cleanup();
          if (cancelled) {
            if (!error && lateCleanup) lateCleanup(value);
            return;
          }
          try {
            if (error) reject(remoteError(error));
            else resolve(parse(value));
          } catch (failure) {
            reject(asError(failure));
            this.disconnect(asError(failure));
          }
        },
      });
      options.signal?.addEventListener("abort", abort, { once: true });
      try {
        this.#send(type, fields, id);
      } catch (error) {
        stop(asError(error));
      }
    });
  }
}
