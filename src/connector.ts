// Connector mode (docs/specs/always-on-supply.md §5): the node dials OUT to the market over an authenticated WebSocket
// (wss://<api>/v1/seller/connect, `Authorization: Bearer icn_…`, subprotocol inferit.connector.v1) and serves its
// offers over that socket. A seller behind NAT or a VPN, or a rented GPU box with no https, needs no public port, no
// tunnel and no TLS of its own; the market never connects to the node.
//
// Each `req` frame becomes a loopback request to this node's own HTTP server (createSellerNode, listening on
// 127.0.0.1) with its SELLER_TOKEN, so validation, MODEL_MAP, mock mode and the upstream error mapping are the same as
// over HTTP. The response goes back as a `head` frame (status, content type, provider-fingerprint headers only), then
// binary DATA frames with the body bytes exactly as received, then `end`. Flow control: the node never sends more body
// bytes than the market granted (`credit`); when it runs out it stops reading its server, which pushes back on vLLM or
// Ollama.
//
// Reconnects back off 1, 2, 4, 8, 16, 30, then 60 s with ±20% jitter, capped at 60 s (reset after a session that
// stayed up 60 s). Every attempt is a fresh WebSocket on a fresh TCP/TLS connection, with its own HTTP agent for the
// requests it relays, and every failed attempt is logged with its reason (the error code, the HTTP status and the
// market's refusal message). An attempt that is not live 45 s after it started is dropped. A refused token (401/403,
// close 4401/4403) waits 5 minutes and logs loudly, so a revoked token does not hammer the market. Every 30 minutes,
// and when the market says `drain`, the node opens a new session before the old one ends (make before break). A drain
// the node did not ask for, on a session younger than 60 s, means another copy with the same token keeps taking the
// session over: the node then waits on the backoff ladder instead, so two copies do not trade it in a tight loop.
//
// The welcome lists the node's offers on the market ({offerId, model, maxContextTokens}); while the session lasts, the
// market sends an `offers` frame with the new list whenever the seller lists, edits or delists one ('offers' event).
// `updateDetected` re-sends the hello on a new session (make before break) when what the local server reports changes,
// such as the context Ollama serves a model with, which it says only while the model is loaded.
//
// The watchdog (opt-in, the CLI turns it on): with no live session for 10 minutes, it calls `onTrip`. Under launchd,
// systemd, the Windows task or Docker the CLI logs and exits 1, so the service manager starts a fresh process; a
// process nobody would restart logs loudly and keeps trying. A Mac node once sat for 21 hours "reconnect scheduled" in
// a process that could no longer connect while a fresh process connected at once.
// Nothing here logs request or response bodies, or the token.
import { EventEmitter } from 'node:events';
import http from 'node:http';
import WebSocket, { type ClientOptions, type RawData } from 'ws';
import { fingerprintOf } from './providers.js';

export const CONNECTOR_SUBPROTOCOL = 'inferit.connector.v1';
export const NODE_TOKEN_RE = /^icn_[A-Za-z0-9_-]{43}$/;
export const FRAME_DATA = 0x01;
export const MAX_DATA_PAYLOAD_BYTES = 256 * 1024;
export const MAX_REQ_FRAME_BYTES = 12 * 1024 * 1024;
/** Reconnect delays before jitter (ms). */
export const BACKOFF_STEPS_MS = [1_000, 2_000, 4_000, 8_000, 16_000, 30_000, 60_000] as const;
/** No reconnect delay is longer than this, jitter included (the 5-minute wait after a refused token is separate). */
export const MAX_BACKOFF_MS = 60_000;
export const AUTH_RETRY_MS = 5 * 60_000;
export const STABLE_AFTER_MS = 60_000;
export const RECYCLE_EVERY_MS = 30 * 60_000;
export const HEARTBEAT_MS = 20_000;
/** An attempt that has no welcome this long after it started (handshake 15 s + hello) is dropped and retried. */
export const ATTEMPT_DEADLINE_MS = 45_000;
/** The watchdog's default: no live session for this long and the process gives up (see WatchdogOptions). */
export const WATCHDOG_NO_SESSION_MS = 10 * 60_000;
export const WATCHDOG_CHECK_MS = 15_000;
/** Hello limits, matching the market's (apps/api/src/connector/protocol.ts): 64 KiB control frames, 64 models,
 *  200 detected entries, 64-character agent fields, 200-character names, 32-character quantisation. */
export const MAX_HELLO_BYTES = 60 * 1024;
export const MAX_DECLARED_MODELS = 64;
export const MAX_DETECTED_MODELS = 200;
export const MAX_AGENT_FIELD_CHARS = 64;
export const MAX_DETECTED_NAME_CHARS = 200;
export const MAX_QUANTIZATION_CHARS = 32;
export const DETECTED_SERVERS = ['ollama', 'lmstudio', 'vllm', 'llamacpp', 'custom'] as const;
export type DetectedServer = (typeof DETECTED_SERVERS)[number];

export interface ModelDecl {
  id: string;
  maxContextTokens?: number;
  quantization?: string;
}

/** Which app runs the node (optional in the hello; display only on the market). */
export interface AgentInfo {
  name: string;
  version: string;
  os: string;
  arch: string;
}

/** A model found on the machine's local inference server (optional in the hello; display only on the market). */
export interface DetectedModel {
  /** The name the local server uses, e.g. qwen2.5:14b-instruct. */
  local: string;
  server: DetectedServer;
  /** The catalog id it maps to, or null: not in the catalog (not served). */
  model: string | null;
  contextLength?: number;
  quantization?: string;
  sizeBytes?: number;
}

/** One of this node's offers on the market, as the welcome and `offers` frames list it. */
export interface NodeOffer {
  offerId: string;
  /** The model name the market asks for (the catalog id unless the offer names its own upstream model). */
  model: string;
  /** The longest prompt the offer accepts; null = the catalog's window; absent = a market that does not say. */
  maxContextTokens?: number | null;
}

/** The offers list of a welcome or `offers` frame; malformed entries are dropped. */
export function parseNodeOffers(raw: unknown): NodeOffer[] {
  if (!Array.isArray(raw)) return [];
  const out: NodeOffer[] = [];
  for (const o of raw.slice(0, 1_000)) {
    if (o === null || typeof o !== 'object') continue;
    const r = o as Record<string, unknown>;
    if (typeof r.offerId !== 'string' || typeof r.model !== 'string') continue;
    const e: NodeOffer = { offerId: r.offerId.slice(0, 64), model: r.model.slice(0, MAX_DETECTED_NAME_CHARS) };
    if (r.maxContextTokens === null) e.maxContextTokens = null;
    else if (typeof r.maxContextTokens === 'number' && Number.isSafeInteger(r.maxContextTokens) && r.maxContextTokens > 0) e.maxContextTokens = r.maxContextTokens;
    out.push(e);
  }
  return out;
}

/** Why an attempt failed, as far as the node can tell. */
export interface AttemptFailure {
  /** ws or socket error message, e.g. "getaddrinfo ENOTFOUND api.example" or "Opening handshake has timed out". */
  error: string | null;
  /** The error code, e.g. ECONNREFUSED, ENOTFOUND, ETIMEDOUT. */
  code: string | null;
  /** The HTTP status of a refused upgrade. */
  httpStatus: number | null;
  /** The market's refusal ({error: {code, message}} in the 4xx body), e.g. node_revoked. */
  refusal: { code: string | null; message: string | null } | null;
  /** The WebSocket close code (1006 = the connection dropped without a close frame). */
  closeCode: number;
}

export interface WatchdogInfo {
  /** Time without a live session (monotonic ms, so time asleep does not count). */
  noSessionMs: number;
  /** Failed attempts since the last live session. */
  failedAttempts: number;
  lastFailure: AttemptFailure | null;
}

export interface WatchdogOptions {
  /** Give up after this long without a live session. Default 10 minutes. */
  noSessionMs?: number;
  /** How often to check. Default 15 s. */
  checkEveryMs?: number;
  /** Called when it trips. Under a service manager the CLI logs and exits 1, so a fresh process starts. */
  onTrip: (info: WatchdogInfo) => void;
  /** Keep watching after a trip, counting again from the trip (a process nobody restarts: warn, do not exit).
   *  Default false: it trips once. */
  repeat?: boolean;
}

export interface Timers {
  setTimeout(fn: () => void, ms: number): unknown;
  clearTimeout(handle: unknown): void;
  setInterval(fn: () => void, ms: number): unknown;
  clearInterval(handle: unknown): void;
  now(): number;
  /** Wall-clock ms (Date.now). Optional: with it, a heartbeat that finds the wall clock far ahead of the monotonic
   *  clock knows the computer slept, and redials at once instead of waiting for missed pongs. */
  wallNow?(): number;
}

export const realTimers: Timers = {
  setTimeout: (fn, ms) => {
    const h = setTimeout(fn, ms);
    h.unref?.();
    return h;
  },
  clearTimeout: (h) => clearTimeout(h as NodeJS.Timeout),
  setInterval: (fn, ms) => {
    const h = setInterval(fn, ms);
    h.unref?.();
    return h;
  },
  clearInterval: (h) => clearInterval(h as NodeJS.Timeout),
  now: () => performance.now(),
  wallNow: () => Date.now(),
};

/** Wall-clock time this far beyond the monotonic time between two heartbeats means the computer slept. */
export const SLEEP_GAP_MS = 30_000;

export interface ConnectorLog {
  info(obj: object, msg: string): void;
  warn(obj: object, msg: string): void;
  error(obj: object, msg: string): void;
}

const silent: ConnectorLog = { info: () => undefined, warn: () => undefined, error: () => undefined };

export interface ConnectorOptions {
  /** wss://<api>/v1/seller/connect (ws:// only for a local market). */
  url: string;
  /** The node token (icn_…), sent only in the Authorization header. */
  token: string;
  node: { name: string; version: string };
  /** The wallet the seller's account signed in with: the market closes the session (4403) if it differs. */
  wallet?: string;
  /** The catalog ids this node serves (MODEL_MAP keys), with what it actually serves of each. */
  models: ModelDecl[];
  maxConcurrent: number;
  /** This node's own HTTP server (createSellerNode) and its SELLER_TOKEN. */
  local: { baseUrl: string; token?: string };
  heartbeatMs?: number;
  /** Planned make-before-break reconnect interval (0 = never). */
  recycleMs?: number;
  stableMs?: number;
  authRetryMs?: number;
  maxReqFrameBytes?: number;
  timers?: Timers;
  /** For jitter (tests pin it). */
  random?: () => number;
  log?: ConnectorLog;
  /** Extra ws client options (e.g. a CA for a private market). */
  wsOptions?: ClientOptions;
  /** Sent in the hello (optional fields; older markets ignore them). */
  agent?: AgentInfo;
  detected?: DetectedModel[];
  /** Drop an attempt that is not live this long after it started. Default 45 s. */
  attemptDeadlineMs?: number;
  /** Off unless given (tests and library use never exit). */
  watchdog?: WatchdogOptions;
}

/** The reconnect delay after `attempt` failures in a row: the ladder, ±20% jitter, never above 60 s. */
export function reconnectDelayMs(attempt: number, random: () => number = Math.random): number {
  const base = BACKOFF_STEPS_MS[Math.min(Math.max(0, Math.trunc(attempt)), BACKOFF_STEPS_MS.length - 1)]!;
  const r = Math.min(1, Math.max(0, random()));
  return Math.min(MAX_BACKOFF_MS, Math.round(base * (1 + (r * 2 - 1) * 0.2)));
}

/** Control characters out, trimmed, cut to `max` characters. */
function capped(raw: unknown, max: number): string {
  return typeof raw === 'string' ? raw.replace(/[\x00-\x1f\x7f]/g, '').trim().slice(0, max) : '';
}

const QUANT_RE = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,31}$/;
const isServer = (s: unknown): s is DetectedServer => typeof s === 'string' && (DETECTED_SERVERS as readonly string[]).includes(s);
const posInt = (n: unknown, max: number): n is number => typeof n === 'number' && Number.isSafeInteger(n) && n >= 1 && n <= max;

/** A detected list the market accepts: at most 200 entries, strings cleaned and capped, bad optional fields dropped. */
export function sanitizeDetected(list: readonly DetectedModel[] | undefined): DetectedModel[] {
  const out: DetectedModel[] = [];
  for (const d of list ?? []) {
    if (out.length >= MAX_DETECTED_MODELS) break;
    const local = capped(d?.local, MAX_DETECTED_NAME_CHARS);
    if (!local) continue;
    const model = capped(d.model, MAX_DETECTED_NAME_CHARS);
    const e: DetectedModel = { local, server: isServer(d.server) ? d.server : 'custom', model: model || null };
    if (posInt(d.contextLength, 100_000_000)) e.contextLength = d.contextLength;
    const q = capped(d.quantization, MAX_QUANTIZATION_CHARS);
    if (q && QUANT_RE.test(q)) e.quantization = q;
    if (typeof d.sizeBytes === 'number' && Number.isSafeInteger(d.sizeBytes) && d.sizeBytes >= 0) e.sizeBytes = d.sizeBytes;
    out.push(e);
  }
  return out;
}

/**
 * The hello frame. `agent` and `detected` are optional (an older market drops them); the frame stays under
 * MAX_HELLO_BYTES by dropping detected entries, the ones not in the catalog first.
 */
export function buildHello(o: {
  node: { name: string; version: string };
  wallet?: string;
  models: ModelDecl[];
  maxConcurrent: number;
  agent?: AgentInfo;
  detected?: readonly DetectedModel[];
}): Record<string, unknown> {
  const hello: Record<string, unknown> = {
    t: 'hello',
    v: 1,
    node: o.node,
    ...(o.wallet ? { wallet: o.wallet } : {}),
    models: o.models.slice(0, MAX_DECLARED_MODELS),
    capacity: { maxConcurrent: o.maxConcurrent },
  };
  if (o.agent) {
    hello.agent = {
      name: capped(o.agent.name, MAX_AGENT_FIELD_CHARS) || 'inferit-seller',
      version: capped(o.agent.version, MAX_AGENT_FIELD_CHARS) || '0',
      os: capped(o.agent.os, MAX_AGENT_FIELD_CHARS),
      arch: capped(o.agent.arch, MAX_AGENT_FIELD_CHARS),
    };
  }
  if (o.detected) {
    let detected = sanitizeDetected(o.detected);
    const size = () => Buffer.byteLength(JSON.stringify({ ...hello, detected }));
    while (detected.length > 0 && size() > MAX_HELLO_BYTES) {
      const i = detected.map((d) => d.model === null).lastIndexOf(true);
      detected = detected.filter((_, j) => j !== (i >= 0 ? i : detected.length - 1));
    }
    hello.detected = detected;
  }
  return hello;
}

/** A DATA frame: 0x01, the 16-byte request id, the payload. */
export function encodeData(idHex: string, payload: Buffer): Buffer {
  const out = Buffer.allocUnsafe(17 + payload.length);
  out[0] = FRAME_DATA;
  Buffer.from(idHex, 'hex').copy(out, 1);
  payload.copy(out, 17);
  return out;
}

type ConnState = 'connecting' | 'hello' | 'live' | 'draining' | 'closed';

interface ReqFrame {
  t: 'req';
  id: string;
  path: string;
  body: Record<string, unknown>;
  credit: number;
}

function isReqFrame(f: Record<string, unknown>): f is Record<string, unknown> & ReqFrame {
  return (
    typeof f.id === 'string' &&
    /^[0-9a-f]{32}$/.test(f.id) &&
    f.path === '/v1/chat/completions' &&
    f.body !== null &&
    typeof f.body === 'object' &&
    !Array.isArray(f.body) &&
    typeof f.credit === 'number' &&
    Number.isSafeInteger(f.credit) &&
    f.credit >= 0
  );
}

class Conn {
  state: ConnState = 'connecting';
  sessionId: string | null = null;
  nodeId: string | null = null;
  readonly inflight = new Map<string, LocalRequest>();
  heartbeat: unknown = null;
  deadline: unknown = null;
  lastSeen: number;
  httpStatus: number | null = null;
  error: { message: string; code: string | null } | null = null;
  refusal: { code: string | null; message: string | null } | null = null;
  /** Set once the market welcomed this session. */
  wasLive = false;
  /** When the market welcomed it (monotonic ms). */
  liveAt: number | null = null;
  /** Drained by the market, unasked, soon after it went live: another connection with the same token took over. */
  takenOver = false;
  /** This session's own agent for the requests it relays to the local server: nothing outlives the session. */
  readonly agent = new http.Agent({ keepAlive: true, maxSockets: 64 });
  constructor(
    readonly ws: WebSocket,
    now: number,
  ) {
    this.lastSeen = now;
  }

  sendJson(obj: object): void {
    if (this.ws.readyState !== WebSocket.OPEN) return;
    this.ws.send(JSON.stringify(obj));
  }
}

/** One request relayed from the socket to the local server and back. */
class LocalRequest {
  private credit: number;
  private readonly queue: Buffer[] = [];
  private headSent = false;
  private upstreamDone = false;
  private finished = false;
  private res: http.IncomingMessage | null = null;
  private req: http.ClientRequest | null = null;

  constructor(
    private readonly conn: Conn,
    readonly id: string,
    credit: number,
    private readonly onDone: (r: LocalRequest) => void,
  ) {
    this.credit = credit;
  }

  start(local: { baseUrl: string; token?: string }, body: Record<string, unknown>, agent: http.Agent): void {
    const payload = Buffer.from(JSON.stringify(body));
    const u = new URL('/v1/chat/completions', local.baseUrl);
    const headers: Record<string, string> = {
      'content-type': 'application/json',
      accept: body.stream === true ? 'text/event-stream' : 'application/json',
      'content-length': String(payload.length),
    };
    if (local.token) headers.authorization = `Bearer ${local.token}`;
    const req = http.request({ hostname: u.hostname, port: u.port, path: u.pathname, method: 'POST', headers, agent });
    this.req = req;
    req.on('response', (res) => this.onResponse(res));
    req.on('error', () => this.fail('upstream_unreachable', 'the node could not reach its local server'));
    req.end(payload);
  }

  private onResponse(res: http.IncomingMessage): void {
    if (this.finished) {
      res.destroy();
      return;
    }
    this.res = res;
    const status = res.statusCode !== undefined && res.statusCode >= 200 && res.statusCode <= 599 ? res.statusCode : 502;
    const contentType = String(res.headers['content-type'] ?? 'application/octet-stream').slice(0, 200);
    const fingerprint = fingerprintOf(Object.entries(res.headers));
    this.conn.sendJson({ t: 'head', id: this.id, status, contentType, ...(Object.keys(fingerprint).length > 0 ? { fingerprint } : {}) });
    this.headSent = true;
    res.on('data', (chunk: Buffer) => {
      if (this.finished) return;
      this.queue.push(chunk);
      this.flush();
    });
    res.on('end', () => {
      this.upstreamDone = true;
      this.flush();
    });
    res.on('error', () => this.fail('internal', 'the local response failed'));
    res.on('close', () => {
      if (!res.complete) this.fail('internal', 'the local response was cut off');
    });
  }

  /** Sends queued bytes up to the granted credit; pauses the local response while credit is exhausted. */
  private flush(): void {
    if (this.finished) return;
    while (this.queue.length > 0 && this.credit > 0) {
      const c = this.queue[0]!;
      const n = Math.min(c.length, this.credit, MAX_DATA_PAYLOAD_BYTES);
      if (this.conn.ws.readyState !== WebSocket.OPEN) return this.abort();
      this.conn.ws.send(encodeData(this.id, c.subarray(0, n)));
      this.credit -= n;
      if (n === c.length) this.queue.shift();
      else this.queue[0] = c.subarray(n);
    }
    if (this.queue.length > 0) this.res?.pause();
    else this.res?.resume();
    if (this.upstreamDone && this.queue.length === 0) {
      this.conn.sendJson({ t: 'end', id: this.id });
      this.done();
    }
  }

  addCredit(bytes: number): void {
    this.credit += bytes;
    this.flush();
  }

  private fail(code: 'upstream_unreachable' | 'internal', message: string): void {
    if (this.finished) return;
    this.conn.sendJson({ t: 'error', id: this.id, code: this.headSent ? 'internal' : code, message });
    this.abort();
  }

  /** Stops the local request (cancel from the market, the socket closed, or a failure). */
  abort(): void {
    if (this.finished) return;
    this.res?.destroy();
    this.req?.destroy();
    this.done();
  }

  private done(): void {
    if (this.finished) return;
    this.finished = true;
    this.onDone(this);
  }
}

export interface ConnectorStatus {
  state: 'live' | 'connecting' | 'waiting' | 'stopped';
  nodeId: string | null;
  sessionId: string | null;
  inFlight: number;
  /** Consecutive failed attempts (the backoff rung). */
  attempt: number;
  /** Failed attempts since the last live session. */
  failedAttempts: number;
  lastFailure: AttemptFailure | null;
  /** Offers on the market that point at this node, as of the last welcome or `offers` frame. */
  offers: number | null;
  /** Those offers, with their context limits; null before the first welcome. */
  offerList: NodeOffer[] | null;
}

/**
 * The connect-out client. Events: 'live' (welcome: {nodeId, sessionId, offers, offerList}), 'offers' (the market's
 * offers for this node changed during the session: {offers, offerList}), 'closed' ({code, httpStatus}), 'failed' (an
 * attempt that never went live: AttemptFailure & {attempt, delayMs}), 'scheduled' ({delayMs, reason}), 'drain',
 * 'watchdog' (WatchdogInfo).
 */
export class SellerConnector extends EventEmitter {
  private readonly timers: Timers;
  private readonly random: () => number;
  private readonly log: ConnectorLog;
  private readonly conns = new Set<Conn>();
  private live: Conn | null = null;
  private connecting: Conn | null = null;
  private retryTimer: unknown = null;
  private stableTimer: unknown = null;
  private recycleTimer: unknown = null;
  private watchdogTimer: unknown = null;
  private attempt = 0;
  private failedAttempts = 0;
  private lastFailure: AttemptFailure | null = null;
  private lastLiveAt = 0;
  private offers: number | null = null;
  private offerList: NodeOffer[] | null = null;
  private stopping = false;
  private started = false;

  constructor(private readonly opts: ConnectorOptions) {
    super();
    if (!NODE_TOKEN_RE.test(opts.token)) throw new Error('connector: the node token must be icn_ followed by 43 base64url characters');
    if (opts.models.length === 0) throw new Error('connector: declare at least one model (MODEL_MAP)');
    this.timers = opts.timers ?? realTimers;
    this.random = opts.random ?? Math.random;
    this.log = opts.log ?? silent;
  }

  start(): void {
    if (this.started) return;
    this.started = true;
    this.lastLiveAt = this.timers.now();
    const wd = this.opts.watchdog;
    if (wd) this.watchdogTimer = this.timers.setInterval(() => this.checkWatchdog(wd), wd.checkEveryMs ?? WATCHDOG_CHECK_MS);
    this.connect();
  }

  status(): ConnectorStatus {
    let inFlight = 0;
    for (const c of this.conns) inFlight += c.inflight.size;
    return {
      state: this.stopping ? 'stopped' : this.live ? 'live' : this.connecting ? 'connecting' : 'waiting',
      nodeId: this.live?.nodeId ?? null,
      sessionId: this.live?.sessionId ?? null,
      inFlight,
      attempt: this.attempt,
      failedAttempts: this.failedAttempts,
      lastFailure: this.lastFailure,
      offers: this.offers,
      offerList: this.offerList,
    };
  }

  /**
   * New `detected` facts for the hello (e.g. the served context of a model that was not loaded at start). While a
   * session is live, a new one is opened with the new hello and the old one drains (make before break), as the planned
   * reconnect does; otherwise the next attempt carries it.
   */
  updateDetected(detected: DetectedModel[]): void {
    this.opts.detected = detected;
    if (this.started && !this.stopping && this.live && !this.connecting) {
      this.log.info({ sessionId: this.live.sessionId }, 'what the local server reports changed: saying hello again');
      this.connect();
    }
  }

  private checkWatchdog(wd: WatchdogOptions): void {
    if (this.stopping) return;
    const now = this.timers.now();
    if (this.live) {
      this.lastLiveAt = now;
      return;
    }
    const noSessionMs = now - this.lastLiveAt;
    if (noSessionMs < (wd.noSessionMs ?? WATCHDOG_NO_SESSION_MS)) return;
    if (wd.repeat) this.lastLiveAt = now;
    else {
      if (this.watchdogTimer !== null) this.timers.clearInterval(this.watchdogTimer);
      this.watchdogTimer = null;
    }
    const info: WatchdogInfo = { noSessionMs, failedAttempts: this.failedAttempts, lastFailure: this.lastFailure };
    this.log.error(
      { noSessionMinutes: Math.round(noSessionMs / 60_000), failedAttempts: info.failedAttempts, lastFailure: info.lastFailure },
      'watchdog: no market session for too long',
    );
    this.emit('watchdog', info);
    wd.onTrip(info);
  }

  /** Tells the market which models are served now and whether the node is ready (a `status` frame). */
  sendStatus(ready: boolean, models: ModelDecl[] = this.opts.models): void {
    const c = this.live;
    if (!c) return;
    c.sendJson({ t: 'status', models, inFlight: c.inflight.size, ready });
  }

  /** Closes every session (1000) and stops reconnecting. In-flight requests are aborted. */
  async stop(): Promise<void> {
    this.stopping = true;
    for (const t of [this.retryTimer, this.stableTimer, this.recycleTimer]) if (t !== null) this.timers.clearTimeout(t);
    this.retryTimer = this.stableTimer = this.recycleTimer = null;
    if (this.watchdogTimer !== null) this.timers.clearInterval(this.watchdogTimer);
    this.watchdogTimer = null;
    const closingConns = [...this.conns];
    const closing = closingConns.map(
      (c) =>
        new Promise<void>((resolve) => {
          if (c.ws.readyState === WebSocket.CLOSED) return resolve();
          c.ws.once('close', () => resolve());
          try {
            if (c.ws.readyState === WebSocket.CONNECTING) c.ws.terminate();
            else c.ws.close(1000, 'node stopping');
          } catch {
            resolve();
          }
          setTimeout(() => {
            c.ws.terminate();
            resolve();
          }, 2_000).unref();
        }),
    );
    await Promise.all(closing);
    for (const c of closingConns) c.agent.destroy();
  }

  private connect(): void {
    if (this.stopping || this.connecting) return;
    // A fresh WebSocket per attempt, with no agent: ws opens a new TCP (and TLS) connection, so nothing from an
    // earlier attempt (a dead socket, a stale session) can carry over.
    const ws = new WebSocket(this.opts.url, [CONNECTOR_SUBPROTOCOL], {
      headers: { authorization: `Bearer ${this.opts.token}` },
      maxPayload: this.opts.maxReqFrameBytes ?? MAX_REQ_FRAME_BYTES,
      perMessageDeflate: false,
      handshakeTimeout: 15_000,
      followRedirects: false,
      ...(this.opts.wsOptions ?? {}),
    });
    const c = new Conn(ws, this.timers.now());
    this.conns.add(c);
    this.connecting = c;
    c.deadline = this.timers.setTimeout(() => {
      c.deadline = null;
      if (c.wasLive || c.state === 'closed') return;
      c.error ??= { message: `no welcome from the market within ${Math.round((this.opts.attemptDeadlineMs ?? ATTEMPT_DEADLINE_MS) / 1000)} s`, code: 'ATTEMPT_DEADLINE' };
      c.ws.terminate();
    }, this.opts.attemptDeadlineMs ?? ATTEMPT_DEADLINE_MS);
    ws.on('open', () => this.onOpen(c));
    ws.on('message', (data, isBinary) => this.onMessage(c, data, isBinary));
    ws.on('pong', () => {
      c.lastSeen = this.timers.now();
    });
    ws.on('ping', () => {
      c.lastSeen = this.timers.now();
    });
    ws.on('unexpected-response', (_req, res) => {
      // A refused upgrade: keep the status and the market's reason ({error: {code, message}}), then drop the attempt.
      c.httpStatus = res.statusCode ?? null;
      const chunks: Buffer[] = [];
      let size = 0;
      let done = false;
      const finish = () => {
        if (done) return;
        done = true;
        try {
          const body = JSON.parse(Buffer.concat(chunks).toString('utf8')) as { error?: { code?: unknown; message?: unknown } };
          const code = typeof body?.error?.code === 'string' ? body.error.code.slice(0, 64) : null;
          const message = typeof body?.error?.message === 'string' ? body.error.message.slice(0, 300) : null;
          if (code || message) c.refusal = { code, message };
        } catch {
          /* not JSON: the status says enough */
        }
        ws.terminate();
      };
      res.on('data', (chunk: Buffer) => {
        if (size < 4096) chunks.push(chunk);
        size += chunk.length;
      });
      res.on('end', finish);
      res.on('error', finish);
      res.on('close', finish);
      setTimeout(finish, 3_000).unref();
    });
    ws.on('error', (err: Error & { code?: unknown }) => {
      const m = /Unexpected server response: (\d{3})/.exec(err.message);
      if (m) c.httpStatus = Number(m[1]);
      // The first error is the cause; "closed before the connection was established" follows our own terminate()
      // (after a refused upgrade, whose status and reason are already kept).
      if (/closed before the connection was established/.test(err.message) && (c.httpStatus !== null || c.error)) return;
      if (!c.error || c.error.code === null) c.error = { message: err.message.slice(0, 300), code: typeof err.code === 'string' ? err.code : null };
    });
    ws.on('close', (code) => this.onClose(c, code));
  }

  private onOpen(c: Conn): void {
    c.state = 'hello';
    c.lastSeen = this.timers.now();
    c.sendJson(
      buildHello({
        node: this.opts.node,
        ...(this.opts.wallet ? { wallet: this.opts.wallet } : {}),
        models: this.opts.models,
        maxConcurrent: this.opts.maxConcurrent,
        ...(this.opts.agent ? { agent: this.opts.agent } : {}),
        ...(this.opts.detected ? { detected: this.opts.detected } : {}),
      }),
    );
    const hb = this.opts.heartbeatMs ?? HEARTBEAT_MS;
    let lastTick = { mono: this.timers.now(), wall: this.timers.wallNow?.() };
    c.heartbeat = this.timers.setInterval(() => {
      if (c.state === 'closed') return;
      const tick = { mono: this.timers.now(), wall: this.timers.wallNow?.() };
      const slept = tick.wall !== undefined && lastTick.wall !== undefined ? tick.wall - lastTick.wall - (tick.mono - lastTick.mono) : 0;
      lastTick = tick;
      if (slept > SLEEP_GAP_MS) {
        // The monotonic clock stops while a Mac sleeps; the market dropped this session long ago. Redial now.
        this.log.info({ sessionId: c.sessionId, asleepSeconds: Math.round(slept / 1000) }, 'the computer was asleep; reconnecting');
        c.error ??= { message: 'the computer was asleep', code: 'SLEPT' };
        c.ws.terminate();
        return;
      }
      if (this.timers.now() - c.lastSeen > 2 * hb) {
        this.log.warn({ sessionId: c.sessionId }, 'connector heartbeat timeout; reconnecting');
        c.ws.terminate();
        return;
      }
      try {
        c.ws.ping();
      } catch {
        /* the close event follows */
      }
    }, hb);
  }

  private onMessage(c: Conn, data: RawData, isBinary: boolean): void {
    c.lastSeen = this.timers.now();
    if (isBinary) return; // the market sends JSON only
    let f: Record<string, unknown>;
    try {
      const parsed = JSON.parse((Array.isArray(data) ? Buffer.concat(data) : Buffer.from(data as Buffer)).toString('utf8')) as unknown;
      if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) return;
      f = parsed as Record<string, unknown>;
    } catch {
      return;
    }
    switch (f.t) {
      case 'welcome':
        return this.onWelcome(c, f);
      case 'req':
        return this.onReq(c, f);
      case 'cancel': {
        if (typeof f.id === 'string') c.inflight.get(f.id)?.abort();
        return;
      }
      case 'credit': {
        if (typeof f.id === 'string' && typeof f.bytes === 'number' && Number.isSafeInteger(f.bytes) && f.bytes > 0) c.inflight.get(f.id)?.addCredit(f.bytes);
        return;
      }
      case 'drain':
        return this.onDrain(c);
      case 'offers':
        return this.onOffers(c, f);
    }
  }

  private onOffers(c: Conn, f: Record<string, unknown>): void {
    if (this.live !== c || !Array.isArray(f.offers)) return;
    this.offerList = parseNodeOffers(f.offers);
    this.offers = this.offerList.length;
    this.log.info({ offers: this.offers }, 'the offers on the market for this node changed');
    this.emit('offers', { offers: this.offers, offerList: this.offerList });
  }

  private onWelcome(c: Conn, f: Record<string, unknown>): void {
    if (c.state !== 'hello' || typeof f.nodeId !== 'string' || typeof f.sessionId !== 'string') return;
    c.state = 'live';
    c.wasLive = true;
    c.liveAt = this.timers.now();
    c.nodeId = f.nodeId;
    c.sessionId = f.sessionId;
    if (c.deadline !== null) this.timers.clearTimeout(c.deadline);
    c.deadline = null;
    this.lastLiveAt = this.timers.now();
    this.failedAttempts = 0;
    if (this.connecting === c) this.connecting = null;
    const prev = this.live;
    // Make before break: the market drains the previous session (4409 once its requests are done).
    if (prev && prev !== c && prev.state === 'live') prev.state = 'draining';
    this.live = c;
    if (this.stableTimer !== null) this.timers.clearTimeout(this.stableTimer);
    this.stableTimer = this.timers.setTimeout(() => {
      this.stableTimer = null;
      if (this.live === c) this.attempt = 0;
    }, this.opts.stableMs ?? STABLE_AFTER_MS);
    const recycle = this.opts.recycleMs ?? RECYCLE_EVERY_MS;
    if (this.recycleTimer !== null) this.timers.clearTimeout(this.recycleTimer);
    this.recycleTimer = null;
    if (recycle > 0) {
      this.recycleTimer = this.timers.setTimeout(() => {
        this.recycleTimer = null;
        if (this.live === c && !this.stopping) {
          this.log.info({ sessionId: c.sessionId }, 'connector planned reconnect');
          this.connect();
        }
      }, recycle);
    }
    this.offerList = parseNodeOffers(f.offers);
    const offers = this.offerList.length;
    this.offers = offers;
    this.log.info({ nodeId: c.nodeId, sessionId: c.sessionId, offers }, 'connected to the market');
    this.emit('live', { nodeId: c.nodeId, sessionId: c.sessionId, offers, offerList: this.offerList });
  }

  private onDrain(c: Conn): void {
    if (c.state === 'closed') return;
    // Not our own planned reconnect (that one is already connecting, or live), and the session is young: someone else
    // said hello with this token (or the API is restarting; its 4503 close still reconnects at once, in onClose).
    const unasked = this.live === c && !this.connecting;
    const young = c.liveAt !== null && this.timers.now() - c.liveAt < (this.opts.stableMs ?? STABLE_AFTER_MS);
    c.state = 'draining';
    if (this.live === c) this.live = null;
    this.emit('drain');
    if (unasked && young) c.takenOver = true;
    // Make before break: a new session, unless one already replaced this one.
    else if (!this.live) this.connect();
    if (c.inflight.size === 0) c.ws.close(1000, 'drained');
  }

  private onReq(c: Conn, f: Record<string, unknown>): void {
    if (!isReqFrame(f)) return;
    if (c.inflight.has(f.id)) return;
    if (c.state !== 'live' && c.state !== 'draining') {
      c.sendJson({ t: 'error', id: f.id, code: 'internal', message: 'session not ready' });
      return;
    }
    if (c.inflight.size >= this.opts.maxConcurrent) {
      c.sendJson({ t: 'error', id: f.id, code: 'busy', message: 'node at capacity' });
      return;
    }
    const r = new LocalRequest(c, f.id, f.credit, (done) => {
      c.inflight.delete(done.id);
      if (c.state === 'draining' && c.inflight.size === 0 && c.ws.readyState === WebSocket.OPEN) c.ws.close(1000, 'drained');
    });
    c.inflight.set(f.id, r);
    r.start(this.opts.local, f.body, c.agent);
  }

  private onClose(c: Conn, code: number): void {
    const wasLive = this.live === c;
    c.state = 'closed';
    if (c.heartbeat !== null) this.timers.clearInterval(c.heartbeat);
    if (c.deadline !== null) this.timers.clearTimeout(c.deadline);
    c.heartbeat = c.deadline = null;
    for (const r of [...c.inflight.values()]) r.abort();
    c.agent.destroy();
    c.ws.removeAllListeners('message');
    this.conns.delete(c);
    if (this.live === c) this.live = null;
    if (this.connecting === c) this.connecting = null;
    if (c.wasLive) this.lastLiveAt = this.timers.now();
    this.emit('closed', { code, httpStatus: c.httpStatus });
    if (this.stopping) return;
    const authRefused = code === 4401 || code === 4403 || c.httpStatus === 401 || c.httpStatus === 403;
    const failure: AttemptFailure = {
      error: c.error?.message ?? null,
      code: c.error?.code ?? null,
      httpStatus: c.httpStatus,
      refusal: c.refusal,
      closeCode: code,
    };
    if (!c.wasLive) {
      this.failedAttempts++;
      this.lastFailure = failure;
    }
    if (authRefused) {
      this.log.error(
        { closeCode: code, httpStatus: c.httpStatus, refusal: c.refusal },
        `the market refused this node token${c.refusal?.message ? ` (${c.refusal.message})` : ''}; retrying in 5 minutes. If it was revoked, add the machine again in the Sell workspace and run the new connect command.`,
      );
    } else if (wasLive) {
      this.log.warn({ closeCode: code, sessionId: c.sessionId }, 'connector session ended; reconnecting');
    }
    // Another session (live or being opened) covers the node: nothing to schedule.
    if (this.live || this.connecting) {
      if (!c.wasLive && !authRefused) this.log.warn({ attempt: this.failedAttempts, ...failure }, 'connector attempt failed; another session covers the node');
      return;
    }
    let delay: number;
    let reason: string;
    if (authRefused) {
      delay = this.opts.authRetryMs ?? AUTH_RETRY_MS;
      reason = 'auth';
    } else if (code === 4503 || code === 1012 || (code === 4409 && !c.takenOver)) {
      delay = reconnectDelayMs(0, this.random); // the market is restarting or moved us: come back quickly
      reason = 'draining';
    } else if (c.takenOver) {
      delay = reconnectDelayMs(this.attempt, this.random);
      this.attempt++;
      reason = 'replaced';
      this.log.warn(
        { sessionId: c.sessionId, retryInMs: delay },
        'another connection with this node token took over the session soon after it started. If this token also runs on another machine or in Docker, stop one copy: each machine needs its own token (Add a machine in the Sell workspace).',
      );
    } else {
      delay = reconnectDelayMs(this.attempt, this.random);
      this.attempt++;
      reason = 'backoff';
    }
    if (!c.wasLive) {
      if (!authRefused) this.log.warn({ attempt: this.failedAttempts, ...failure, retryInMs: delay }, `connector attempt failed: ${describeFailure(failure)}`);
      this.emit('failed', { ...failure, attempt: this.failedAttempts, delayMs: delay });
    }
    this.schedule(delay, reason);
  }

  private schedule(delayMs: number, reason: string): void {
    if (this.retryTimer !== null) this.timers.clearTimeout(this.retryTimer);
    this.emit('scheduled', { delayMs, reason });
    this.retryTimer = this.timers.setTimeout(() => {
      this.retryTimer = null;
      this.connect();
    }, delayMs);
  }
}

/** One line for a failed attempt, e.g. "HTTP 403 seller_not_registered: Register as a seller…" or "ECONNREFUSED". */
export function describeFailure(f: AttemptFailure): string {
  if (f.httpStatus !== null) {
    const r = f.refusal ? ` ${[f.refusal.code, f.refusal.message].filter(Boolean).join(': ')}` : '';
    return `HTTP ${f.httpStatus}${r}`;
  }
  if (f.error) return f.code && !f.error.includes(f.code) ? `${f.code} ${f.error}` : f.error;
  return `connection closed (code ${f.closeCode})`;
}
