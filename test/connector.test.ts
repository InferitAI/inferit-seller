// The connect-out client (src/connector.ts) against a fake market hub, with injected timers: the hello, relaying a
// request through the node's own server, credit, cancel, busy, reconnect backoff, the 5-minute wait after a refused
// token, make-before-break on drain and on the planned recycle, heartbeats and stop. Plus the connector config.
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import type { Duplex } from 'node:stream';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import WebSocket, { WebSocketServer } from 'ws';
import { createServer as createTcpServer, type Server as TcpServer } from 'node:net';
import {
  BACKOFF_STEPS_MS,
  MAX_BACKOFF_MS,
  SellerConnector,
  buildHello,
  connectorOptionsFromEnv,
  createSellerNode,
  describeFailure,
  parseModelMeta,
  reconnectDelayMs,
  sellerNodeOptionsFromEnv,
  type AttemptFailure,
  type DetectedModel,
  type SellerNode,
  type Timers,
  type WatchdogInfo,
} from '../src/index.ts';

const TOKEN = `icn_${'T'.repeat(43)}`;
const LOCAL_TOKEN = 'local-seller-token';
const MODEL = 'qwen/qwen-2.5-14b-instruct';

/** Timers driven by the test: nothing fires until advance(). */
class ManualTimers implements Timers {
  t = 0;
  private seq = 0;
  readonly tasks = new Map<number, { at: number; fn: () => void; every: number | null }>();
  setTimeout(fn: () => void, ms: number): unknown {
    const id = ++this.seq;
    this.tasks.set(id, { at: this.t + ms, fn, every: null });
    return id;
  }
  clearTimeout(h: unknown): void {
    this.tasks.delete(h as number);
  }
  setInterval(fn: () => void, ms: number): unknown {
    const id = ++this.seq;
    this.tasks.set(id, { at: this.t + ms, fn, every: ms });
    return id;
  }
  clearInterval(h: unknown): void {
    this.tasks.delete(h as number);
  }
  now(): number {
    return this.t;
  }
  /** One-shot delays scheduled now (relative). */
  timeouts(): number[] {
    return [...this.tasks.values()].filter((x) => x.every === null).map((x) => x.at - this.t);
  }
  advance(ms: number): void {
    const end = this.t + ms;
    for (;;) {
      let next: [number, { at: number; fn: () => void; every: number | null }] | null = null;
      for (const e of this.tasks) if (e[1].at <= end && (!next || e[1].at < next[1].at)) next = e;
      if (!next) break;
      const [id, task] = next;
      this.t = task.at;
      if (task.every !== null) task.at += task.every;
      else this.tasks.delete(id);
      task.fn();
    }
    this.t = end;
  }
}

interface HubConn {
  ws: WebSocket;
  auth: string | undefined;
  url: string;
  protocol: string;
  frames: Record<string, unknown>[];
  data: Buffer[];
  closed: Promise<number>;
}

/** A fake market: answers hellos with a welcome (unless told not to) and records everything. */
class FakeHub {
  readonly conns: HubConn[] = [];
  /** Every upgrade request, refused or not, by the client's source port (a fresh connection has a fresh port). */
  readonly upgrades: number[] = [];
  refuseWith: number | null = null;
  /** A JSON body for the refusal, like the API's ({error: {code, message}}). */
  refuseBody: string | null = null;
  autoWelcome = true;
  private server!: Server;
  private wss!: WebSocketServer;
  url = '';

  async start(opts: { autoPong?: boolean } = {}): Promise<void> {
    this.wss = new WebSocketServer({ noServer: true, autoPong: opts.autoPong ?? true, handleProtocols: (p) => (p.has('inferit.connector.v1') ? 'inferit.connector.v1' : false) });
    this.server = createServer((_req, res) => res.writeHead(404).end());
    this.server.on('upgrade', (req: IncomingMessage, socket: Duplex, head: Buffer) => {
      this.upgrades.push(req.socket.remotePort ?? 0);
      if (this.refuseWith !== null) {
        const body = this.refuseBody ?? '';
        socket.end(`HTTP/1.1 ${this.refuseWith} Refused\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: ${Buffer.byteLength(body)}\r\n\r\n${body}`);
        return;
      }
      this.wss.handleUpgrade(req, socket, head, (ws) => {
        const index = this.conns.length;
        let resolveClosed!: (c: number) => void;
        const c: HubConn = { ws, auth: req.headers.authorization, url: req.url ?? '', protocol: ws.protocol, frames: [], data: [], closed: new Promise((r) => (resolveClosed = r)) };
        ws.on('close', (code) => resolveClosed(code));
        ws.on('message', (d, isBinary) => {
          if (isBinary) {
            c.data.push(Buffer.from(d as Buffer));
            return;
          }
          const f = JSON.parse(d.toString()) as Record<string, unknown>;
          c.frames.push(f);
          if (f.t === 'hello' && this.autoWelcome) {
            ws.send(JSON.stringify({ t: 'welcome', v: 1, nodeId: 'node_test', sessionId: `cs_${index}`, heartbeatMs: 20_000, maxConcurrent: 4, offers: [] }));
          }
        });
        this.conns.push(c);
      });
    });
    await new Promise<void>((r) => this.server.listen(0, '127.0.0.1', r));
    this.url = `ws://127.0.0.1:${(this.server.address() as AddressInfo).port}/v1/seller/connect`;
  }

  send(c: HubConn, obj: unknown): void {
    c.ws.send(JSON.stringify(obj));
  }

  async close(): Promise<void> {
    for (const c of this.conns) c.ws.terminate();
    await new Promise<void>((r) => this.server.close(() => r()));
  }
}

async function waitFor<T>(get: () => T | undefined | false, what: string, timeoutMs = 5_000): Promise<T> {
  const started = Date.now();
  for (;;) {
    const v = get();
    if (v !== undefined && v !== false) return v;
    if (Date.now() - started > timeoutMs) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 5));
  }
}

const frame = (c: HubConn, t: string) => c.frames.find((f) => f.t === t);
const id = (n: number) => n.toString(16).padStart(32, '0');

describe('reconnectDelayMs', () => {
  it('walks 1, 2, 4, 8, 16, 30, 60 s with ±20% jitter', () => {
    expect(BACKOFF_STEPS_MS).toEqual([1_000, 2_000, 4_000, 8_000, 16_000, 30_000, 60_000]);
    expect([0, 1, 2, 3, 4, 5, 6, 7, 50].map((a) => reconnectDelayMs(a, () => 0.5))).toEqual([1_000, 2_000, 4_000, 8_000, 16_000, 30_000, 60_000, 60_000, 60_000]);
    expect(reconnectDelayMs(0, () => 0)).toBe(800);
    expect(reconnectDelayMs(0, () => 1)).toBe(1_200);
    expect(reconnectDelayMs(-3, () => 0.5)).toBe(1_000);
    for (let i = 0; i < 100; i++) {
      const d = reconnectDelayMs(6);
      expect(d).toBeGreaterThanOrEqual(48_000);
      expect(d).toBeLessThanOrEqual(72_000);
    }
  });
});

describe('connector config', () => {
  const base = { MOCK: '1', MODEL_MAP: `${MODEL}=qwen2.5:14b`, CONNECT_URL: 'wss://api.example/v1/seller/connect', CONNECT_TOKEN: TOKEN };
  const conn = (env: NodeJS.ProcessEnv) => connectorOptionsFromEnv(env, sellerNodeOptionsFromEnv(env));

  it('is off without CONNECT_URL', () => {
    expect(conn({ MOCK: '1' })).toBeNull();
    expect(() => conn({ MOCK: '1', CONNECT_TOKEN: TOKEN })).toThrow(/CONNECT_URL is not/);
  });

  it('reads the node, the models and their context and quantisation', () => {
    expect(
      conn({ ...base, NODE_NAME: 'vast-3060-1', CONNECT_MAX_CONCURRENCY: '8', CONNECT_WALLET: `0x${'ab'.repeat(20)}`, MODEL_META: JSON.stringify({ [MODEL]: { maxContextTokens: 32768, quantization: 'w4a16' } }) }),
    ).toEqual({
      url: base.CONNECT_URL,
      token: TOKEN,
      nodeName: 'vast-3060-1',
      maxConcurrent: 8,
      wallet: `0x${'ab'.repeat(20)}`,
      models: [{ id: MODEL, maxContextTokens: 32768, quantization: 'w4a16' }],
    });
    expect(conn(base)).toMatchObject({ nodeName: 'seller-node', maxConcurrent: 4, models: [{ id: MODEL }] });
    expect(conn({ ...base, CONNECT_URL: 'ws://127.0.0.1:8787/v1/seller/connect' })).not.toBeNull(); // a local market
  });

  it('refuses plain ws to a remote market, URL credentials, a bad token, a public HOST, no MODEL_MAP and bad metadata', () => {
    const bad: [NodeJS.ProcessEnv, RegExp][] = [
      [{ ...base, CONNECT_URL: 'ws://api.example/v1/seller/connect' }, /wss/],
      [{ ...base, CONNECT_URL: 'https://api.example/v1/seller/connect' }, /wss/],
      [{ ...base, CONNECT_URL: `wss://api.example/v1/seller/connect?token=${TOKEN}` }, /query/],
      [{ ...base, CONNECT_URL: 'wss://u:p@api.example/v1/seller/connect' }, /credentials/],
      [{ ...base, CONNECT_TOKEN: 'ik_seller_abc' }, /icn_/],
      [{ ...base, HOST: '0.0.0.0' }, /HOST must be 127\.0\.0\.1/],
      [{ ...base, MODEL_MAP: '' }, /MODEL_MAP is required/],
      [{ ...base, MODEL_META: '{"other/model":{}}' }, /not in MODEL_MAP/],
      [{ ...base, NODE_NAME: 'x'.repeat(65) }, /NODE_NAME/],
      [{ ...base, CONNECT_WALLET: '0x12' }, /CONNECT_WALLET/],
      [{ ...base, CONNECT_MAX_CONCURRENCY: '0' }, /CONNECT_MAX_CONCURRENCY/],
    ];
    for (const [env, re] of bad) expect(() => conn(env), JSON.stringify(env)).toThrow(re);
  });

  it('parses MODEL_META strictly', () => {
    expect(parseModelMeta(undefined)).toEqual({});
    expect(() => parseModelMeta('nope')).toThrow(/invalid JSON/);
    expect(() => parseModelMeta('[]')).toThrow(/JSON object/);
    expect(() => parseModelMeta('{"m":1}')).toThrow(/bad entry/);
    expect(() => parseModelMeta('{"m":{"maxContextTokens":0}}')).toThrow(/positive integer/);
    expect(() => parseModelMeta('{"m":{"quantization":"4 bit!"}}')).toThrow(/short name/);
  });

  it('refuses an UPSTREAM_BASE_URL on a hosted provider API', () => {
    expect(() => sellerNodeOptionsFromEnv({ UPSTREAM_BASE_URL: 'https://api.openai.com/v1', SELLER_TOKEN: 's' })).toThrow(/openai\.com, a hosted inference API/);
    expect(() => sellerNodeOptionsFromEnv({ UPSTREAM_BASE_URL: 'https://api.groq.com/openai/v1', SELLER_TOKEN: 's' })).toThrow(/groq\.com/);
    expect(sellerNodeOptionsFromEnv({ UPSTREAM_BASE_URL: 'http://127.0.0.1:8000/v1', SELLER_TOKEN: 's' }).upstreamBaseUrl).toBe('http://127.0.0.1:8000/v1');
  });
});

describe('proxy mode passes on provider fingerprint headers only', () => {
  it('forwards openai-organization, drops other upstream headers', async () => {
    const upstream = createServer((_req: IncomingMessage, res: ServerResponse) => {
      res.writeHead(200, { 'content-type': 'application/json', 'openai-organization': 'org-123', 'x-internal-secret': 'no' });
      res.end(JSON.stringify({ id: 'x', choices: [{ index: 0, message: { role: 'assistant', content: 'hi' } }] }));
    });
    await new Promise<void>((r) => upstream.listen(0, '127.0.0.1', r));
    const node = await createSellerNode({ upstreamBaseUrl: `http://127.0.0.1:${(upstream.address() as AddressInfo).port}/v1`, sellerToken: LOCAL_TOKEN });
    try {
      const res = await fetch(`${node.baseUrl}/chat/completions`, {
        method: 'POST',
        headers: { authorization: `Bearer ${LOCAL_TOKEN}`, 'content-type': 'application/json' },
        body: JSON.stringify({ model: MODEL, messages: [{ role: 'user', content: 'hi' }] }),
      });
      expect(res.status).toBe(200);
      expect(res.headers.get('openai-organization')).toBe('org-123');
      expect(res.headers.get('x-internal-secret')).toBeNull();
    } finally {
      await node.close();
      await new Promise<void>((r) => upstream.close(() => r()));
    }
  });
});

describe('SellerConnector', () => {
  let hub: FakeHub;
  let local: SellerNode;
  let timers: ManualTimers;
  let connector: SellerConnector | null;

  const make = (extra: Partial<ConstructorParameters<typeof SellerConnector>[0]> = {}) => {
    connector = new SellerConnector({
      url: hub.url,
      token: TOKEN,
      node: { name: 'test-node', version: '9.9.9' },
      models: [{ id: MODEL, maxContextTokens: 32768, quantization: 'q4_k_m' }],
      maxConcurrent: 2,
      local: { baseUrl: local.url, token: LOCAL_TOKEN },
      timers,
      random: () => 0.5,
      // Long by default, so advancing the manual clock does not trip the heartbeat (it has its own test).
      heartbeatMs: 3_600_000,
      ...extra,
    });
    return connector;
  };

  beforeEach(async () => {
    hub = new FakeHub();
    await hub.start();
    local = await createSellerNode({ mock: { clock: () => 1_790_000_000_000 }, sellerToken: LOCAL_TOKEN, modelMap: { [MODEL]: 'qwen2.5:14b' } });
    timers = new ManualTimers();
    connector = null;
  });
  afterEach(async () => {
    await connector?.stop();
    await hub.close();
    await local.close();
  });

  it('dials out with the token in the Authorization header and the subprotocol, and says hello', async () => {
    const c = make({ wallet: `0x${'cd'.repeat(20)}` });
    const live = new Promise((r) => c.once('live', r));
    c.start();
    await live;
    const conn = hub.conns[0]!;
    expect(conn.auth).toBe(`Bearer ${TOKEN}`);
    expect(conn.url).toBe('/v1/seller/connect'); // never the token in the URL
    expect(conn.protocol).toBe('inferit.connector.v1');
    expect(frame(conn, 'hello')).toEqual({
      t: 'hello',
      v: 1,
      node: { name: 'test-node', version: '9.9.9' },
      wallet: `0x${'cd'.repeat(20)}`,
      models: [{ id: MODEL, maxContextTokens: 32768, quantization: 'q4_k_m' }],
      capacity: { maxConcurrent: 2 },
    });
    expect(c.status()).toMatchObject({ state: 'live', nodeId: 'node_test', sessionId: 'cs_0', inFlight: 0 });
  });

  it('relays a request through its own server: head, DATA, end', async () => {
    const c = make();
    const live = new Promise((r) => c.once('live', r));
    c.start();
    await live;
    const conn = hub.conns[0]!;
    hub.send(conn, { t: 'req', id: id(1), model: MODEL, path: '/v1/chat/completions', stream: false, body: { model: MODEL, messages: [{ role: 'user', content: 'hello node' }], max_tokens: 5 }, firstByteTimeoutMs: 1000, credit: 262144 });
    await waitFor(() => frame(conn, 'end'), 'end');
    expect(frame(conn, 'head')).toMatchObject({ t: 'head', id: id(1), status: 200, contentType: expect.stringContaining('application/json') });
    expect(frame(conn, 'head')).not.toHaveProperty('fingerprint');
    const body = Buffer.concat(conn.data.map((d) => d.subarray(17)));
    expect(conn.data.every((d) => d[0] === 1 && d.subarray(1, 17).toString('hex') === id(1))).toBe(true);
    const json = JSON.parse(body.toString()) as { id: string; usage: { completion_tokens: number } };
    expect(json.id).toMatch(/^chatcmpl-mock-/);
    expect(json.usage.completion_tokens).toBe(5);

    // An unknown model answers 404 through the same path (the local server's own mapping).
    hub.send(conn, { t: 'req', id: id(2), model: 'x/y', path: '/v1/chat/completions', stream: false, body: { model: 'x/y', messages: [{ role: 'user', content: 'x' }] }, firstByteTimeoutMs: 1000, credit: 262144 });
    await waitFor(() => conn.frames.find((f) => f.t === 'head' && f.id === id(2)), 'the 404 head');
    expect(conn.frames.find((f) => f.t === 'head' && f.id === id(2))).toMatchObject({ status: 404 });
    // Malformed requests are ignored, not relayed.
    hub.send(conn, { t: 'req', id: 'nothex', path: '/v1/chat/completions', body: {}, credit: 1 });
    hub.send(conn, { t: 'req', id: id(3), path: '/admin', body: {}, credit: 1 });
    await new Promise((r) => setTimeout(r, 50));
    expect(conn.frames.filter((f) => f.id === 'nothex' || f.id === id(3))).toEqual([]);
  });

  it('never sends more than its credit; more credit resumes the body', async () => {
    const big = createServer((_req, res) => {
      res.writeHead(200, { 'content-type': 'application/octet-stream' });
      res.end(Buffer.alloc(600 * 1024, 7));
    });
    await new Promise<void>((r) => big.listen(0, '127.0.0.1', r));
    const c = make({ local: { baseUrl: `http://127.0.0.1:${(big.address() as AddressInfo).port}` } });
    try {
      const live = new Promise((r) => c.once('live', r));
      c.start();
      await live;
      const conn = hub.conns[0]!;
      hub.send(conn, { t: 'req', id: id(9), model: MODEL, path: '/v1/chat/completions', stream: true, body: { model: MODEL, messages: [] }, firstByteTimeoutMs: 1000, credit: 100_000 });
      const sent = () => conn.data.reduce((n, d) => n + d.length - 17, 0);
      await waitFor(() => sent() === 100_000, 'the first 100 000 bytes');
      await new Promise((r) => setTimeout(r, 100));
      expect(sent()).toBe(100_000);
      expect(frame(conn, 'end')).toBeUndefined();
      hub.send(conn, { t: 'credit', id: id(9), bytes: 1_000_000 });
      await waitFor(() => frame(conn, 'end'), 'end');
      expect(sent()).toBe(600 * 1024);
      expect(conn.data.every((d) => d.length - 17 <= 256 * 1024)).toBe(true);
    } finally {
      await new Promise<void>((r) => {
        big.closeAllConnections();
        big.close(() => r());
      });
    }
  });

  it('cancel aborts the local request; over capacity it answers busy', async () => {
    let aborted = false;
    let hold: ServerResponse | null = null;
    const slow = createServer((req, res) => {
      hold = res;
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      res.write('data: 1\n\n');
      req.socket.on('close', () => (aborted = true));
    });
    await new Promise<void>((r) => slow.listen(0, '127.0.0.1', r));
    const c = make({ maxConcurrent: 1, local: { baseUrl: `http://127.0.0.1:${(slow.address() as AddressInfo).port}` } });
    try {
      const live = new Promise((r) => c.once('live', r));
      c.start();
      await live;
      const conn = hub.conns[0]!;
      const req = (n: number) => hub.send(conn, { t: 'req', id: id(n), model: MODEL, path: '/v1/chat/completions', stream: true, body: { model: MODEL, messages: [] }, firstByteTimeoutMs: 1000, credit: 262144 });
      req(1);
      await waitFor(() => conn.data.length > 0, 'the first bytes');
      req(2);
      await waitFor(() => conn.frames.find((f) => f.t === 'error' && f.id === id(2)), 'busy');
      expect(conn.frames.find((f) => f.id === id(2))).toMatchObject({ t: 'error', code: 'busy' });
      hub.send(conn, { t: 'cancel', id: id(1), reason: 'client_closed' });
      await waitFor(() => aborted, 'the local request to be aborted');
      expect(c.status().inFlight).toBe(0);
      void hold;
    } finally {
      await new Promise<void>((r) => {
        slow.closeAllConnections();
        slow.close(() => r());
      });
    }
  });

  it('reconnects with backoff after a drop, and resets after a stable minute', async () => {
    const c = make();
    const scheduled: { delayMs: number; reason: string }[] = [];
    c.on('scheduled', (e: { delayMs: number; reason: string }) => scheduled.push(e));
    let live = new Promise((r) => c.once('live', r));
    c.start();
    await live;
    // Two failed attempts in a row (the hub refuses with 502), then success.
    hub.refuseWith = 502;
    hub.conns[0]!.ws.terminate();
    await waitFor(() => scheduled.length === 1, 'the first retry');
    timers.advance(1_000);
    await waitFor(() => scheduled.length === 2, 'the second retry');
    timers.advance(2_000);
    await waitFor(() => scheduled.length === 3, 'the third retry');
    expect(scheduled.map((s) => s.delayMs)).toEqual([1_000, 2_000, 4_000]);
    expect(c.status()).toMatchObject({ state: 'waiting', attempt: 3 });
    hub.refuseWith = null;
    live = new Promise((r) => c.once('live', r));
    timers.advance(4_000);
    await live;
    // Not yet stable: a drop now continues the ladder.
    expect(c.status().attempt).toBe(3);
    timers.advance(60_000);
    expect(c.status().attempt).toBe(0);
  });

  it('waits 5 minutes after a refused token (401/403 or close 4401/4403)', async () => {
    hub.refuseWith = 401;
    const c = make();
    const scheduled: { delayMs: number; reason: string }[] = [];
    c.on('scheduled', (e: { delayMs: number; reason: string }) => scheduled.push(e));
    c.start();
    await waitFor(() => scheduled.length === 1, 'the retry');
    expect(scheduled[0]).toEqual({ delayMs: 300_000, reason: 'auth' });

    hub.refuseWith = null;
    const live = new Promise((r) => c.once('live', r));
    timers.advance(300_000);
    await live;
    hub.conns.at(-1)!.ws.close(4403, 'revoked');
    await waitFor(() => scheduled.length === 2, 'the second retry');
    expect(scheduled[1]).toEqual({ delayMs: 300_000, reason: 'auth' });
  });

  it('on drain opens a new session first, then closes the old one when its work is done', async () => {
    const c = make();
    let live = new Promise((r) => c.once('live', r));
    c.start();
    await live;
    timers.advance(60_000); // an established session (a young one drained unasked is the next test)
    const old = hub.conns[0]!;
    live = new Promise((r) => c.once('live', r));
    hub.send(old, { t: 'drain' });
    await live;
    expect(hub.conns).toHaveLength(2);
    expect(await old.closed).toBe(1000);
    expect(c.status()).toMatchObject({ state: 'live', sessionId: 'cs_1' });
  });

  it('a young session taken over by another copy with the same token waits on the backoff ladder, not in a tight loop', async () => {
    const c = make();
    const scheduled: { delayMs: number; reason: string }[] = [];
    c.on('scheduled', (e: { delayMs: number; reason: string }) => scheduled.push(e));
    let live = new Promise((r) => c.once('live', r));
    c.start();
    await live;
    // Another machine says hello with this token: the market drains this session (then closes it 4409).
    hub.send(hub.conns[0]!, { t: 'drain' });
    hub.conns[0]!.ws.close(4409, 'replaced');
    await waitFor(() => scheduled.length === 1, 'the retry');
    expect(hub.conns).toHaveLength(1); // no new session at once
    expect(scheduled[0]).toEqual({ delayMs: 1_000, reason: 'replaced' });
    live = new Promise((r) => c.once('live', r));
    timers.advance(1_000);
    await live;
    hub.send(hub.conns[1]!, { t: 'drain' });
    hub.conns[1]!.ws.close(4409, 'replaced');
    await waitFor(() => scheduled.length === 2, 'the second retry');
    expect(scheduled[1]).toEqual({ delayMs: 2_000, reason: 'replaced' }); // the ladder climbs
    // The API restarting (drain, then 4503) still comes back at once.
    live = new Promise((r) => c.once('live', r));
    timers.advance(2_000);
    await live;
    hub.send(hub.conns[2]!, { t: 'drain' });
    hub.conns[2]!.ws.close(4503, 'api shutting down');
    await waitFor(() => scheduled.length === 3, 'the third retry');
    expect(scheduled[2]).toEqual({ delayMs: 1_000, reason: 'draining' });
  });

  it('recycles the session every 30 minutes, make before break', async () => {
    const c = make();
    let live = new Promise((r) => c.once('live', r));
    c.start();
    await live;
    expect(timers.timeouts()).toContain(30 * 60_000);
    live = new Promise((r) => c.once('live', r));
    timers.advance(30 * 60_000);
    await live;
    expect(hub.conns).toHaveLength(2);
    expect(hub.conns[0]!.ws.readyState).toBe(WebSocket.OPEN); // the market drains the old one, not the node
    expect(c.status().sessionId).toBe('cs_1');
  });

  it('a market that stops answering pings is dropped after two heartbeats, then redialled', async () => {
    await hub.close();
    hub = new FakeHub();
    await hub.start({ autoPong: false });
    const c = make({ heartbeatMs: 20_000 });
    const scheduled: { delayMs: number; reason: string }[] = [];
    c.on('scheduled', (e: { delayMs: number; reason: string }) => scheduled.push(e));
    const live = new Promise((r) => c.once('live', r));
    c.start();
    await live;
    timers.advance(20_000); // ping, no pong
    timers.advance(20_000); // ping, no pong
    expect(hub.conns[0]!.ws.readyState).toBe(WebSocket.OPEN);
    timers.advance(20_000); // 60 s since the last sign of life: terminate
    await hub.conns[0]!.closed;
    await waitFor(() => scheduled.length === 1, 'the redial');
    expect(scheduled[0]!.reason).toBe('backoff');
  });

  it('stop() closes the session and stops redialling', async () => {
    const c = make();
    const live = new Promise((r) => c.once('live', r));
    c.start();
    await live;
    await c.stop();
    expect(await hub.conns[0]!.closed).toBe(1000);
    timers.advance(10 * 60_000);
    await new Promise((r) => setTimeout(r, 50));
    expect(hub.conns).toHaveLength(1);
    expect(c.status().state).toBe('stopped');
    connector = null;
  });

  it('refuses a malformed token or an empty model list up front', () => {
    expect(() => make({ token: 'ik_seller_x' })).toThrow(/icn_/);
    expect(() => make({ models: [] })).toThrow(/at least one model/);
  });
});

describe('reconnect: the cap and the failure reasons', () => {
  it('never waits more than 60 s between attempts, jitter included', () => {
    expect(MAX_BACKOFF_MS).toBe(60_000);
    expect(reconnectDelayMs(6, () => 1)).toBe(60_000); // 72 000 before the cap
    expect(reconnectDelayMs(5, () => 1)).toBe(36_000);
    expect(reconnectDelayMs(99, () => 0)).toBe(48_000);
  });

  it('describes a failed attempt in one line', () => {
    const f = (p: Partial<AttemptFailure>): AttemptFailure => ({ error: null, code: null, httpStatus: null, refusal: null, closeCode: 1006, ...p });
    expect(describeFailure(f({ httpStatus: 403, refusal: { code: 'seller_not_registered', message: 'Register first.' } }))).toBe('HTTP 403 seller_not_registered: Register first.');
    expect(describeFailure(f({ httpStatus: 502 }))).toBe('HTTP 502');
    expect(describeFailure(f({ error: 'connect ECONNREFUSED 127.0.0.1:9', code: 'ECONNREFUSED' }))).toBe('connect ECONNREFUSED 127.0.0.1:9');
    expect(describeFailure(f({ error: 'getaddrinfo failed', code: 'ENOTFOUND' }))).toBe('ENOTFOUND getaddrinfo failed');
    expect(describeFailure(f({}))).toBe('connection closed (code 1006)');
  });
});

describe('the hello: agent and detected models', () => {
  const base = { node: { name: 'n', version: '0.3.0' }, models: [{ id: MODEL }], maxConcurrent: 4 };

  it('adds agent and detected only when given (an older market drops them)', () => {
    expect(buildHello(base)).toEqual({ t: 'hello', v: 1, node: base.node, models: base.models, capacity: { maxConcurrent: 4 } });
    const h = buildHello({
      ...base,
      agent: { name: 'inferit-seller', version: '0.3.0', os: 'darwin 24.1.0', arch: 'arm64' },
      detected: [{ local: 'qwen2.5:14b-instruct', server: 'ollama', model: MODEL, contextLength: 4096, quantization: 'q4_k_m', sizeBytes: 8988124069 }],
    });
    expect(h.agent).toEqual({ name: 'inferit-seller', version: '0.3.0', os: 'darwin 24.1.0', arch: 'arm64' });
    expect(h.detected).toEqual([{ local: 'qwen2.5:14b-instruct', server: 'ollama', model: MODEL, contextLength: 4096, quantization: 'q4_k_m', sizeBytes: 8988124069 }]);
  });

  it('caps the list at 200, cleans and cuts strings, drops bad fields, and stays under 60 KiB', () => {
    const many: DetectedModel[] = Array.from({ length: 250 }, (_, i) => ({ local: `m${i}`, server: 'ollama', model: null }));
    expect((buildHello({ ...base, detected: many }).detected as unknown[]).length).toBe(200);
    const odd = buildHello({
      ...base,
      agent: { name: 'x'.repeat(100), version: '1', os: 'linux\n6.1', arch: 'x64' },
      detected: [
        { local: `  ${'L'.repeat(300)}  `, server: 'weird' as never, model: '', contextLength: 0, quantization: 'q4 k m!', sizeBytes: -1 },
        { local: '   ', server: 'ollama', model: null },
      ],
    });
    expect((odd.agent as { name: string; os: string }).name).toHaveLength(64);
    expect((odd.agent as { os: string }).os).toBe('linux6.1');
    expect(odd.detected).toEqual([{ local: 'L'.repeat(200), server: 'custom', model: null }]);
    const big: DetectedModel[] = Array.from({ length: 200 }, (_, i) => ({ local: `${'x'.repeat(190)}${i}`, server: 'ollama', model: i % 2 ? `${'y'.repeat(190)}${i}` : null }));
    const h = buildHello({ ...base, detected: big });
    expect(Buffer.byteLength(JSON.stringify(h))).toBeLessThanOrEqual(60 * 1024);
    const kept = h.detected as DetectedModel[];
    expect(kept.length).toBeLessThan(200);
    expect(kept.filter((d) => d.model !== null).length).toBe(100); // the matched ones are kept, the unmatched dropped first
  });
});

describe('SellerConnector: resilience', () => {
  let hub: FakeHub;
  let local: SellerNode;
  let timers: ManualTimers;
  let connector: SellerConnector | null;
  const logs: { level: string; obj: Record<string, unknown>; msg: string }[] = [];
  const log = {
    info: (obj: object, msg: string) => logs.push({ level: 'info', obj: obj as Record<string, unknown>, msg }),
    warn: (obj: object, msg: string) => logs.push({ level: 'warn', obj: obj as Record<string, unknown>, msg }),
    error: (obj: object, msg: string) => logs.push({ level: 'error', obj: obj as Record<string, unknown>, msg }),
  };

  const make = (extra: Partial<ConstructorParameters<typeof SellerConnector>[0]> = {}) => {
    connector = new SellerConnector({
      url: hub.url,
      token: TOKEN,
      node: { name: 'test-node', version: '0.3.0' },
      models: [{ id: MODEL, quantization: 'q4_k_m' }],
      maxConcurrent: 2,
      local: { baseUrl: local.url, token: LOCAL_TOKEN },
      timers,
      random: () => 0.5,
      heartbeatMs: 3_600_000,
      recycleMs: 0,
      log,
      ...extra,
    });
    return connector;
  };
  const failures = (c: SellerConnector) => {
    const out: (AttemptFailure & { attempt: number; delayMs: number })[] = [];
    c.on('failed', (f: AttemptFailure & { attempt: number; delayMs: number }) => out.push(f));
    return out;
  };

  beforeEach(async () => {
    hub = new FakeHub();
    await hub.start();
    local = await createSellerNode({ mock: { clock: () => 1_790_000_000_000 }, sellerToken: LOCAL_TOKEN, modelMap: { [MODEL]: 'qwen2.5:14b' } });
    timers = new ManualTimers();
    connector = null;
    logs.length = 0;
  });
  afterEach(async () => {
    await connector?.stop();
    await hub.close();
    await local.close();
  });

  it('says hello with the agent and the detected models', async () => {
    const detected: DetectedModel[] = [
      { local: 'qwen2.5:14b-instruct', server: 'ollama', model: MODEL, quantization: 'q4_k_m', sizeBytes: 8988124069 },
      { local: 'deepseek-r1:14b', server: 'ollama', model: null },
    ];
    const c = make({ agent: { name: 'inferit-seller', version: '0.3.0', os: 'linux 6.8.0', arch: 'x64' }, detected });
    const live = new Promise((r) => c.once('live', r));
    c.start();
    await live;
    expect(frame(hub.conns[0]!, 'hello')).toEqual({
      t: 'hello',
      v: 1,
      node: { name: 'test-node', version: '0.3.0' },
      models: [{ id: MODEL, quantization: 'q4_k_m' }],
      capacity: { maxConcurrent: 2 },
      agent: { name: 'inferit-seller', version: '0.3.0', os: 'linux 6.8.0', arch: 'x64' },
      detected,
    });
  });

  it('logs every failed attempt with its reason, uses a fresh connection each time, and comes back after the failures', async () => {
    const c = make();
    const failed = failures(c);
    const scheduled: { delayMs: number; reason: string }[] = [];
    c.on('scheduled', (e: { delayMs: number; reason: string }) => scheduled.push(e));
    hub.refuseWith = 502;
    hub.refuseBody = JSON.stringify({ error: { message: 'Bad gateway upstream', type: 'service_unavailable', code: 'bad_gateway' } });
    c.start();
    await waitFor(() => failed.length === 1, 'the first failure');
    timers.advance(1_000);
    await waitFor(() => failed.length === 2, 'the second failure');
    expect(failed.map((f) => [f.attempt, f.httpStatus, f.refusal?.code, f.delayMs])).toEqual([
      [1, 502, 'bad_gateway', 1_000],
      [2, 502, 'bad_gateway', 2_000],
    ]);
    const warns = logs.filter((l) => l.level === 'warn' && l.msg.startsWith('connector attempt failed'));
    expect(warns.map((l) => l.msg)).toEqual(['connector attempt failed: HTTP 502 bad_gateway: Bad gateway upstream', 'connector attempt failed: HTTP 502 bad_gateway: Bad gateway upstream']);
    expect(warns[0]!.obj).toMatchObject({ attempt: 1, httpStatus: 502, error: null, retryInMs: 1_000 });
    expect(JSON.stringify(logs)).not.toContain(TOKEN);
    expect(c.status()).toMatchObject({ state: 'waiting', failedAttempts: 2, lastFailure: { httpStatus: 502 } });

    hub.refuseWith = null;
    const live = new Promise((r) => c.once('live', r));
    timers.advance(2_000);
    await live;
    expect(new Set(hub.upgrades).size).toBe(hub.upgrades.length); // three attempts, three connections
    expect(hub.upgrades).toHaveLength(3);
    expect(c.status()).toMatchObject({ state: 'live', failedAttempts: 0, offers: 0 });
    expect(scheduled.map((x) => x.reason)).toEqual(['backoff', 'backoff']);
  });

  it("logs the market's reason for a refused token and waits 5 minutes", async () => {
    hub.refuseWith = 403;
    hub.refuseBody = JSON.stringify({ error: { message: 'Register as a seller on the escrow first (registerSeller from your wallet).', type: 'permission_error', code: 'seller_not_registered' } });
    const c = make();
    const scheduled: { delayMs: number; reason: string }[] = [];
    c.on('scheduled', (e: { delayMs: number; reason: string }) => scheduled.push(e));
    c.start();
    await waitFor(() => scheduled.length === 1, 'the retry');
    expect(scheduled[0]).toEqual({ delayMs: 300_000, reason: 'auth' });
    const err = logs.find((l) => l.level === 'error')!;
    expect(err.msg).toContain('(Register as a seller on the escrow first (registerSeller from your wallet).); retrying in 5 minutes');
    expect(err.obj).toMatchObject({ httpStatus: 403, refusal: { code: 'seller_not_registered' } });
  });

  it('reports a connection refused and a handshake that never answers', async () => {
    const dead = createTcpServer();
    await new Promise<void>((r) => dead.listen(0, '127.0.0.1', r));
    const port = (dead.address() as AddressInfo).port;
    await new Promise<void>((r) => dead.close(() => r()));
    const c1 = make({ url: `ws://127.0.0.1:${port}/v1/seller/connect` });
    const f1 = failures(c1);
    c1.start();
    await waitFor(() => f1.length === 1, 'the refused attempt');
    expect(f1[0]).toMatchObject({ code: 'ECONNREFUSED', httpStatus: null });
    expect(describeFailure(f1[0]!)).toMatch(/ECONNREFUSED/);
    await c1.stop();

    // A server that accepts the TCP connection and never answers the upgrade (what a wedged network path looks like).
    const sockets: import('node:net').Socket[] = [];
    const silent: TcpServer = createTcpServer((s) => sockets.push(s));
    await new Promise<void>((r) => silent.listen(0, '127.0.0.1', r));
    try {
      const c2 = make({ url: `ws://127.0.0.1:${(silent.address() as AddressInfo).port}/v1/seller/connect`, wsOptions: { handshakeTimeout: 150 } });
      const f2 = failures(c2);
      c2.start();
      await waitFor(() => f2.length === 1, 'the timed-out attempt');
      expect(f2[0]!.error).toBe('Opening handshake has timed out');
      expect(c2.status().state).toBe('waiting');
    } finally {
      for (const s of sockets) s.destroy();
      await new Promise<void>((r) => silent.close(() => r()));
    }
  });

  it('drops an attempt that gets no welcome within 45 s, and retries', async () => {
    hub.autoWelcome = false;
    const c = make();
    const failed = failures(c);
    c.start();
    await waitFor(() => hub.conns[0] && frame(hub.conns[0], 'hello'), 'the hello');
    expect(c.status().state).toBe('connecting');
    timers.advance(44_999);
    expect(failed).toHaveLength(0);
    timers.advance(1);
    await waitFor(() => failed.length === 1, 'the dropped attempt');
    expect(failed[0]).toMatchObject({ code: 'ATTEMPT_DEADLINE', error: 'no welcome from the market within 45 s', delayMs: 1_000 });
    hub.autoWelcome = true;
    const live = new Promise((r) => c.once('live', r));
    timers.advance(1_000);
    await live;
  });

  it('the watchdog gives up after 10 minutes with no session while attempts keep failing, once', async () => {
    const trips: WatchdogInfo[] = [];
    const c = make({ watchdog: { onTrip: (i) => trips.push(i) } });
    const failed = failures(c);
    hub.refuseWith = 502;
    c.start();
    let seen = 0;
    let elapsed = 0;
    while (trips.length === 0 && elapsed < 15 * 60_000) {
      await waitFor(() => failed.length > seen, `failure ${seen + 1}`);
      seen = failed.length;
      const d = failed.at(-1)!.delayMs;
      timers.advance(d);
      elapsed += d;
    }
    expect(trips).toHaveLength(1);
    expect(trips[0]!.noSessionMs).toBe(600_000);
    expect(trips[0]!.failedAttempts).toBeGreaterThanOrEqual(14);
    expect(trips[0]!.lastFailure).toMatchObject({ httpStatus: 502 });
    expect(logs.some((l) => l.level === 'error' && l.msg.startsWith('watchdog: no market session'))).toBe(true);
    timers.advance(30 * 60_000);
    await new Promise((r) => setTimeout(r, 20));
    expect(trips).toHaveLength(1);
  });

  it('the watchdog never trips while a session is live, and counts from when the session ended', async () => {
    const trips: WatchdogInfo[] = [];
    const c = make({ watchdog: { onTrip: (i) => trips.push(i), noSessionMs: 120_000 } });
    const live = new Promise((r) => c.once('live', r));
    c.start();
    await live;
    timers.advance(25 * 60_000);
    expect(trips).toHaveLength(0);
    // The market goes away for good: the drop is at t = 25 min, the trip 2 minutes later, not at once.
    hub.refuseWith = 502;
    const failed = failures(c);
    const scheduled: { delayMs: number }[] = [];
    c.on('scheduled', (e: { delayMs: number }) => scheduled.push(e));
    hub.conns[0]!.ws.terminate();
    await waitFor(() => scheduled.length === 1, 'the redial after the drop');
    expect(scheduled[0]!.delayMs).toBe(1_000); // the backoff was reset by the long session
    timers.advance(1_000);
    await waitFor(() => failed.length === 1, 'the first failure');
    timers.advance(59_000);
    expect(trips).toHaveLength(0);
    timers.advance(60_000);
    expect(trips).toHaveLength(1);
    expect(trips[0]!.noSessionMs).toBe(120_000);
  });

  it('after the computer sleeps, the next heartbeat redials at once', async () => {
    // A wall clock that jumps while the monotonic clock does not (macOS stops the monotonic clock in sleep).
    let wall = 1_790_000_000_000;
    const t = timers as ManualTimers & { wallNow?: () => number };
    t.wallNow = () => wall;
    const c = make({ heartbeatMs: 20_000 });
    let live = new Promise((r) => c.once('live', r));
    c.start();
    await live;
    timers.advance(20_000);
    wall += 20_000;
    expect(hub.conns[0]!.ws.readyState).toBe(WebSocket.OPEN); // an ordinary tick
    const scheduled: { delayMs: number; reason: string }[] = [];
    c.on('scheduled', (e: { delayMs: number; reason: string }) => scheduled.push(e));
    wall += 8 * 3_600_000; // eight hours asleep
    timers.advance(20_000);
    wall += 20_000;
    await waitFor(() => scheduled.length === 1, 'the redial');
    expect(logs.some((l) => l.msg === 'the computer was asleep; reconnecting' && l.obj.asleepSeconds === 28_800)).toBe(true);
    live = new Promise((r) => c.once('live', r));
    timers.advance(scheduled[0]!.delayMs);
    await live;
    expect(hub.conns).toHaveLength(2);
  });
});
