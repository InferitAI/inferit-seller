// `start` end to end (src/start.ts): a fake Ollama and a fake market (catalog + connector socket) on loopback. The app
// finds the server, maps qwen2.5:14b-instruct to the catalog, says hello with agent and detected, relays a request with
// the model renamed, keeps the state file, waits for a server that is not up yet, and the watchdog exits.
import fs from 'node:fs';
import { createServer, type IncomingMessage, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import os from 'node:os';
import path from 'node:path';
import type { Duplex } from 'node:stream';
import { fileURLToPath } from 'node:url';
import { Writable } from 'node:stream';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import WebSocket, { WebSocketServer } from 'ws';
import { makeLogger } from '../src/log.ts';
import { planModels, startSeller, type RunningSeller } from '../src/start.ts';
import { readState, tokenPrefix } from '../src/userconfig.ts';
import { PACKAGE_VERSION } from '../src/version.ts';
import { SELLER_NODE_VERSION } from '../src/index.ts';
import { CATALOG_BODY, OLLAMA_ROUTES, closedPort } from './fixtures.ts';

const TOKEN = `icn_${'S'.repeat(43)}`;
const MODEL = 'qwen/qwen-2.5-14b-instruct';

interface MarketConn {
  ws: WebSocket;
  auth: string | undefined;
  frames: Record<string, unknown>[];
  data: Buffer[];
}

/** The market: GET /v1/models, and the connector socket that welcomes every hello (with `offers`, default none). */
async function fakeMarket(o: { refuse?: number; offers?: unknown[]; catalog?: { object: string; data: unknown[] } } = {}) {
  const conns: MarketConn[] = [];
  let modelReads = 0;
  const wss = new WebSocketServer({ noServer: true, handleProtocols: (p) => (p.has('inferit.connector.v1') ? 'inferit.connector.v1' : false) });
  const server = createServer((req, res) => {
    if (req.url === '/v1/models') {
      modelReads++;
      res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify(o.catalog ?? CATALOG_BODY)); // read now: a test may change it
    }
    else res.writeHead(404).end();
  });
  server.on('upgrade', (req: IncomingMessage, socket: Duplex, head: Buffer) => {
    if (o.refuse) {
      socket.end(`HTTP/1.1 ${o.refuse} No\r\nconnection: close\r\ncontent-length: 0\r\n\r\n`);
      return;
    }
    wss.handleUpgrade(req, socket, head, (ws) => {
      const c: MarketConn = { ws, auth: req.headers.authorization, frames: [], data: [] };
      conns.push(c);
      ws.on('message', (d, bin) => {
        if (bin) return void c.data.push(Buffer.from(d as Buffer));
        const f = JSON.parse(d.toString()) as Record<string, unknown>;
        c.frames.push(f);
        if (f.t === 'hello') ws.send(JSON.stringify({ t: 'welcome', v: 1, nodeId: `node_${'ab'.repeat(12)}`, sessionId: `cs_${conns.length}`, heartbeatMs: 20_000, maxConcurrent: 4, offers: o.offers ?? [] }));
      });
    });
  });
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
  return {
    server,
    conns,
    modelReads: () => modelReads,
    url: `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
    close: async () => {
      for (const c of conns) c.ws.terminate();
      await new Promise<void>((r) => server.close(() => r()));
    },
  };
}

/** Ollama: its native API plus /v1/chat/completions that answers with the model it was asked for, and the load-only
 *  /api/generate keep-warm sends (`holdGenerate` leaves those unanswered until release(): a load that takes long).
 *  `routes` may be changed while it runs (a model loading changes /api/ps). */
async function fakeOllama(port = 0, routes: Record<string, unknown> = OLLAMA_ROUTES, o: { holdGenerate?: boolean } = {}) {
  const asked: string[] = [];
  const generated: Record<string, unknown>[] = [];
  const held: (() => void)[] = [];
  const server = createServer((req, res) => {
    if (req.method === 'POST' && req.url === '/api/generate') {
      let raw = '';
      req.on('data', (c: Buffer) => (raw += c.toString()));
      req.on('end', () => {
        const body = JSON.parse(raw) as Record<string, unknown>;
        generated.push(body);
        const answer = () => res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify({ model: body.model, response: '', done: true, done_reason: 'load' }));
        if (o.holdGenerate) held.push(answer);
        else answer();
      });
      return;
    }
    if (req.method === 'POST' && req.url === '/v1/chat/completions') {
      let raw = '';
      req.on('data', (c: Buffer) => (raw += c.toString()));
      req.on('end', () => {
        const body = JSON.parse(raw) as { model: string };
        asked.push(body.model);
        res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify({ id: 'chatcmpl-1', model: body.model, choices: [{ index: 0, message: { role: 'assistant', content: 'hi' } }], usage: { prompt_tokens: 3, completion_tokens: 1 } }));
      });
      return;
    }
    const body = routes[req.url ?? ''];
    if (body === undefined) res.writeHead(404).end();
    else res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify(body));
  });
  await new Promise<void>((r) => server.listen(port, '127.0.0.1', r));
  return {
    server,
    asked,
    generated,
    origin: `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
    release: () => {
      for (const a of held.splice(0)) a();
    },
  };
}

async function waitFor<T>(get: () => T | undefined | false | null, what: string, timeoutMs = 5_000): Promise<T> {
  const t0 = Date.now();
  for (;;) {
    const v = get();
    if (v !== undefined && v !== false && v !== null) return v;
    if (Date.now() - t0 > timeoutMs) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 10));
  }
}

const closeServer = (s: Server) =>
  new Promise<void>((r) => {
    s.closeAllConnections();
    s.close(() => r());
  });

let dir: string;
let lines: Record<string, unknown>[];
let sink: Writable;
let running: RunningSeller | null;
const toClose: (() => Promise<void>)[] = [];
beforeEach(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'inferit-seller-start-'));
  lines = [];
  sink = new Writable({
    write(chunk: Buffer, _e, cb) {
      for (const l of chunk.toString().split('\n')) if (l.trim()) lines.push(JSON.parse(l) as Record<string, unknown>);
      cb();
    },
  });
  running = null;
});
afterEach(async () => {
  await running?.stop();
  for (const f of toClose.splice(0)) await f();
  fs.rmSync(dir, { recursive: true, force: true });
});

const deps = (env: NodeJS.ProcessEnv, extra: Partial<Parameters<typeof startSeller>[0]> = {}): Parameters<typeof startSeller>[0] => ({
  env,
  appHome: dir,
  config: null,
  log: makeLogger(sink, 'debug'),
  sink,
  exit: () => {
    throw new Error('exit');
  },
  service: null,
  keepAwake: () => null,
  ...extra,
});

describe('startSeller (the CLI start)', () => {
  it('finds Ollama, maps its model, says hello with agent and detected, and relays a request', async () => {
    const market = await fakeMarket();
    const ollama = await fakeOllama();
    toClose.push(market.close, () => closeServer(ollama.server));
    running = await startSeller(deps({ INFERIT_TOKEN: TOKEN, INFERIT_API_URL: market.url, UPSTREAM_BASE_URL: `${ollama.origin}/v1`, NODE_NAME: 'jeans-pc' }));
    expect(running.served).toEqual([{ catalogId: MODEL, local: 'qwen2.5:14b-instruct', server: 'ollama', baseUrl: `${ollama.origin}/v1`, quantization: 'q4_k_m', contextLength: 4096 }]);
    await waitFor(() => readState(dir)?.phase === 'live', 'the live state');

    const conn = market.conns[0]!;
    expect(conn.auth).toBe(`Bearer ${TOKEN}`);
    expect(conn.frames[0]).toEqual({
      t: 'hello',
      v: 1,
      node: { name: 'jeans-pc', version: PACKAGE_VERSION },
      models: [{ id: MODEL, quantization: 'q4_k_m' }],
      capacity: { maxConcurrent: 4 },
      agent: { name: 'inferit-seller', version: PACKAGE_VERSION, os: `${process.platform} ${os.release()}`.slice(0, 64), arch: process.arch },
      detected: [
        { local: 'qwen2.5:14b-instruct', server: 'ollama', model: MODEL, contextLength: 4096, quantization: 'q4_k_m', sizeBytes: 8988124069 },
        { local: 'nomic-embed-text:latest', server: 'ollama', model: null, quantization: 'f16', sizeBytes: 274302450, chat: false },
      ],
    });

    conn.ws.send(JSON.stringify({ t: 'req', id: '0'.repeat(31) + '1', model: MODEL, path: '/v1/chat/completions', stream: false, body: { model: MODEL, messages: [{ role: 'user', content: 'hi' }] }, firstByteTimeoutMs: 1000, credit: 262144 }));
    await waitFor(() => conn.frames.find((f) => f.t === 'end'), 'the end frame');
    expect(conn.frames.find((f) => f.t === 'head')).toMatchObject({ status: 200 });
    expect(ollama.asked).toEqual(['qwen2.5:14b-instruct']); // the local name upstream
    const reply = JSON.parse(Buffer.concat(conn.data.map((d) => d.subarray(17))).toString()) as { model: string };
    expect(reply.model).toBe(MODEL); // the catalog id back to the market

    const st = readState(dir)!;
    expect(st).toMatchObject({ pid: process.pid, phase: 'live', nodeId: `node_${'ab'.repeat(12)}`, offers: 0, tokenPrefix: 'icn_SSSS…SSSS', models: [{ id: MODEL, local: 'qwen2.5:14b-instruct', server: 'ollama' }] });
    expect(lines.some((l) => String(l.msg).includes('No price is listed for this machine yet'))).toBe(true);
    expect(JSON.stringify(lines)).not.toContain(TOKEN);
    expect(fs.existsSync(path.join(dir, 'catalog.json'))).toBe(true);
    await running.stop();
    running = null;
    expect(readState(dir)!.phase).toBe('stopped');
  });

  it('keeps the market\'s offers for this machine (welcome, then offers frames) in the state file for status and doctor', async () => {
    const market = await fakeMarket();
    const ollama = await fakeOllama();
    toClose.push(market.close, () => closeServer(ollama.server));
    running = await startSeller(deps({ INFERIT_TOKEN: TOKEN, INFERIT_API_URL: market.url, UPSTREAM_BASE_URL: `${ollama.origin}/v1` }));
    await waitFor(() => readState(dir)?.phase === 'live', 'the live state');
    expect(readState(dir)).toMatchObject({ offers: 0, offerList: [] });
    // The seller lists a price in the Sell workspace: the market tells the live session.
    market.conns[0]!.ws.send(JSON.stringify({ t: 'offers', offers: [{ offerId: 'of_1', model: MODEL, maxContextTokens: 4096 }] }));
    await waitFor(() => readState(dir)?.offers === 1, 'the offers in the state file');
    expect(readState(dir)!.offerList).toEqual([{ model: MODEL, maxContextTokens: 4096 }]);
    expect(running.connector!.status()).toMatchObject({ offers: 1, offerList: [{ offerId: 'of_1', model: MODEL, maxContextTokens: 4096 }] });
    expect(lines.some((l) => l.msg === 'offers on the market for this machine: 1')).toBe(true);
    // Delisted: back to none. Malformed entries are dropped.
    market.conns[0]!.ws.send(JSON.stringify({ t: 'offers', offers: [{ model: 1 }] }));
    await waitFor(() => readState(dir)?.offers === 0, 'no offers');
    expect(readState(dir)!.offerList).toEqual([]);
  });

  it('a model Ollama loads after start: the served context is read again, said in a new hello and kept for doctor', async () => {
    const market = await fakeMarket();
    const routes: Record<string, unknown> = { ...OLLAMA_ROUTES, '/api/ps': { models: [] } }; // nothing loaded yet
    const ollama = await fakeOllama(0, routes);
    toClose.push(market.close, () => closeServer(ollama.server));
    running = await startSeller(deps({ INFERIT_TOKEN: TOKEN, INFERIT_API_URL: market.url, UPSTREAM_BASE_URL: `${ollama.origin}/v1` }, { contextRecheckMs: 30 }));
    await waitFor(() => readState(dir)?.phase === 'live', 'the live state');
    const hello1 = market.conns[0]!.frames[0] as { detected: Record<string, unknown>[] };
    // Not loaded: no served context, and never the model's 32768 maximum from /api/tags.
    expect(hello1.detected[0]).toEqual({ local: 'qwen2.5:14b-instruct', server: 'ollama', model: MODEL, quantization: 'q4_k_m', sizeBytes: 8988124069 });
    // Keep-warm loaded it (and timed the load); this fake's /api/ps does not change on its own.
    expect(readState(dir)!.models).toEqual([{ id: MODEL, local: 'qwen2.5:14b-instruct', server: 'ollama', coldLoadMs: expect.any(Number), coldLoadAt: expect.any(String) }]);

    // Ollama loads it (a request, or `ollama run`) with 8192 tokens of context.
    routes['/api/ps'] = { models: [{ name: 'qwen2.5:14b-instruct', size: 9459613039, context_length: 8192 }] };
    await waitFor(() => market.conns[1]?.frames[0], 'a second hello');
    expect((market.conns[1]!.frames[0] as { detected: Record<string, unknown>[] }).detected[0]).toMatchObject({ local: 'qwen2.5:14b-instruct', contextLength: 8192 });
    await waitFor(() => readState(dir)?.models[0]?.contextLength === 8192, 'the served context in the state file');
    expect(lines.some((l) => l.msg === 'Ollama serves qwen2.5:14b-instruct with 8192 tokens of context')).toBe(true);

    // Unloaded again (keep_alive ran out): the last known value stays; no new hello.
    routes['/api/ps'] = { models: [] };
    await new Promise((r) => setTimeout(r, 150));
    expect(market.conns).toHaveLength(2);
    expect(readState(dir)!.models[0]!.contextLength).toBe(8192);
  });

  it('waits for the local server at boot, then starts', async () => {
    const market = await fakeMarket();
    toClose.push(market.close);
    const port = await closedPort();
    const waits: number[] = [];
    let ollama: Awaited<ReturnType<typeof fakeOllama>> | null = null;
    running = await startSeller(
      deps(
        { INFERIT_TOKEN: TOKEN, INFERIT_API_URL: market.url, UPSTREAM_BASE_URL: `http://127.0.0.1:${port}/v1` },
        {
          sleep: async (ms) => {
            waits.push(ms);
            if (waits.length === 2) ollama = await fakeOllama(port); // Ollama comes up during the second wait
          },
        },
      ),
    );
    toClose.push(() => closeServer(ollama!.server));
    expect(waits).toEqual([5_000, 10_000]);
    expect(running.served.map((s) => s.catalogId)).toEqual([MODEL]);
    expect(lines.filter((l) => String(l.msg).startsWith('waiting:'))).toHaveLength(1); // said once, not every retry
    await waitFor(() => market.conns[0], 'the connection');
  });

  it('the watchdog exits 1 when the market keeps refusing', async () => {
    const market = await fakeMarket({ refuse: 502 });
    const ollama = await fakeOllama();
    toClose.push(market.close, () => closeServer(ollama.server));
    const exits: number[] = [];
    running = await startSeller(
      deps({ INFERIT_TOKEN: TOKEN, INFERIT_API_URL: market.url, UPSTREAM_BASE_URL: `${ollama.origin}/v1`, INFERIT_WATCHDOG_MINUTES: '0.005' }, { exit: (c) => exits.push(c) }),
    );
    await waitFor(() => exits.length > 0, 'the watchdog exit', 5_000);
    expect(exits).toEqual([1]);
    expect(lines.some((l) => String(l.msg).startsWith('connector attempt failed: HTTP 502'))).toBe(true);
    expect(readState(dir)).toMatchObject({ phase: 'stopped', message: expect.stringContaining('watchdog') });
  });

  it('the watchdog exits only when something restarts the process: under a service, never in a bare terminal', async () => {
    const market = await fakeMarket({ refuse: 502 });
    const ollama = await fakeOllama();
    toClose.push(market.close, () => closeServer(ollama.server));
    const env = { INFERIT_TOKEN: TOKEN, INFERIT_API_URL: market.url, UPSTREAM_BASE_URL: `${ollama.origin}/v1` };

    // A terminal (no service, INFERIT_WATCHDOG_MINUTES unset): it warns, again each window, and keeps trying.
    const terminalExits: number[] = [];
    running = await startSeller(deps(env, { watchdogMinutes: 0.005, exit: (c) => terminalExits.push(c) }));
    const stillTrying = () => lines.filter((l) => String(l.msg).includes('still trying'));
    await waitFor(() => stillTrying().length >= 2, 'two watchdog warnings', 5_000);
    expect(terminalExits).toEqual([]);
    expect(stillTrying()[0]).toMatchObject({ level: 50, lastFailure: expect.stringContaining('HTTP 502') });
    expect(running.connector!.status().state).not.toBe('stopped');
    await running.stop();
    running = null;

    // The same under launchd: it exits 1 so launchd starts a fresh process.
    const serviceExits: number[] = [];
    running = await startSeller(deps(env, { watchdogMinutes: 0.005, service: 'launchd', exit: (c) => serviceExits.push(c) }));
    await waitFor(() => serviceExits.length > 0, 'the watchdog exit', 5_000);
    expect(serviceExits).toEqual([1]);
  });

  it('keep-warm: asks Ollama to keep the model loaded before dialing out, and says so in the state file', async () => {
    const market = await fakeMarket({ offers: [{ offerId: 'off_1', model: MODEL }] });
    const ollama = await fakeOllama();
    toClose.push(market.close, () => closeServer(ollama.server));
    running = await startSeller(deps({ INFERIT_TOKEN: TOKEN, INFERIT_API_URL: market.url, UPSTREAM_BASE_URL: `${ollama.origin}/v1` }));
    // The round at start is done before the connector starts (this Ollama answers at once).
    expect(ollama.generated).toEqual([{ model: 'qwen2.5:14b-instruct', keep_alive: '10m' }]);
    await waitFor(() => readState(dir)?.phase === 'live', 'the live state');
    expect(readState(dir)!.keepWarm).toEqual({ on: true, keepAlive: '10m', everyMs: 240_000, models: ['qwen2.5:14b-instruct'] });
    // /api/ps lists it: already loaded, so no cold load to record.
    expect(readState(dir)!.models[0]).not.toHaveProperty('coldLoadMs');
    expect(lines.some((l) => l.msg === 'qwen2.5:14b-instruct is loaded in Ollama; keep-warm keeps it there while the app sells (keep_alive 10m, refreshed every 4 min)')).toBe(true);
  });

  it('keep-warm: records a cold load in the state file, and keeps it across a restart with keep-warm off', async () => {
    const market = await fakeMarket();
    const ollama = await fakeOllama(0, { ...OLLAMA_ROUTES, '/api/ps': { models: [] } }); // not loaded
    toClose.push(market.close, () => closeServer(ollama.server));
    const env = { INFERIT_TOKEN: TOKEN, INFERIT_API_URL: market.url, UPSTREAM_BASE_URL: `${ollama.origin}/v1` };
    running = await startSeller(deps(env));
    await waitFor(() => readState(dir)?.phase === 'live', 'the live state');
    const m = readState(dir)!.models[0]!;
    expect(m).toMatchObject({ id: MODEL, coldLoadMs: expect.any(Number), coldLoadAt: expect.any(String) });
    expect(lines.some((l) => /^Ollama loaded qwen2\.5:14b-instruct in \d+\.\d s; keep-warm keeps it in memory/.test(String(l.msg)))).toBe(true);
    await running.stop();

    // Off: nothing is sent to Ollama; the last measured cold load stays for doctor.
    running = await startSeller(deps({ ...env, INFERIT_KEEP_WARM: '0' }));
    await waitFor(() => readState(dir)?.phase === 'live', 'the live state');
    expect(ollama.generated).toHaveLength(1);
    expect(readState(dir)!.keepWarm).toEqual({ on: false, keepAlive: '10m', everyMs: 240_000 });
    expect(readState(dir)!.models[0]).toMatchObject({ coldLoadMs: m.coldLoadMs, coldLoadAt: m.coldLoadAt });
    expect(lines.some((l) => String(l.msg).startsWith('keep-warm is off (INFERIT_KEEP_WARM=0)'))).toBe(true);
  });

  it('keep-warm: waits for a cold load before dialing out, so the market\'s first check finds the model loaded', async () => {
    const market = await fakeMarket({ offers: [{ offerId: 'off_1', model: MODEL }] });
    const ollama = await fakeOllama(0, { ...OLLAMA_ROUTES, '/api/ps': { models: [] } }, { holdGenerate: true });
    toClose.push(market.close, () => closeServer(ollama.server));
    let started = false;
    const p = startSeller(deps({ INFERIT_TOKEN: TOKEN, INFERIT_API_URL: market.url, UPSTREAM_BASE_URL: `${ollama.origin}/v1` }));
    void p.then(() => (started = true));
    await waitFor(() => ollama.generated.length === 1 || undefined, 'the load request');
    await new Promise((r) => setTimeout(r, 300)); // Ollama is still loading
    expect(started).toBe(false);
    expect(market.conns).toHaveLength(0);
    expect(readState(dir)!.message).toBe('asking Ollama to load the models with an offer before connecting');
    ollama.release();
    running = await p;
    await waitFor(() => readState(dir)?.phase === 'live', 'the live state');
    expect(readState(dir)!.models[0]).toMatchObject({ coldLoadMs: expect.any(Number) });
  });

  it('keep-warm: keeps only models with an offer; the last run\'s offers decide at start, the welcome after', async () => {
    const market = await fakeMarket({ offers: [{ offerId: 'off_1', model: MODEL }] });
    const ollama = await fakeOllama();
    toClose.push(market.close, () => closeServer(ollama.server));
    // The last run (same token) had no offer: nothing is loaded at start.
    fs.writeFileSync(path.join(dir, 'state.json'), JSON.stringify({ pid: 1, tokenPrefix: tokenPrefix(TOKEN), phase: 'stopped', models: [], offerList: [], updatedAt: new Date().toISOString() }));
    expect(readState(dir)?.offerList).toEqual([]);
    running = await startSeller(deps({ INFERIT_TOKEN: TOKEN, INFERIT_API_URL: market.url, UPSTREAM_BASE_URL: `${ollama.origin}/v1` }));
    // The welcome lists an offer for it: loaded then.
    await waitFor(() => ollama.generated.length === 1 || undefined, 'the load after the welcome');
    await waitFor(() => readState(dir)?.keepWarm?.models?.length === 1 || undefined, 'the state');
    expect(readState(dir)!.keepWarm!.models).toEqual(['qwen2.5:14b-instruct']);
    // Not by the round at start (that one says "is loaded in Ollama"), by the round after the welcome.
    expect(lines.some((l) => String(l.msg).includes('is loaded in Ollama'))).toBe(false);
    expect(lines.some((l) => l.msg === 'keep-warm: refreshed')).toBe(true);
  });

  it('keep-warm: a slow load does not hold up connecting by more than the head start', async () => {
    const market = await fakeMarket();
    const ollama = await fakeOllama(0, { ...OLLAMA_ROUTES, '/api/ps': { models: [] } }, { holdGenerate: true });
    toClose.push(market.close, () => closeServer(ollama.server));
    running = await startSeller(deps({ INFERIT_TOKEN: TOKEN, INFERIT_API_URL: market.url, UPSTREAM_BASE_URL: `${ollama.origin}/v1` }, { keepWarmHeadStartMs: 50 }));
    await waitFor(() => readState(dir)?.phase === 'live', 'the live state');
    expect(ollama.generated).toHaveLength(1); // still loading
    expect(readState(dir)!.models[0]).not.toHaveProperty('coldLoadMs');
  });

  it('keep-warm: sends nothing to a server that is not Ollama (vLLM keeps its model loaded)', async () => {
    const market = await fakeMarket();
    const hits: string[] = [];
    const vllm = createServer((req, res) => {
      hits.push(`${req.method} ${req.url}`);
      if (req.url === '/v1/models') res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify({ object: 'list', data: [{ id: 'Qwen/Qwen2.5-14B-Instruct', owned_by: 'vllm', max_model_len: 32768 }] }));
      else res.writeHead(404).end();
    });
    await new Promise<void>((r) => vllm.listen(0, '127.0.0.1', r));
    toClose.push(market.close, () => closeServer(vllm));
    running = await startSeller(deps({ INFERIT_TOKEN: TOKEN, INFERIT_API_URL: market.url, UPSTREAM_BASE_URL: `http://127.0.0.1:${(vllm.address() as AddressInfo).port}/v1` }));
    expect(running.served).toMatchObject([{ catalogId: MODEL, server: 'vllm' }]);
    await waitFor(() => readState(dir)?.phase === 'live', 'the live state');
    expect(hits.filter((h) => h.includes('/api/generate'))).toEqual([]);
    expect(readState(dir)!.keepWarm).toBeNull();
  });

  it('open listing: a machine with only a model the market lacks connects, then serves it once the seller lists it (no restart)', async () => {
    const LOCAL = 'my-finetune:latest';
    const LISTED = 'acme/my-finetune-7b';
    const catalog: { object: string; data: unknown[] } = { object: 'list', data: [...CATALOG_BODY.data] };
    const market = await fakeMarket({ catalog });
    const routes: Record<string, unknown> = {
      ...OLLAMA_ROUTES,
      '/api/tags': {
        models: [
          { name: LOCAL, model: LOCAL, size: 4_700_000_000, details: { family: 'qwen2', parameter_size: '7.6B', quantization_level: 'Q4_0' }, capabilities: ['completion'] },
          { name: 'nomic-embed-text:latest', size: 274302450, details: { parameter_size: '137M', quantization_level: 'F16' }, capabilities: ['embedding'] },
        ],
      },
      '/api/ps': { models: [{ name: LOCAL, size: 5_000_000_000, context_length: 8192 }] },
    };
    const ollama = await fakeOllama(0, routes);
    toClose.push(market.close, () => closeServer(ollama.server));
    running = await startSeller(deps({ INFERIT_TOKEN: TOKEN, INFERIT_API_URL: market.url, UPSTREAM_BASE_URL: `${ollama.origin}/v1` }));
    await waitFor(() => readState(dir)?.phase === 'live', 'the live state');

    // Nothing to serve yet, but it connects, so the Sell workspace can list the model: no models declared, the chat
    // model reported unmatched and the embedding model marked as not a chat model.
    expect(running.served).toEqual([]);
    const hello1 = market.conns[0]!.frames[0] as { models: unknown[]; detected: Record<string, unknown>[] };
    expect(hello1.models).toEqual([]);
    expect(hello1.detected).toEqual([
      { local: LOCAL, server: 'ollama', model: null, contextLength: 8192, quantization: 'q4_0', sizeBytes: 4_700_000_000 },
      { local: 'nomic-embed-text:latest', server: 'ollama', model: null, quantization: 'f16', sizeBytes: 274302450, chat: false },
    ]);
    expect(lines.some((l) => l.msg === `${LOCAL}: not in the catalog yet: list it in the Sell workspace (https://inferit.app/sell)`)).toBe(true);
    // Its proxy serves no model until one is mapped (an empty map is never "any model").
    const id1 = '0'.repeat(31) + '1';
    market.conns[0]!.ws.send(JSON.stringify({ t: 'req', id: id1, model: LISTED, path: '/v1/chat/completions', stream: false, body: { model: LISTED, messages: [{ role: 'user', content: 'hi' }] }, firstByteTimeoutMs: 1000, credit: 262144 }));
    await waitFor(() => market.conns[0]!.frames.find((f) => f.t === 'head' && f.id === id1), 'the refusal');
    expect(market.conns[0]!.frames.find((f) => f.t === 'head' && f.id === id1)).toMatchObject({ status: 404 });
    expect(ollama.asked).toEqual([]);

    // The seller lists it in the Sell workspace (POST /v1/seller/models, the local name as an alias): GET /v1/models
    // has it now, and the market tells the live session in an offers frame.
    catalog.data.push({ id: LISTED, aliases: [LOCAL], context_length: 8192, servable: true, listed_by: 'seller', catalog: false });
    const reads = market.modelReads();
    market.conns[0]!.ws.send(JSON.stringify({ t: 'offers', offers: [], listed: [LISTED] }));

    // The app reads the model list again, maps the local model to the listing and says a new hello declaring it.
    await waitFor(() => market.conns[1]?.frames[0], 'a second hello');
    expect(market.modelReads()).toBeGreaterThan(reads);
    const hello2 = market.conns[1]!.frames[0] as { models: unknown[]; detected: Record<string, unknown>[] };
    expect(hello2.models).toEqual([{ id: LISTED, quantization: 'q4_0' }]);
    expect(hello2.detected[0]).toMatchObject({ local: LOCAL, model: LISTED, contextLength: 8192 });
    expect(running.served).toEqual([expect.objectContaining({ catalogId: LISTED, local: LOCAL, server: 'ollama' })]);
    expect(lines.some((l) => String(l.msg).startsWith(`serving ${LISTED} from Ollama (${LOCAL})`))).toBe(true);
    await waitFor(() => readState(dir)?.models.some((m) => m.id === LISTED) || undefined, 'the state file');

    // And serves it, under its local name, on the new session.
    await waitFor(() => readState(dir)?.phase === 'live' && market.conns[1]!.frames.length > 0 || undefined, 'the new session');
    const id2 = '0'.repeat(31) + '2';
    market.conns[1]!.ws.send(JSON.stringify({ t: 'req', id: id2, model: LISTED, path: '/v1/chat/completions', stream: false, body: { model: LISTED, messages: [{ role: 'user', content: 'hi' }] }, firstByteTimeoutMs: 1000, credit: 262144 }));
    await waitFor(() => market.conns[1]!.frames.find((f) => f.t === 'end' && f.id === id2), 'the end frame');
    expect(market.conns[1]!.frames.find((f) => f.t === 'head' && f.id === id2)).toMatchObject({ status: 200 });
    expect(ollama.asked).toEqual([LOCAL]);
  });

  it('open listing: when the model list does not show the new listing yet, the app looks again and then serves it', async () => {
    const LOCAL = 'my-finetune:latest';
    const LISTED = 'acme/my-finetune-7b';
    const catalog: { object: string; data: unknown[] } = { object: 'list', data: [...CATALOG_BODY.data] };
    const market = await fakeMarket({ catalog });
    const routes: Record<string, unknown> = {
      ...OLLAMA_ROUTES,
      '/api/tags': { models: [...(OLLAMA_ROUTES['/api/tags'] as { models: unknown[] }).models, { name: LOCAL, model: LOCAL, size: 4_700_000_000, details: { parameter_size: '7.6B', quantization_level: 'Q4_0' }, capabilities: ['completion'] }] },
    };
    const ollama = await fakeOllama(0, routes);
    toClose.push(market.close, () => closeServer(ollama.server));
    running = await startSeller(deps({ INFERIT_TOKEN: TOKEN, INFERIT_API_URL: market.url, UPSTREAM_BASE_URL: `${ollama.origin}/v1` }, { listedRetryMs: [50, 50, 50] }));
    await waitFor(() => readState(dir)?.phase === 'live', 'the live state');
    expect(running.served.map((x) => x.catalogId)).toEqual([MODEL]); // a catalog model is served; the finetune is not yet
    // The frame comes before GET /v1/models shows the listing (another API instance answered).
    market.conns[0]!.ws.send(JSON.stringify({ t: 'offers', offers: [], listed: [LISTED] }));
    await waitFor(() => market.modelReads() >= 2 || undefined, 'a first read of the model list after the frame');
    catalog.data.push({ id: LISTED, aliases: [LOCAL], context_length: 8192, servable: true, listed_by: 'seller' });
    await waitFor(() => market.conns[1]?.frames[0], 'a new hello');
    expect((market.conns[1]!.frames[0] as { models: { id: string }[] }).models.map((m) => m.id)).toEqual([LISTED, MODEL]);
    expect(lines.some((l) => String(l.msg).includes('no local model has that name'))).toBe(false);
  });

  it('open listing: a listed model no local model is named as says so and maps nothing; MODEL_MAP is never remapped', async () => {
    const catalog: { object: string; data: unknown[] } = { object: 'list', data: [...CATALOG_BODY.data] };
    const market = await fakeMarket({ catalog });
    const ollama = await fakeOllama();
    toClose.push(market.close, () => closeServer(ollama.server));
    running = await startSeller(deps({ INFERIT_TOKEN: TOKEN, INFERIT_API_URL: market.url, UPSTREAM_BASE_URL: `${ollama.origin}/v1` }, { listedRetryMs: [20, 20] }));
    await waitFor(() => readState(dir)?.phase === 'live', 'the live state');
    catalog.data.push({ id: 'acme/other-7b', aliases: ['other:7b'], context_length: 8192, servable: true, listed_by: 'seller' });
    const reads0 = market.modelReads();
    market.conns[0]!.ws.send(JSON.stringify({ t: 'offers', offers: [], listed: ['acme/other-7b'] }));
    await waitFor(() => lines.find((l) => String(l.msg).startsWith('acme/other-7b: no local model has that name')), 'the log line');
    expect(market.modelReads() - reads0).toBe(3); // the model list was read again twice before giving up
    expect(running.served.map((s) => s.catalogId)).toEqual([MODEL]);
    expect(market.conns).toHaveLength(1); // no new hello
    await running.stop();
    running = null;

    // With MODEL_MAP the seller's own mapping stands: an offers frame never changes it.
    lines.length = 0;
    const reads = market.modelReads();
    running = await startSeller(deps({ INFERIT_TOKEN: TOKEN, INFERIT_API_URL: market.url, UPSTREAM_BASE_URL: `${ollama.origin}/v1`, MODEL_MAP: `{"${MODEL}":"qwen2.5:14b-instruct"}` }));
    await waitFor(() => readState(dir)?.phase === 'live' && market.conns.length === 2 || undefined, 'the second run live');
    market.conns[1]!.ws.send(JSON.stringify({ t: 'offers', offers: [], listed: ['acme/other-7b'] }));
    await new Promise((r) => setTimeout(r, 100));
    expect(market.modelReads()).toBe(reads);
    expect(market.conns).toHaveLength(2);
  });

  it('refuses a malformed token before doing anything', async () => {
    await expect(startSeller(deps({ INFERIT_TOKEN: 'icn_short', INFERIT_API_URL: 'http://127.0.0.1:9' }))).rejects.toThrow(/node token must be icn_/);
  });
});

describe('startSeller (main.ts, environment only)', () => {
  it('MOCK=1 with no market is the plain HTTP seller scripts/dev.sh runs', async () => {
    running = await startSeller(deps({ MOCK: '1', PORT: '0', SELLER_TOKEN: 'local' }, { appHome: null }));
    expect(running.connector).toBeNull();
    expect((await fetch(`${running.node.url}/health`)).status).toBe(200);
  });

  it('MODEL_MAP with an explicit upstream starts at once, even before the server is up (the Vast box)', async () => {
    const market = await fakeMarket();
    toClose.push(market.close);
    const port = await closedPort();
    running = await startSeller(
      deps(
        {
          CONNECT_URL: `${market.url.replace('http', 'ws')}/v1/seller/connect`,
          CONNECT_TOKEN: TOKEN,
          UPSTREAM_BASE_URL: `http://127.0.0.1:${port}/v1`,
          UPSTREAM_API_KEY: 'local-key',
          MODEL_MAP: '{"meta-llama/llama-3.1-8b-instruct":"llama-3.1-8b"}',
          MODEL_META: '{"meta-llama/llama-3.1-8b-instruct":{"maxContextTokens":32768,"quantization":"w4a16"}}',
          NODE_NAME: 'vast-3060-1',
          PORT: '0',
        },
        { appHome: null },
      ),
    );
    await waitFor(() => market.conns[0]?.frames[0], 'the hello');
    expect(market.conns[0]!.frames[0]).toMatchObject({
      node: { name: 'vast-3060-1', version: SELLER_NODE_VERSION },
      models: [{ id: 'meta-llama/llama-3.1-8b-instruct', maxContextTokens: 32768, quantization: 'w4a16' }],
      detected: [],
    });
    expect(running.served).toEqual([{ catalogId: 'meta-llama/llama-3.1-8b-instruct', local: 'llama-3.1-8b', server: 'custom', baseUrl: `http://127.0.0.1:${port}/v1` }]);
  });
});

describe('planModels', () => {
  it('names the problem when nothing can be served', async () => {
    const port = await closedPort();
    const none = await planModels({ env: {}, apiUrl: 'https://api.example', upstream: `http://127.0.0.1:${port}/v1`, hosts: [], appHome: null });
    expect(none).toMatchObject({ served: [], problem: expect.stringContaining('does not answer with a model list') });
    await expect(planModels({ env: {}, apiUrl: 'https://api.example', upstream: 'https://api.groq.com/openai/v1', hosts: [], appHome: null })).rejects.toThrow(/groq\.com, a hosted inference API/);
    const ollama = await fakeOllama();
    try {
      const offline = await planModels({ env: {}, apiUrl: `http://127.0.0.1:${port}`, upstream: `${ollama.origin}/v1`, hosts: [], appHome: null });
      expect(offline.problem).toMatch(/could not reach the market/);
    } finally {
      await closeServer(ollama.server);
    }
  });

  it('uses the saved catalog when the market is down at boot', async () => {
    const market = await fakeMarket();
    const ollama = await fakeOllama();
    try {
      const first = await planModels({ env: {}, apiUrl: market.url, upstream: `${ollama.origin}/v1`, hosts: [], appHome: dir });
      expect(first.served).toHaveLength(1);
      await market.close();
      const again = await planModels({ env: {}, apiUrl: market.url, upstream: `${ollama.origin}/v1`, hosts: [], appHome: dir });
      expect(again.served.map((s) => s.catalogId)).toEqual([MODEL]);
    } finally {
      await closeServer(ollama.server);
    }
  });
});

describe('version', () => {
  it('is the package version everywhere', () => {
    const pkg = JSON.parse(fs.readFileSync(new URL('../package.json', import.meta.url), 'utf8')) as { name: string; version: string; bin: Record<string, string>; private: boolean; license: string };
    expect(PACKAGE_VERSION).toBe(pkg.version);
    expect(SELLER_NODE_VERSION).toBe(pkg.version);
    expect(pkg).toMatchObject({ name: 'inferit-seller', private: false, license: 'Apache-2.0', bin: { 'inferit-seller': 'dist/cli.js' } });
  });

  it('the package has no workspace imports', () => {
    const src = fileURLToPath(new URL('../src', import.meta.url));
    for (const f of fs.readdirSync(src)) expect(fs.readFileSync(path.join(src, f), 'utf8'), f).not.toMatch(/from '@inferit\//);
  });
});
