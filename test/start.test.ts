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
import { readState } from '../src/userconfig.ts';
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

/** The market: GET /v1/models, and the connector socket that welcomes every hello. */
async function fakeMarket(o: { refuse?: number } = {}) {
  const conns: MarketConn[] = [];
  const wss = new WebSocketServer({ noServer: true, handleProtocols: (p) => (p.has('inferit.connector.v1') ? 'inferit.connector.v1' : false) });
  const server = createServer((req, res) => {
    if (req.url === '/v1/models') res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify(CATALOG_BODY));
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
        if (f.t === 'hello') ws.send(JSON.stringify({ t: 'welcome', v: 1, nodeId: `node_${'ab'.repeat(12)}`, sessionId: `cs_${conns.length}`, heartbeatMs: 20_000, maxConcurrent: 4, offers: [] }));
      });
    });
  });
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
  return {
    server,
    conns,
    url: `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
    close: async () => {
      for (const c of conns) c.ws.terminate();
      await new Promise<void>((r) => server.close(() => r()));
    },
  };
}

/** Ollama: its native API plus /v1/chat/completions that answers with the model it was asked for. */
async function fakeOllama(port = 0) {
  const asked: string[] = [];
  const server = createServer((req, res) => {
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
    const body = OLLAMA_ROUTES[req.url ?? ''];
    if (body === undefined) res.writeHead(404).end();
    else res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify(body));
  });
  await new Promise<void>((r) => server.listen(port, '127.0.0.1', r));
  return { server, asked, origin: `http://127.0.0.1:${(server.address() as AddressInfo).port}` };
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

const closeServer = (s: Server) => new Promise<void>((r) => s.close(() => r()));

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
        { local: 'nomic-embed-text:latest', server: 'ollama', model: null, quantization: 'f16', sizeBytes: 274302450 },
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
