// ~/.inferit-seller (src/userconfig.ts): the config file and its permissions, URL handling, names, the state file's
// liveness, and how the environment and the saved config combine (src/start.ts resolveTarget).
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { resolveTarget } from '../src/start.ts';
import {
  DEFAULT_API_URL,
  appHome,
  bootAt,
  connectUrlFromApi,
  defaultNodeName,
  isWorldReadable,
  liveState,
  loadConfig,
  normaliseApiUrl,
  readState,
  saveConfig,
  tokenPrefix,
  writeState,
  type RunState,
} from '../src/userconfig.ts';

const TOKEN = `icn_${'A'.repeat(43)}`;
const posix = process.platform !== 'win32';
let dir: string;
beforeEach(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'inferit-seller-cfg-'));
});
afterEach(() => {
  fs.rmSync(dir, { recursive: true, force: true });
});

describe('the config file', () => {
  it('lives in ~/.inferit-seller unless INFERIT_SELLER_HOME says otherwise', () => {
    expect(appHome({}, '/home/jean')).toBe(path.join('/home/jean', '.inferit-seller'));
    expect(appHome({ INFERIT_SELLER_HOME: dir }, '/home/jean')).toBe(dir);
  });

  it.runIf(posix)('is written 0600 in a 0700 directory, and stays 0600 when it was readable before', () => {
    const home = path.join(dir, 'h');
    const file = saveConfig(home, { token: TOKEN, apiUrl: DEFAULT_API_URL, connectUrl: connectUrlFromApi(DEFAULT_API_URL), nodeName: 'jeans-pc' });
    expect(fs.statSync(file).mode & 0o777).toBe(0o600);
    expect(fs.statSync(home).mode & 0o777).toBe(0o700);
    expect(isWorldReadable(file)).toBe(false);
    fs.chmodSync(file, 0o644);
    expect(isWorldReadable(file)).toBe(true);
    saveConfig(home, { token: TOKEN, apiUrl: DEFAULT_API_URL, connectUrl: connectUrlFromApi(DEFAULT_API_URL), nodeName: 'jeans-pc' });
    expect(fs.statSync(file).mode & 0o777).toBe(0o600);
    expect(fs.readdirSync(home)).toEqual(['config.json']); // no temporary file left behind
  });

  it('round-trips, and refuses a file without a valid token', () => {
    expect(loadConfig(dir)).toBeNull();
    saveConfig(dir, { token: TOKEN, apiUrl: 'https://api.example', connectUrl: 'wss://api.example/v1/seller/connect', nodeName: 'box', upstream: 'http://127.0.0.1:1234/v1', keepAwake: true });
    expect(loadConfig(dir)).toMatchObject({ token: TOKEN, apiUrl: 'https://api.example', connectUrl: 'wss://api.example/v1/seller/connect', nodeName: 'box', upstream: 'http://127.0.0.1:1234/v1', keepAwake: true });
    fs.writeFileSync(path.join(dir, 'config.json'), '{"token":"ik_seller_x"}');
    expect(() => loadConfig(dir)).toThrow(/no valid node token/);
    fs.writeFileSync(path.join(dir, 'config.json'), 'nope');
    expect(() => loadConfig(dir)).toThrow(/not valid JSON/);
  });
});

describe('URLs and names', () => {
  it('takes an https market URL (http only on this machine) and derives the socket URL', () => {
    expect(normaliseApiUrl('https://api.example/')).toBe('https://api.example');
    expect(normaliseApiUrl('https://api.example/v1')).toBe('https://api.example');
    expect(normaliseApiUrl('http://127.0.0.1:8787')).toBe('http://127.0.0.1:8787');
    expect(() => normaliseApiUrl('http://api.example')).toThrow(/https/);
    expect(() => normaliseApiUrl('https://u:p@api.example')).toThrow(/credentials/);
    expect(() => normaliseApiUrl('not a url')).toThrow(/--api/);
    expect(connectUrlFromApi('https://api.example')).toBe('wss://api.example/v1/seller/connect');
    expect(connectUrlFromApi('http://127.0.0.1:8787/')).toBe('ws://127.0.0.1:8787/v1/seller/connect');
    expect(connectUrlFromApi(DEFAULT_API_URL)).toBe('wss://api-production-c74b9.up.railway.app/v1/seller/connect');
  });

  it('names the machine after its hostname, in the characters the market accepts', () => {
    expect(defaultNodeName('Jeans-PC.local')).toBe('Jeans-PC');
    expect(defaultNodeName('box_1 (gpu)')).toBe('box_1 (gpu)');
    expect(defaultNodeName('héllo wörld!')).toBe('h-llo w-rld');
    expect(defaultNodeName('x'.repeat(80))).toHaveLength(64);
    expect(defaultNodeName('!!!')).toBe('my-machine');
    expect(tokenPrefix(TOKEN)).toBe('icn_AAAA…AAAA');
  });
});

describe('the state file', () => {
  const state = (patch: Partial<RunState> = {}): RunState => ({
    pid: process.pid,
    bootAt: bootAt(),
    startedAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    version: '0.3.0',
    tokenPrefix: null,
    service: null,
    phase: 'live',
    nodeId: 'node_x',
    sessionId: 'cs_1',
    liveSince: new Date().toISOString(),
    offers: 1,
    failedAttempts: 0,
    lastFailure: null,
    lastFailureAt: null,
    models: [],
    message: null,
    ...patch,
  });

  it('counts as running only for a live pid, from this boot, updated in the last 150 s', () => {
    writeState(dir, state());
    expect(readState(dir)?.pid).toBe(process.pid);
    expect(liveState(readState(dir))).toBe(true);
    expect(liveState(state({ phase: 'stopped' }))).toBe(false);
    expect(liveState(state({ updatedAt: new Date(Date.now() - 200_000).toISOString() }))).toBe(false);
    expect(liveState(state({ bootAt: bootAt() - 3_600_000 }))).toBe(false); // a pid from before a reboot
    expect(liveState(state({ pid: 2 ** 22 + 12345 }))).toBe(false);
    expect(liveState(null)).toBe(false);
  });
});

describe('resolveTarget', () => {
  const cfg = { token: TOKEN, apiUrl: 'https://api.example', connectUrl: 'wss://api.example/v1/seller/connect', nodeName: 'saved-name', upstream: 'http://127.0.0.1:1234/v1' };

  it('uses the saved config, and the environment wins over it', () => {
    expect(resolveTarget({}, cfg)).toEqual({ token: TOKEN, apiUrl: 'https://api.example', connectUrl: cfg.connectUrl, nodeName: 'saved-name', upstream: cfg.upstream });
    const other = `icn_${'B'.repeat(43)}`;
    expect(resolveTarget({ INFERIT_TOKEN: other, INFERIT_API_URL: 'https://other.example', NODE_NAME: 'env-name', UPSTREAM_BASE_URL: 'http://127.0.0.1:8000/v1' }, cfg)).toEqual({
      token: other,
      apiUrl: 'https://other.example',
      connectUrl: 'wss://other.example/v1/seller/connect',
      nodeName: 'env-name',
      upstream: 'http://127.0.0.1:8000/v1',
    });
    expect(resolveTarget({ CONNECT_URL: 'wss://x.example/v1/seller/connect', CONNECT_TOKEN: other }, cfg)).toMatchObject({ token: other, connectUrl: 'wss://x.example/v1/seller/connect' });
    expect(resolveTarget({}, cfg, { upstream: 'http://127.0.0.1:11434' }).upstream).toBe('http://127.0.0.1:11434');
  });

  it('with no token and no market URL there is nothing to connect to', () => {
    expect(resolveTarget({}, null)).toMatchObject({ token: undefined, connectUrl: undefined, apiUrl: DEFAULT_API_URL });
    expect(resolveTarget({ INFERIT_TOKEN: TOKEN }, null).connectUrl).toBe('wss://api-production-c74b9.up.railway.app/v1/seller/connect');
  });
});
