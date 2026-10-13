// Logging for `start` (src/log.ts): short lines on a terminal, JSON for journals, a rotating file for launchd.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { describe, expect, it } from 'vitest';
import { RotatingFile, logSink, makeLogger, prettyLine } from '../src/log.ts';

describe('log', () => {
  it('prints a pino line as a short plain line', () => {
    const t = new Date(2026, 9, 13, 10, 45, 1).getTime();
    expect(prettyLine(JSON.stringify({ level: 30, time: t, pid: 1, hostname: 'h', msg: 'connected to the market', nodeId: 'node_x', offers: 0 }))).toBe('10:45:01 info  connected to the market  nodeId=node_x offers=0');
    expect(prettyLine(JSON.stringify({ level: 40, time: t, msg: 'connector attempt failed: HTTP 502', lastFailure: { httpStatus: 502 } }))).toBe('10:45:01 warn  connector attempt failed: HTTP 502  lastFailure={"httpStatus":502}');
    expect(prettyLine(JSON.stringify({ level: 30, time: t, msg: 'request completed', reqId: 'req-1', req: { method: 'POST', url: '/v1/chat/completions' }, res: { statusCode: 200 }, responseTime: 12.5 }))).toBe('10:45:01 info  request completed  POST /v1/chat/completions status=200 responseTime=12.5');
    expect(prettyLine('not json')).toBe('not json');
  });

  it('filters by level and writes whole lines through the sink', () => {
    const out: string[] = [];
    const sink = logSink({ format: 'json', out: { write: (s: string) => out.push(s) } as unknown as NodeJS.WritableStream });
    const log = makeLogger(sink, 'warn');
    log.info({}, 'hidden');
    log.warn({ a: 1 }, 'shown');
    sink.write('{"level":30,"msg":"part');
    sink.write('ial"}\n');
    expect(out.map((l) => (JSON.parse(l) as { msg: string }).msg)).toEqual(['shown', 'partial']);
  });

  it('rotates the log file past its size', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'inferit-seller-log-'));
    try {
      const file = path.join(dir, 'logs', 'seller.log');
      const f = new RotatingFile(file, 100);
      for (let i = 0; i < 5; i++) f.write(`${'x'.repeat(40)}\n`);
      expect(fs.existsSync(`${file}.1`)).toBe(true);
      expect(fs.statSync(file).size).toBeLessThanOrEqual(123);
      if (process.platform !== 'win32') expect(fs.statSync(file).mode & 0o777).toBe(0o600);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});
