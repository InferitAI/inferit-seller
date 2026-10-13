// Logging for `start`. Fastify (pino) and the app write the same JSON lines into one sink, which:
//   - on a terminal, prints short plain lines ("10:45:01 info  connected to the market  nodeId=node_…");
//   - under systemd or Docker, passes the JSON lines to stdout (the journal / docker logs keep them);
//   - under launchd and the Windows task, appends them to ~/.inferit-seller/logs/seller.log, rotated at 10 MB.
// Request and response bodies are never logged, and the token never reaches a log line (pino redacts auth headers).
import fs from 'node:fs';
import path from 'node:path';
import { Writable } from 'node:stream';

export interface Logger {
  debug(obj: object, msg: string): void;
  info(obj: object, msg: string): void;
  warn(obj: object, msg: string): void;
  error(obj: object, msg: string): void;
}

const LEVELS: Record<string, number> = { trace: 10, debug: 20, info: 30, warn: 40, error: 50, fatal: 60 };
const NAMES: Record<number, string> = { 10: 'trace', 20: 'debug', 30: 'info', 40: 'warn', 50: 'error', 60: 'fatal' };
const SKIP = new Set(['level', 'time', 'pid', 'hostname', 'msg', 'v']);

export function levelNumber(name: string | undefined): number {
  return LEVELS[(name ?? 'info').toLowerCase()] ?? 30;
}

function scalar(v: unknown): string {
  if (v === null || v === undefined) return String(v);
  if (typeof v === 'string') return /\s/.test(v) ? JSON.stringify(v) : v;
  if (typeof v === 'number' || typeof v === 'boolean') return String(v);
  const s = JSON.stringify(v);
  return s.length > 300 ? `${s.slice(0, 300)}…` : s;
}

/** One pino JSON line as a short plain line. */
export function prettyLine(line: string): string {
  let o: Record<string, unknown>;
  try {
    o = JSON.parse(line) as Record<string, unknown>;
  } catch {
    return line;
  }
  const t = typeof o.time === 'number' ? new Date(o.time) : new Date();
  const hh = t.toTimeString().slice(0, 8);
  const level = (NAMES[Number(o.level)] ?? 'info').padEnd(5);
  // Fastify's request lines: method, url and status only.
  const req = o.req as { method?: string; url?: string } | undefined;
  const res = o.res as { statusCode?: number } | undefined;
  const extras: string[] = [];
  if (req?.method) extras.push(`${req.method} ${req.url ?? ''}`);
  if (res?.statusCode) extras.push(`status=${res.statusCode}`);
  for (const [k, v] of Object.entries(o)) if (!SKIP.has(k) && k !== 'req' && k !== 'res' && k !== 'reqId') extras.push(`${k}=${scalar(v)}`);
  return `${hh} ${level} ${String(o.msg ?? '')}${extras.length ? `  ${extras.join(' ')}` : ''}`;
}

/** Appends to a file, renaming it to <file>.1 when it passes `maxBytes`. */
export class RotatingFile {
  private fd: number | null = null;
  private size = 0;
  constructor(
    private readonly file: string,
    private readonly maxBytes = 10 * 1024 * 1024,
  ) {}

  private open(): number {
    if (this.fd !== null) return this.fd;
    fs.mkdirSync(path.dirname(this.file), { recursive: true, mode: 0o700 });
    this.fd = fs.openSync(this.file, 'a', 0o600);
    this.size = fs.fstatSync(this.fd).size;
    return this.fd;
  }

  write(text: string): void {
    try {
      if (this.open() !== null && this.size > this.maxBytes) {
        fs.closeSync(this.fd!);
        this.fd = null;
        fs.renameSync(this.file, `${this.file}.1`);
      }
      const buf = Buffer.from(text);
      fs.writeSync(this.open(), buf);
      this.size += buf.length;
    } catch {
      /* logging must never take the seller down */
    }
  }
}

export type LogFormat = 'pretty' | 'json' | 'file';

/** The sink both Fastify and the app logger write to (whole JSON lines). */
export function logSink(o: { format: LogFormat; file?: string; out?: NodeJS.WritableStream }): Writable {
  const out = o.out ?? process.stdout;
  const file = o.format === 'file' && o.file ? new RotatingFile(o.file) : null;
  let partial = '';
  return new Writable({
    write(chunk: Buffer | string, _enc, cb) {
      partial += chunk.toString();
      const lines = partial.split('\n');
      partial = lines.pop() ?? '';
      for (const line of lines) {
        if (line.trim() === '') continue;
        if (file) file.write(`${line}\n`);
        else out.write(`${o.format === 'pretty' ? prettyLine(line) : line}\n`);
      }
      cb();
    },
  });
}

/** A pino-compatible logger over a sink, for the parts of the app that run before or beside Fastify. */
export function makeLogger(sink: NodeJS.WritableStream, level = 'info'): Logger {
  const min = levelNumber(level);
  const emit = (lvl: number, obj: object, msg: string) => {
    if (lvl < min) return;
    sink.write(`${JSON.stringify({ level: lvl, time: Date.now(), pid: process.pid, ...obj, msg })}\n`);
  };
  return {
    debug: (obj, msg) => emit(20, obj, msg),
    info: (obj, msg) => emit(30, obj, msg),
    warn: (obj, msg) => emit(40, obj, msg),
    error: (obj, msg) => emit(50, obj, msg),
  };
}
