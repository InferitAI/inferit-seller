// Seller-node entrypoint, configured by the environment only (scripts/dev.sh, the Railway demo seller). The
// inferit-seller command (cli.ts) adds ~/.inferit-seller/config.json, the service and the checks on top.
//
// Env (SPEC §6): UPSTREAM_BASE_URL, UPSTREAM_API_KEY, MODEL_MAP, SELLER_TOKEN, MOCK, MOCK_LATENCY_MS, MOCK_FAIL_RATE
// (+ MOCK_CHUNK_DELAY_MS, MOCK_SEED, MOCK_CACHED_TOKENS), PORT, HOST, LOG_LEVEL.
// Connector mode (docs/specs/always-on-supply.md §5): CONNECT_URL (or INFERIT_API_URL), CONNECT_TOKEN (or
// INFERIT_TOKEN), NODE_NAME, CONNECT_MAX_CONCURRENCY, CONNECT_WALLET, MODEL_META. The node then dials out to the
// market and listens on 127.0.0.1 only; with MODEL_MAP unset it finds the local server and maps its models to the
// catalog itself. INFERIT_WATCHDOG_MINUTES (default 10, 0 = never): after that long without a market session, exit 1
// so a supervisor starts a fresh process. It exits only under a detected service manager or Docker, or when the
// variable is set; otherwise it logs an error and keeps trying (an unsupervised process that exited would stay down).
import { logSink, makeLogger } from './log.js';
import { serviceContext } from './service.js';
import { startSeller } from './start.js';

async function main(): Promise<void> {
  const sink = logSink({ format: 'json' });
  const running = await startSeller({
    env: process.env,
    appHome: null,
    config: null,
    log: makeLogger(sink, process.env.LOG_LEVEL ?? 'info'),
    sink,
    exit: (code) => process.exit(code),
    service: serviceContext(),
  });
  const shutdown = (signal: string) => {
    running.node.app.log.info({ signal }, 'shutting down');
    running.stop().then(
      () => process.exit(0),
      () => process.exit(1),
    );
  };
  process.once('SIGINT', () => shutdown('SIGINT'));
  process.once('SIGTERM', () => shutdown('SIGTERM'));
}

main().catch((err: unknown) => {
  console.error(`seller-node failed to start: ${err instanceof Error ? err.message : String(err)}`);
  process.exit(1);
});
