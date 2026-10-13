# inferit-seller

Sell the AI inference of your own computer on [Inferit](https://inferit.cc), the peer-to-peer market for AI inference. The app finds the model server on your machine, matches its models to the Inferit catalog and dials out to the market, so a laptop or a home PC can sell with no open port and no domain.

> **Status:** Inferit is a testnet demo on Whitechain Sepolia. Buyers pay in Inferit Test Credit (ITC), a test token with no monetary value. The contracts are not audited.

## What it does

- **Finds your model server.** Ollama on port 11434, LM Studio on 1234, vLLM on 8000 and the llama.cpp server on 8080. `--upstream` names another OpenAI-compatible server.
- **Matches your models to the catalog.** It reads the market's model list (`GET /v1/models`: each model's id and aliases) and maps local names to catalog ids, for example `qwen2.5:14b-instruct` to `qwen/qwen-2.5-14b-instruct`. A model that is not in the catalog is reported and not offered.
- **Dials out.** One WebSocket to the market (`wss://…/v1/seller/connect`), with your node token in the `Authorization` header. Requests come down that socket, so you need no open port, no domain, no certificate and no reverse proxy. A home router, NAT or a VPN is fine.
- **Serves through a local proxy.** Each request goes to a proxy on `127.0.0.1` that renames the model for your server and streams the answer back unchanged.
- **Keeps running.** `install-service` starts it at login and restarts it if it stops (a macOS LaunchAgent, a Linux systemd user unit or a Windows scheduled task). After sleep or a network change it reconnects on its own, and with no market session for 10 minutes it exits so the service starts a fresh process.

## Sell from your computer in about 10 minutes

You need [Node.js](https://nodejs.org) 20 or newer (`node --version`) and a model server.

**1. Run a catalog model on your computer.** With [Ollama](https://ollama.com):

```bash
ollama pull qwen2.5:14b-instruct
```

Or load a model in LM Studio and start its local server (Developer tab, Start Server). The catalog is at `https://api-production-c74b9.up.railway.app/v1/models`.

**2. Add the machine in the Sell workspace.** Open [inferit.app/sell](https://inferit.app/sell), sign in with your wallet, choose **Add a machine** and name it. The page shows one command with your node token (`icn_…`) in it. The token is shown once: copy the command now.

**3. Run the command.** On macOS or Linux:

```bash
npx -y github:OGcryptonaut/inferit-seller connect --token icn_… && npx -y github:OGcryptonaut/inferit-seller install-service
```

On Windows, in PowerShell, run the two parts one after the other:

```powershell
npx -y github:OGcryptonaut/inferit-seller connect --token icn_…
npx -y github:OGcryptonaut/inferit-seller install-service
```

`connect` checks the token with the market and saves it to `~/.inferit-seller/config.json`, readable by you only. `install-service` copies the app to `~/.inferit-seller/app` and starts it in the background.

**4. Check it.**

```bash
npx -y github:OGcryptonaut/inferit-seller status
npx -y github:OGcryptonaut/inferit-seller doctor
```

`doctor` checks the model server, the catalog match, the market, the token and the service, and says how to fix what is wrong. In the Sell workspace the machine shows as online, with the models it found.

**5. Set your prices.** In the Sell workspace, set a price for each matched model, in dollars per 1M input and output tokens. The market stores prices in µ (micro-dollars): $1 per 1M tokens is 1 000 000 µ. Buyers pay your price plus the 5% platform fee: an ask of 70 000 µ per 1M input tokens ($0.07, in test dollars on the testnet) is 73 500 µ to the buyer. You are paid per request to the payout wallet you register (test ITC on the testnet).

To install it for good instead of through npx: `npm install -g github:OGcryptonaut/inferit-seller`, then run `inferit-seller` directly.

## Commands

| Command | What it does |
|---|---|
| `connect --token <icn_…> [--api <url>] [--name <name>] [--upstream <url>]` | Checks the token with the market and saves it to `~/.inferit-seller/config.json` (mode 0600). `--name` is this machine's name in the hello (default: the hostname). |
| `start [--upstream <url>]` | Runs in the terminal: finds the server, maps the models, connects. Stop with Ctrl+C. |
| `install-service [--keep-awake] [--dry-run]` | Runs it in the background, at every login, restarted if it stops. `--keep-awake` keeps the computer from sleeping while it runs (macOS, Linux). `--dry-run` prints the service file and the commands and changes nothing. |
| `uninstall-service` | Stops the service and removes its file. The token stays in the config. |
| `status [--json]` | The token (its first characters only), the service, the market connection, the servers found and how each model maps. |
| `doctor [--json]` | Every check, with a fix for each problem. Exits 1 while something must be fixed. |
| `--version`, `--help` | |

Where the service lives:

| System | Service | Logs |
|---|---|---|
| macOS | `~/Library/LaunchAgents/cc.inferit.seller.plist` (RunAtLoad, KeepAlive) | `~/.inferit-seller/logs/seller.log` |
| Linux | `~/.config/systemd/user/inferit-seller.service` (Restart=always) | `journalctl --user -u inferit-seller -f` |
| Windows | Task Scheduler task `InferitSeller` (at logon, restarted if it stops) | `%USERPROFILE%\.inferit-seller\logs\seller.log` |

## Docker

```bash
docker run -d --restart unless-stopped --name inferit-seller -e INFERIT_TOKEN=icn_… \
  --add-host=host.docker.internal:host-gateway ghcr.io/ogcryptonaut/inferit-seller
```

The container looks for the model server on `host.docker.internal`. On Linux, Ollama listens on `127.0.0.1` by default, which a container cannot reach: start it with `OLLAMA_HOST=0.0.0.0` and keep port 11434 closed to the outside. Check it with `docker exec inferit-seller node /app/dist/cli.js status`.

## Configuration

The saved config covers most machines. Environment variables override it:

| Variable | Meaning |
|---|---|
| `INFERIT_TOKEN` (or `CONNECT_TOKEN`) | The node token (`icn_…`). |
| `INFERIT_API_URL` | The market (default `https://api-production-c74b9.up.railway.app`). `CONNECT_URL` sets the socket URL directly. |
| `NODE_NAME` | This machine's name in the hello. |
| `UPSTREAM_BASE_URL`, `UPSTREAM_API_KEY` | Use this server instead of finding one, and the key it needs. |
| `MODEL_MAP` | Your own mapping instead of the automatic one: `catalogId=localName` pairs or a JSON object. |
| `MODEL_META` | Per model, the context and quantisation you serve: `{"qwen/qwen-2.5-14b-instruct":{"maxContextTokens":32768}}`. |
| `INFERIT_DETECT_HOSTS` | Hosts to look for servers on (default `127.0.0.1`; the Docker image uses `host.docker.internal`). |
| `INFERIT_WATCHDOG_MINUTES` | Under the service or in Docker, exit after this long without a market session so a fresh process starts (default 10; 0 turns it off). In a terminal, `start` only logs a warning and keeps trying, unless you set this. |
| `INFERIT_KEEP_AWAKE=1` | Keep the computer awake while the app runs (macOS, Linux). |
| `INFERIT_SELLER_HOME` | Where the config, the state and the logs live (default `~/.inferit-seller`). |
| `CONNECT_MAX_CONCURRENCY` | Requests served at once (default 4). |
| `LOG_LEVEL` | `info` by default; `debug` shows every reconnect. |

## Troubleshooting

**The computer sleeps.** While it sleeps it cannot sell: the market marks it offline and sends buyers to other sellers. When it wakes, the app reconnects; if it cannot within 10 minutes, it exits and the service starts it again. To keep it awake while the app runs, use `install-service --keep-awake`.

**After a reboot.** The service starts at login. On Linux, `install-service` also runs `loginctl enable-linger`, so it starts at boot without a login; if that needs an administrator, it prints the command.

**A model is "not in the catalog".** The market lists only catalog models. Pull a catalog model (for example `ollama pull qwen2.5:14b-instruct`), or set `MODEL_MAP` if your server names a catalog model differently.

**Long prompts are cut.** Ollama often serves a smaller context than the model allows (4096 tokens in many versions) unless told otherwise; `doctor` warns when your server serves less than the catalog lists. Set `OLLAMA_CONTEXT_LENGTH` (for example 32768, as memory allows) for the Ollama server and restart it, or set a lower context limit with your price.

**The token is refused.** `doctor` shows the market's reason. A revoked or unknown token: add the machine again in the Sell workspace and run the new command. "Not registered as a seller": finish seller registration in the Sell workspace (it registers your payout wallet on chain); the app connects once it is done.

**One token, one machine.** Two copies with the same token replace each other on the market; each logs a warning and waits longer between tries, so neither serves reliably. `start` refuses to run next to a running copy on the same computer; on another machine or in Docker, stop one copy or add that machine with its own token.

**Why no port or domain?** The app opens the connection to the market, the way a browser does, so the market never connects to your computer. A home router, NAT or a VPN does not matter.

**What the market sees.** The hello carries this machine's name, the app's version, the operating system, the models it serves and the models it found. The token goes only in the `Authorization` header. The app never logs prompts or answers. The seller that serves a request does see it: that is you, for requests your machine serves.

## Development

This repository is the `apps/seller-node` package of the Inferit market. With Node 20 or newer:

```bash
npm ci
npm test
npm run typecheck
npm run build          # dist/, also run by npm's prepare step
node dist/cli.js doctor
```

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
