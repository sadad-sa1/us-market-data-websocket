# US Market Data WebSocket

Standalone real-time U.S. equities market-data gateway. It maintains **one upstream Alpaca Market Data WebSocket** and multiplexes that stream to downstream browser/app clients over `/ws`.

## Architecture

```text
Alpaca Market Data WebSocket
          │
          ▼
  us-market-data-websocket
  - one upstream connection
  - subscriptions/reference counts
  - trade/quote/bar normalization
  - 1m/5m momentum analytics
  - reconnect + heartbeat
          │
          ▼
Browser / mobile / backend clients
          wss://<host>/ws
```

## Supported upstream data

- Trades
- Level-1 quotes
- Minute bars / updated bars
- IEX by default; SIP/delayed SIP/BOATS/overnight can be configured if your Alpaca subscription allows them.

The `momentum` field is an analytics score from 0–100 based on short-window price movement and quote-size imbalance. It is **not** an order instruction or investment recommendation.

## Required environment variables

Copy `.env.example` to `.env` locally and fill the required keys, or set them in your runtime environment. `npm start` reads exported environment variables; use `node --env-file=.env server.mjs` to load the local file. Never commit real credentials.

```text
ALPACA_API_KEY_ID=...
ALPACA_API_SECRET_KEY=...
ALPACA_FEED=iex
```

For an always-available integration test, set:

```text
ALPACA_TEST_MODE=true
DEFAULT_SYMBOLS=FAKEPACA
```

The service then connects to Alpaca's test stream and only accepts `FAKEPACA`.

## Start locally

```bash
npm ci
npm run verify
node --env-file=.env server.mjs
```

HTTP endpoints:

- `GET /health` — process health
- `GET /ready` — upstream authentication readiness
- `GET /stats` — connection/subscription diagnostics
- `GET /` — service metadata

WebSocket endpoint:

```text
ws://localhost:10000/ws
```

Public production connections should use `wss://`.

## Client protocol

Subscribe:

```json
{
  "action": "subscribe",
  "symbols": ["AAPL", "NVDA"],
  "channels": ["trades", "quotes", "bars"]
}
```

Unsubscribe:

```json
{
  "action": "unsubscribe",
  "symbols": ["NVDA"],
  "channels": ["quotes"]
}
```

Request the latest in-memory snapshot:

```json
{
  "action": "snapshot",
  "symbols": ["AAPL"]
}
```

Application-level ping:

```json
{ "action": "ping" }
```

Server events include `hello`, `subscription`, `trade`, `quote`, `bar`, `snapshot`, `upstream_status`, `pong`, and `error`.

A market event includes both Alpaca's raw event and a derived snapshot. Example shape:

```json
{
  "type": "quote",
  "symbol": "AAPL",
  "raw": { "T": "q", "S": "AAPL" },
  "snapshot": {
    "symbol": "AAPL",
    "price": 233.12,
    "analytics": {
      "change1mPct": 0.08,
      "change5mPct": 0.31,
      "quoteImbalance": 0.25,
      "spreadBps": 0.86,
      "momentum": {
        "score": 61.4,
        "regime": "up"
      }
    }
  }
}
```

## Production controls

- `MAX_SYMBOLS`: maximum number of unique symbols across base + clients.
- `MAX_CLIENTS`: maximum simultaneous downstream WebSockets.
- `MAX_MESSAGES_PER_MINUTE`: per-client message-rate limit.
- `ALLOWED_ORIGINS`: comma-separated browser Origin allowlist; use explicit production origins instead of `*` when the frontend domain is known.
- 30-second heartbeat terminates dead downstream and upstream sockets.
- Upstream reconnect uses capped exponential backoff with jitter.
- Payloads are capped at 64 KiB and per-message compression is disabled.

## Render

`render.yaml` is included. Render requires the HTTP server to bind to `0.0.0.0` and the `PORT` it provides; this project does both.

For initial validation, the Blueprint uses the free plan. For production continuous market streaming, choose an always-on paid instance appropriate for your traffic and latency requirements.

After deployment:

```bash
node scripts/smoke-test.mjs https://YOUR-SERVICE.onrender.com
```

Then connect a WebSocket client to:

```text
wss://YOUR-SERVICE.onrender.com/ws
```

## Verification boundaries

`npm run verify` checks the actual root-level modules and runs offline unit/integration tests. It does not contact Alpaca. Tests cover missing credentials, HTTP routes, WebSocket messages, subscription limits and shared subscriptions.

`/health` is liveness only. `/ready` confirms Alpaca authentication, not successful subscriptions or fresh prices. Compare `desired` with `upstream.active` in `/stats`. `marketMessageCount` and `lastMarketMessageAt` track received upstream market events since process start; they are not per-symbol freshness guarantees.

Run a live end-to-end check during the selected feed's session:

```bash
SMOKE_SYMBOL=AAPL npm run smoke -- https://YOUR-SERVICE.onrender.com --require-data
```

For test mode use `SMOKE_SYMBOL=FAKEPACA`. Without `--require-data`, smoke checks HTTP readiness and WebSocket hello/pong only. Delayed feeds intentionally may fail the 60-second freshness check.

`ALLOWED_ORIGINS` restricts browser origins, not client authentication. Clients without an Origin header are accepted. Keep credentials server-side.
