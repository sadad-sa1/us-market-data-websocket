import http from 'node:http';
import crypto from 'node:crypto';
import { WebSocketServer, WebSocket } from 'ws';
import { loadConfig, validateRuntimeConfig } from './config.mjs';
import { createLogger } from './logger.mjs';
import { encodeMessage, parseClientMessage } from './protocol.mjs';
import { MarketState } from './market-state.mjs';
import { SubscriptionManager } from './subscriptions.mjs';
import { AlpacaMarketStream } from './alpaca-client.mjs';

const config = loadConfig();
const logger = createLogger(config.logLevel);
const missingSecrets = validateRuntimeConfig(config);
const state = new MarketState();
const subscriptions = new SubscriptionManager(config.defaultSymbols);
const clients = new Map();
let shuttingDown = false;
let marketMessageCount = 0;
let lastMarketMessageAt = null;
let upstreamStatus = { state: missingSecrets.length ? 'not_configured' : 'starting' };

function jsonResponse(res, status, body, extraHeaders = {}) {
  const payload = JSON.stringify(body);
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': Buffer.byteLength(payload),
    'cache-control': 'no-store',
    ...extraHeaders,
  });
  res.end(payload);
}

function originAllowed(origin) {
  if (!origin) return true;
  return config.allowedOrigins.has('*') || config.allowedOrigins.has(origin);
}

function wsSend(ws, type, data = {}) {
  if (ws.readyState !== WebSocket.OPEN) return false;
  try {
    ws.send(encodeMessage(type, data));
    return true;
  } catch {
    return false;
  }
}

function broadcast(type, data, predicate = null) {
  for (const client of clients.values()) {
    if (!predicate || predicate(client)) wsSend(client.ws, type, data);
  }
}

function interestedIn(client, marketMessage) {
  const symbol = marketMessage.S;
  const channel = marketMessage.T === 't'
    ? 'trades'
    : marketMessage.T === 'q'
      ? 'quotes'
      : 'bars';
  return client.subscriptions[channel].includes(symbol);
}

function syncUpstream() {
  const desired = subscriptions.desired();
  const totalSymbols = subscriptions.uniqueSymbols().size;
  if (totalSymbols > config.maxSymbols) return false;
  upstream?.setDesired(desired);
  return true;
}

const upstream = missingSecrets.length
  ? null
  : new AlpacaMarketStream({
      config,
      logger,
      onMarketMessage(message) {
        marketMessageCount += 1;
        lastMarketMessageAt = new Date().toISOString();
        const snapshot = state.apply(message);
        if (!snapshot) return;
        const eventType = message.T === 't' ? 'trade' : message.T === 'q' ? 'quote' : 'bar';
        broadcast(eventType, { symbol: message.S, raw: message, snapshot }, (client) => interestedIn(client, message));
      },
      onStatus(status) {
        upstreamStatus = status;
        broadcast('upstream_status', { status });
      },
    });

const server = http.createServer((req, res) => {
  const url = new URL(req.url ?? '/', `http://${req.headers.host ?? 'localhost'}`);
  const origin = req.headers.origin;
  const cors = originAllowed(origin) && origin
    ? { 'access-control-allow-origin': origin, vary: 'Origin' }
    : {};

  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      ...cors,
      'access-control-allow-methods': 'GET,OPTIONS',
      'access-control-allow-headers': 'content-type',
      'access-control-max-age': '86400',
    });
    res.end();
    return;
  }

  if (req.method !== 'GET') {
    jsonResponse(res, 405, { ok: false, error: 'method_not_allowed' }, cors);
    return;
  }

  if (url.pathname === '/') {
    jsonResponse(res, 200, {
      service: 'us-market-data-websocket',
      version: '1.0.0',
      websocket: '/ws',
      health: '/health',
      ready: '/ready',
      stats: '/stats',
      feed: config.testMode ? 'test' : config.feed,
    }, cors);
    return;
  }

  if (url.pathname === '/health') {
    jsonResponse(res, 200, { ok: true, service: 'us-market-data-websocket', ts: new Date().toISOString() }, cors);
    return;
  }

  if (url.pathname === '/ready') {
    const ready = Boolean(upstream?.status.authenticated);
    jsonResponse(res, ready ? 200 : 503, {
      ok: ready,
      upstream: upstream?.status ?? { connected: false, authenticated: false },
      missingConfiguration: missingSecrets,
    }, cors);
    return;
  }

  if (url.pathname === '/stats') {
    jsonResponse(res, 200, {
      ok: true,
      marketMessageCount,
      lastMarketMessageAt,
      clients: clients.size,
      maxClients: config.maxClients,
      maxSymbols: config.maxSymbols,
      desired: subscriptions.desired(),
      upstream: upstream?.status ?? { connected: false, authenticated: false },
      upstreamStatus,
    }, cors);
    return;
  }

  jsonResponse(res, 404, { ok: false, error: 'not_found' }, cors);
});

const wss = new WebSocketServer({ noServer: true, maxPayload: 64 * 1024, perMessageDeflate: false });

server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url ?? '/', `http://${req.headers.host ?? 'localhost'}`);
  if (url.pathname !== '/ws') {
    socket.write('HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n');
    socket.destroy();
    return;
  }
  if (!originAllowed(req.headers.origin)) {
    socket.write('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n');
    socket.destroy();
    return;
  }
  if (shuttingDown || clients.size >= config.maxClients) {
    socket.write('HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\n');
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req));
});

wss.on('connection', (ws, req) => {
  const id = crypto.randomUUID();
  const client = {
    id,
    ws,
    isAlive: true,
    messageTimes: [],
    subscriptions: { trades: [], quotes: [], bars: [] },
    remoteAddress: req.socket.remoteAddress ?? null,
  };
  clients.set(id, client);
  subscriptions.addClient(id);

  ws.on('pong', () => { client.isAlive = true; });

  ws.on('message', (raw) => {
    const now = Date.now();
    client.messageTimes = client.messageTimes.filter((time) => now - time < 60_000);
    if (client.messageTimes.length >= config.maxMessagesPerMinute) {
      wsSend(ws, 'error', { code: 'rate_limited', message: 'Too many client messages.' });
      ws.close(1008, 'rate_limited');
      return;
    }
    client.messageTimes.push(now);

    const parsed = parseClientMessage(raw, { testMode: config.testMode });
    if (!parsed.ok) {
      wsSend(ws, 'error', { code: parsed.error });
      return;
    }

    const message = parsed.value;
    if (message.action === 'ping') {
      wsSend(ws, 'pong', { ts: new Date().toISOString() });
      return;
    }
    if (message.action === 'snapshot') {
      const symbols = message.symbols ?? [...new Set(Object.values(client.subscriptions).flat())];
      wsSend(ws, 'snapshot', { data: state.snapshots(symbols) });
      return;
    }

    const before = subscriptions.uniqueSymbols().size;
    const previousSubscriptions = subscriptions.clientView(id);
    if (message.action === 'subscribe') {
      subscriptions.subscribe(id, message.symbols, message.channels);
    } else {
      subscriptions.unsubscribe(id, message.symbols, message.channels);
    }

    if (subscriptions.uniqueSymbols().size > config.maxSymbols) {
      if (message.action === 'subscribe') {
        subscriptions.removeClient(id);
        subscriptions.addClient(id);
        for (const [channel, symbols] of Object.entries(previousSubscriptions)) {
          subscriptions.subscribe(id, symbols, [channel]);
        }
      }
      wsSend(ws, 'error', {
        code: 'symbol_limit_exceeded',
        message: `The gateway supports at most ${config.maxSymbols} unique symbols.`,
      });
      return;
    }

    client.subscriptions = subscriptions.clientView(id);
    syncUpstream();
    wsSend(ws, 'subscription', {
      subscriptions: client.subscriptions,
      globalUniqueSymbols: subscriptions.uniqueSymbols().size,
      previousGlobalUniqueSymbols: before,
    });
  });

  ws.on('close', () => {
    subscriptions.removeClient(id);
    clients.delete(id);
    syncUpstream();
  });

  ws.on('error', (error) => logger.warn('Downstream WebSocket error', { clientId: id, error }));

  wsSend(ws, 'hello', {
    clientId: id,
    service: 'us-market-data-websocket',
    feed: config.testMode ? 'test' : config.feed,
    defaultSymbols: config.defaultSymbols,
    maxSymbols: config.maxSymbols,
    channels: ['trades', 'quotes', 'bars'],
    upstream: upstream?.status ?? { connected: false, authenticated: false },
  });
});

const heartbeat = setInterval(() => {
  for (const client of clients.values()) {
    if (!client.isAlive) {
      client.ws.terminate();
      continue;
    }
    client.isAlive = false;
    try { client.ws.ping(); } catch { client.ws.terminate(); }
  }
}, 30_000);
heartbeat.unref?.();

if (upstream) {
  upstream.setDesired(subscriptions.desired());
  upstream.start();
} else {
  logger.warn('Alpaca credentials are not configured', { missing: missingSecrets });
}

server.listen(config.port, config.host, () => {
  logger.info('Market data gateway listening', {
    host: config.host,
    port: config.port,
    feed: config.testMode ? 'test' : config.feed,
    defaultSymbols: config.defaultSymbols,
  });
});

async function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  logger.info('Graceful shutdown started', { signal });
  clearInterval(heartbeat);
  upstream?.stop();
  for (const client of clients.values()) {
    try { client.ws.close(1001, 'server_shutdown'); } catch {}
  }
  wss.close();
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 10_000).unref();
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('uncaughtException', (error) => {
  logger.error('Uncaught exception', { error });
  shutdown('uncaughtException');
});
process.on('unhandledRejection', (error) => {
  logger.error('Unhandled rejection', { error });
});
