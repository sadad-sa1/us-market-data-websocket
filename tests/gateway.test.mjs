import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createServer } from 'node:net';
import { once } from 'node:events';
import WebSocket from 'ws';
import { loadConfig } from '../config.mjs';
import { parseClientMessage } from '../protocol.mjs';
import { SubscriptionManager } from '../subscriptions.mjs';
import { MarketState } from '../market-state.mjs';

test('configuration rejects partially numeric limits', () => {
  const previous = process.env.MAX_SYMBOLS;
  try {
    process.env.MAX_SYMBOLS = '30junk';
    assert.throws(loadConfig, /MAX_SYMBOLS/);
  } finally {
    if (previous === undefined) delete process.env.MAX_SYMBOLS;
    else process.env.MAX_SYMBOLS = previous;
  }
});
test('protocol rejects malformed messages and enforces test symbols', () => {
  assert.equal(parseClientMessage('{').error, 'invalid_json');
  assert.equal(parseClientMessage(JSON.stringify({ action: 'subscribe', symbols: ['AAPL'] }), { testMode: true }).ok, false);
  assert.deepEqual(parseClientMessage(JSON.stringify({ action: 'subscribe', symbols: ['aapl', 'AAPL'], channels: ['trades'] })).value.symbols, ['AAPL']);
});
test('shared subscriptions survive one client disconnecting', () => {
  const manager = new SubscriptionManager(['MSFT']);
  manager.subscribe('a', ['AAPL'], ['trades']);
  manager.subscribe('b', ['AAPL'], ['trades']);
  manager.removeClient('a');
  assert.deepEqual(manager.desired().trades, ['AAPL', 'MSFT']);
  manager.removeClient('b');
  assert.deepEqual(manager.desired().trades, ['MSFT']);
});
test('market snapshots derive spread and price movement', () => {
  const state = new MarketState();
  state.apply({ T: 't', S: 'AAPL', p: 100, s: 1, t: '2026-01-01T10:00:00Z' });
  state.apply({ T: 't', S: 'AAPL', p: 101, s: 1, t: '2026-01-01T10:05:00Z' });
  const snapshot = state.apply({ T: 'q', S: 'AAPL', bp: 100, ap: 102, bs: 30, as: 10 });
  assert.equal(snapshot.analytics.change5mPct, 1);
  assert.equal(snapshot.analytics.spread, 2);
  assert.equal(snapshot.analytics.quoteImbalance, 0.5);
});

test('HTTP readiness, WebSocket validation and atomic subscription rollback', { timeout: 15000 }, async t => {
  const probe = createServer();
  probe.listen(0, '127.0.0.1');
  await once(probe, 'listening');
  const port = probe.address().port;
  await new Promise(resolve => probe.close(resolve));
  const child = spawn(process.execPath, ['server.mjs'], {
    cwd: new URL('../', import.meta.url),
    env: { ...process.env, PORT: String(port), ALPACA_API_KEY_ID: '', ALPACA_API_SECRET_KEY: '', ALPACA_TEST_MODE: 'false', ALPACA_FEED: 'iex', DEFAULT_SYMBOLS: 'MSFT', MAX_SYMBOLS: '2', ALLOWED_ORIGINS: '*' },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  t.after(async () => {
    if (child.exitCode === null) {
      const closed = once(child, 'exit');
      child.kill('SIGTERM');
      await closed;
    }
  });
  await new Promise((resolve, reject) => {
    child.on('error', reject);
    child.once('exit', code => reject(new Error(`Server exited: ${code}`)));
    child.stdout.on('data', data => { if (String(data).includes('gateway listening')) resolve(); });
  });
  const base = `http://127.0.0.1:${port}`;
  assert.equal((await fetch(`${base}/health`)).status, 200);
  const ready = await fetch(`${base}/ready`);
  assert.equal(ready.status, 503);
  assert.equal((await ready.json()).missingConfiguration.length, 2);
  assert.equal((await fetch(`${base}/unknown`)).status, 404);
  assert.equal((await fetch(`${base}/health`, { method: 'POST' })).status, 405);
  const ws = new WebSocket(`ws://127.0.0.1:${port}/ws`);
  t.after(() => ws.terminate());
  const inbox = [];
  let pending;
  ws.on('message', raw => { const value = JSON.parse(String(raw)); if (pending) { const resolve = pending; pending = null; resolve(value); } else inbox.push(value); });
  const next = () => inbox.length ? Promise.resolve(inbox.shift()) : new Promise(resolve => { pending = resolve; });
  assert.equal((await next()).type, 'hello');
  ws.send(JSON.stringify({ action: 'subscribe', symbols: ['AAPL'], channels: ['trades'] }));
  assert.deepEqual((await next()).subscriptions.trades, ['AAPL']);
  ws.send(JSON.stringify({ action: 'subscribe', symbols: ['AAPL', 'NVDA'], channels: ['trades'] }));
  assert.equal((await next()).code, 'symbol_limit_exceeded');
  const stats = await (await fetch(`${base}/stats`)).json();
  assert.deepEqual(stats.desired.trades, ['AAPL', 'MSFT']);
  ws.send('{');
  assert.equal((await next()).code, 'invalid_json');
  ws.send(JSON.stringify({ action: 'ping' }));
  assert.equal((await next()).type, 'pong');
  ws.close();
  await once(ws, 'close');
});
