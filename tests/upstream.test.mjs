import { test } from 'node:test';
import assert from 'node:assert/strict';
import { WebSocketServer } from 'ws';
import { once } from 'node:events';
import { AlpacaMarketStream } from '../alpaca-client.mjs';

test('upstream authentication synchronizes subscriptions and forwards market events', { timeout: 5000 }, async t => {
  const server = new WebSocketServer({ port: 0, host: '127.0.0.1' });
  await once(server, 'listening');
  let received;
  const market = new Promise(resolve => { received = resolve; });
  let headers;
  server.on('connection', (ws, req) => {
    headers = req.headers;
    ws.send(JSON.stringify([{ T: 'success', msg: 'authenticated' }]));
    ws.on('message', raw => {
      const message = JSON.parse(String(raw));
      assert.equal(message.action, 'subscribe');
      assert.deepEqual(message.trades, ['AAPL']);
      ws.send(JSON.stringify([{ T: 'subscription', trades: ['AAPL'], quotes: [], bars: [] }, { T: 't', S: 'AAPL', p: 100, s: 1, t: new Date().toISOString() }]));
    });
  });
  const stream = new AlpacaMarketStream({
    config: { upstreamUrl: `ws://127.0.0.1:${server.address().port}`, apiKeyId: 'fixture-key', apiSecretKey: 'fixture-secret', feed: 'iex' },
    logger: { info() {}, warn() {}, error() {} },
    onMarketMessage: received,
  });
  t.after(async () => {
    stream.stop();
    for (const client of server.clients) client.terminate();
    await new Promise(resolve => server.close(resolve));
  });
  stream.setDesired({ trades: ['AAPL'] });
  stream.start();
  assert.equal((await market).p, 100);
  assert.equal(headers['apca-api-key-id'], 'fixture-key');
  assert.equal(stream.status.authenticated, true);
  assert.deepEqual(stream.status.active.trades, ['AAPL']);
  stream.stop();
  assert.equal(stream.status.authenticated, false);
});
