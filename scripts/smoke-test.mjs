import WebSocket from 'ws';
const base = new URL(process.argv[2] ?? 'http://localhost:10000');
const requireData = process.argv.includes('--require-data');
const symbol = process.env.SMOKE_SYMBOL ?? 'AAPL';
for (const path of ['/health', '/ready', '/stats']) {
  const response = await fetch(new URL(path, base), { signal: AbortSignal.timeout(15000) });
  const body = await response.json();
  if (!response.ok || !body.ok) throw new Error(`${path}: HTTP ${response.status}`);
  console.log(`${path}: OK`);
}
const url = new URL('/ws', base);
url.protocol = base.protocol === 'https:' ? 'wss:' : 'ws:';
await new Promise((resolve, reject) => {
  const ws = new WebSocket(url, { handshakeTimeout: 15000 });
  const timer = setTimeout(() => finish(new Error('WebSocket verification timed out')), 45000);
  let hello = false;
  function finish(error) {
    clearTimeout(timer);
    ws.terminate();
    error ? reject(error) : resolve();
  }
  ws.on('error', finish);
  ws.on('close', () => { clearTimeout(timer); reject(new Error('WebSocket closed before verification completed')); });
  ws.on('message', raw => {
    try {
      const message = JSON.parse(String(raw));
      if (message.type === 'error') return finish(new Error(`Gateway error: ${message.code}`));
      if (message.type === 'hello') {
        hello = true;
        ws.send(JSON.stringify({ action: 'ping' }));
        if (requireData) ws.send(JSON.stringify({ action: 'subscribe', symbols: [symbol], channels: ['trades', 'quotes', 'bars'] }));
      }
      if (!requireData && hello && message.type === 'pong') finish();
      if (requireData && ['trade', 'quote', 'bar'].includes(message.type) && message.symbol === symbol) {
        const age = Date.now() - Date.parse(message.raw?.t);
        if (Number.isFinite(age) && age >= -5000 && age <= 60000) finish();
      }
    } catch (error) { finish(error); }
  });
});
console.log(requireData ? 'WebSocket: fresh market data received' : 'WebSocket: hello/pong OK (market data not checked)');
