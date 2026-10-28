import http from 'node:http';
import { WebSocket } from 'ws';

const port = Number(process.env.PORT || 10000);
const token = process.env.BENZINGA_API_KEY;
const state = { connected: false, messages: 0, lastMessageAt: null, lastError: null, lastEvent: null };
let socket, retryTimer, pingTimer, retryCount = 0;
let shuttingDown = false;

function connect() {
  if (!token || shuttingDown) return;
  const u = new URL('wss://api.benzinga.com/api/v1/news/stream');
  u.searchParams.set('token', token);
  socket = new WebSocket(u.toString(), { handshakeTimeout: 15000 });
  socket.on('open', () => {
    retryCount = 0;
    state.connected = true;
    state.lastError = null;
    console.log(JSON.stringify({ event: 'benzinga_connected', at: new Date().toISOString() }));
    pingTimer = setInterval(() => { if (socket.readyState === WebSocket.OPEN) socket.ping(); }, 25000);
  });
  socket.on('message', (data) => {
    const now = new Date().toISOString();
    state.messages++;
    state.lastMessageAt = now;
    let parsed;
    try { parsed = JSON.parse(data.toString()); } catch {}
    const content = parsed?.data?.content ?? {};
    const symbols = Array.isArray(content.stocks) ? content.stocks.map(s => s.name).filter(Boolean) : [];
    state.lastEvent = { at: now, action: parsed?.data?.action ?? null, id: parsed?.id ?? null, symbols };
    console.log(JSON.stringify({ event: 'benzinga_message', ...state.lastEvent, title: content.title ?? null }));
  });
  socket.on('unexpected-response', (_request, response) => {
    state.lastError = 'HTTP ' + response.statusCode;
    console.error(JSON.stringify({ event: 'benzinga_auth_or_endpoint_error', status: response.statusCode }));
    response.resume();
  });
  socket.on('error', err => {
    state.lastError = err.code ?? err.message;
    console.error(JSON.stringify({ event: 'benzinga_error', code: err.code ?? 'UNKNOWN' }));
  });
  socket.on('close', code => {
    state.connected = false;
    clearInterval(pingTimer);
    console.log(JSON.stringify({ event: 'benzinga_closed', code }));
    if (shuttingDown) return;
    retryCount++;
    const delay = Math.min(60000, 1000 * Math.pow(2, Math.min(6, retryCount)));
    retryTimer = setTimeout(connect, delay);
  });
}

http.createServer((req, res) => {
  if (req.url !== '/health' && req.url !== '/stats' && req.url !== '/') {
    res.writeHead(404); return res.end('Not found');
  }
  const payload = { service: 'benzinga-live-news', configured: Boolean(token), ...state };
  res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
  res.end(JSON.stringify(req.url === '/health' ? { ok: true, configured: payload.configured, connected: state.connected } : payload));
}).listen(port, '0.0.0.0', () => {
  console.log(JSON.stringify({ event: 'server_listening', port, configured: Boolean(token) }));
  if (token) connect();
  else console.warn('BENZINGA_API_KEY is not configured; streaming disabled.');
});

process.on('SIGTERM', () => {
  shuttingDown = true;
  clearTimeout(retryTimer); clearInterval(pingTimer);
  socket?.close();
  process.exit(0);
});
