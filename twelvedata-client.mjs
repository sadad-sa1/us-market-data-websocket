import { WebSocket } from 'ws';

/**
 * Optional Twelve Data price-stream adapter. No secrets are logged.
 * A separate price-only source: Twelve Data WebSocket does not publish OHLCV bars or bid/ask.
 */
export class TwelveDataPriceStream {
  constructor({ apiKey = process.env.TWELVE_DATA_API_KEY, symbols = [], onPrice = () => {}, onStatus = () => {}, logger = console } = {}) {
    this.apiKey = apiKey;
    this.symbols = [...new Set(symbols.map(x => String(x).toUpperCase().trim()).filter(Boolean))];
    this.onPrice = onPrice;
    this.onStatus = onStatus;
    this.logger = logger;
    this.running = false;
    this.socket = null;
    this.retry = 0;
    this.retryTimer = null;
    this.heartbeat = null;
  }
  start() {
    if (!this.apiKey) throw new Error('TWELVE_DATA_API_KEY is required');
    if (this.running) return;
    this.running = true;
    this.connect();
  }
  connect() {
    if (!this.running) return;
    const ws = new WebSocket('wss://ws.twelvedata.com/v1/quotes/price?apikey=' + encodeURIComponent(this.apiKey), {
      handshakeTimeout: 15000,
      perMessageDeflate: false,
    });
    this.socket = ws;
    ws.on('open', () => {
      this.retry = 0;
      this.onStatus({ state: 'connected' });
      if (this.symbols.length) ws.send(JSON.stringify({ action: 'subscribe', params: { symbols: this.symbols.join(',') } }));
      this.heartbeat = setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ action: 'heartbeat' }));
      }, 10000);
    });
    ws.on('message', payload => {
      let message;
      try { message = JSON.parse(payload.toString()); } catch { return; }
      if (message.event === 'price' && message.symbol && Number.isFinite(Number(message.price))) {
        this.onPrice({ type: 'price', symbol: message.symbol, price: Number(message.price), timestamp: message.timestamp ?? null, source: 'twelvedata' });
      } else {
        this.onStatus({ state: 'message', event: message.event ?? 'unknown', status: message.status ?? null, code: message.code ?? null, message: message.message ?? null });
      }
    });
    ws.on('error', err => this.onStatus({ state: 'error', message: err.message }));
    ws.on('close', () => {
      if (this.socket !== ws) return;
      clearInterval(this.heartbeat);
      this.heartbeat = null;
      this.onStatus({ state: 'disconnected' });
      if (this.running) {
        const delay = Math.min(30000, 1000 * 2 ** Math.min(this.retry++, 5)) + Math.floor(Math.random() * 500);
        this.retryTimer = setTimeout(() => this.connect(), delay);
      }
    });
  }
  setSymbols(symbols) {
    this.symbols = [...new Set(symbols.map(x => String(x).toUpperCase().trim()).filter(Boolean))];
    const ws = this.socket;
    if (ws?.readyState !== WebSocket.OPEN) return;
    ws.send(JSON.stringify({ action: 'reset' }));
    if (this.symbols.length) ws.send(JSON.stringify({ action: 'subscribe', params: { symbols: this.symbols.join(',') } }));
  }
  stop() {
    this.running = false;
    clearTimeout(this.retryTimer);
    clearInterval(this.heartbeat);
    this.socket?.close();
    this.socket = null;
  }
}
