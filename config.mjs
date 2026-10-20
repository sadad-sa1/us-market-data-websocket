const SUPPORTED_FEEDS = new Set(['iex', 'sip', 'delayed_sip', 'boats', 'overnight']);

function envString(name, fallback = '') {
  const value = process.env[name];
  return typeof value === 'string' && value.trim() ? value.trim() : fallback;
}

function envInteger(name, fallback, { min = 1, max = Number.MAX_SAFE_INTEGER } = {}) {
  const raw = process.env[name];
  if (raw == null || raw === '') return fallback;
  const value = /^\d+$/.test(raw.trim()) ? Number(raw) : NaN;
  if (!Number.isInteger(value) || value < min || value > max) {
    throw new Error(`${name} must be an integer between ${min} and ${max}`);
  }
  return value;
}

function envBoolean(name, fallback = false) {
  const raw = process.env[name];
  if (raw == null || raw === '') return fallback;
  const normalized = raw.trim().toLowerCase();
  if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
  if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
  throw new Error(`${name} must be true or false`);
}

export function normalizeSymbol(value) {
  if (typeof value !== 'string') return null;
  const symbol = value.trim().toUpperCase();
  if (!/^[A-Z][A-Z0-9.-]{0,14}$/.test(symbol)) return null;
  return symbol;
}

function parseSymbols(raw) {
  const result = [];
  const seen = new Set();
  for (const part of raw.split(',')) {
    const symbol = normalizeSymbol(part);
    if (symbol && !seen.has(symbol)) {
      seen.add(symbol);
      result.push(symbol);
    }
  }
  return result;
}

function parseOrigins(raw) {
  const values = raw
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);
  return new Set(values.length ? values : ['*']);
}

export function loadConfig() {
  const testMode = envBoolean('ALPACA_TEST_MODE', false);
  const feed = envString('ALPACA_FEED', 'iex').toLowerCase();
  if (!SUPPORTED_FEEDS.has(feed)) {
    throw new Error(`ALPACA_FEED must be one of: ${[...SUPPORTED_FEEDS].join(', ')}`);
  }

  const maxSymbols = envInteger('MAX_SYMBOLS', 30, { min: 1, max: 5000 });
  let defaultSymbols = parseSymbols(envString('DEFAULT_SYMBOLS', 'AAPL,NVDA,TSLA,AMD,MSFT'));

  if (testMode) defaultSymbols = ['FAKEPACA'];
  if (defaultSymbols.length > maxSymbols) {
    throw new Error('DEFAULT_SYMBOLS exceeds MAX_SYMBOLS');
  }

  const apiKeyId = envString('ALPACA_API_KEY_ID');
  const apiSecretKey = envString('ALPACA_API_SECRET_KEY');

  return Object.freeze({
    port: envInteger('PORT', 10000, { min: 1, max: 65535 }),
    host: '0.0.0.0',
    logLevel: envString('LOG_LEVEL', 'info').toLowerCase(),
    feed,
    testMode,
    apiKeyId,
    apiSecretKey,
    defaultSymbols,
    maxSymbols,
    maxClients: envInteger('MAX_CLIENTS', 100, { min: 1, max: 10000 }),
    maxMessagesPerMinute: envInteger('MAX_MESSAGES_PER_MINUTE', 120, { min: 1, max: 10000 }),
    allowedOrigins: parseOrigins(envString('ALLOWED_ORIGINS', '*')),
    upstreamUrl: testMode
      ? 'wss://stream.data.alpaca.markets/v2/test'
      : `wss://stream.data.alpaca.markets/v2/${feed}`,
  });
}

export function validateRuntimeConfig(config) {
  const missing = [];
  if (!config.apiKeyId) missing.push('ALPACA_API_KEY_ID');
  if (!config.apiSecretKey) missing.push('ALPACA_API_SECRET_KEY');
  return missing;
}
