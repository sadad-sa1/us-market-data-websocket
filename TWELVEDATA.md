# Twelve Data optional WebSocket adapter

The repository includes `twelvedata-client.mjs`, an isolated, **not yet wired into server.mjs** price-stream adapter. The existing Alpaca gateway is unchanged.

Add `TWELVE_DATA_API_KEY` as a server-side secret in Render, never in source code or a browser bundle. Use a Twelve Data plan that authorizes the requested symbols: Basic/Grow can only test eligible trial symbols; full symbols require a sufficient paid plan.

Minimal Node.js example:

```js
import { TwelveDataPriceStream } from './twelvedata-client.mjs';
const stream = new TwelveDataPriceStream({
  symbols: ['AAPL'],
  onPrice: (event) => console.log(event.symbol, event.price, event.timestamp),
  onStatus: (status) => console.log(status),
});
stream.start();
process.on('SIGTERM', () => stream.stop());
```

This adapter receives **price updates only**. It does not claim to provide bid/ask quotes, trade sizes, volume, minute bars, or RVOL through WebSocket. Those require other feeds / independent computation. Keep the Alpaca gateway operational until an integration is implemented and validated.

Official docs: https://support.twelvedata.com/en/articles/5620516-how-to-stream-the-data
