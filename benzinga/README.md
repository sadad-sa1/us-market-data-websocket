# Benzinga News Stream

Independent Render web service built from the `benzinga-news-service` branch.

Build: `cd benzinga && npm install --omit=dev`
Start: `node benzinga/index.js`
Health: `/health`

Set `BENZINGA_API_TOKEN` in Render Environment (required). Optional: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `TICKERS`. Never commit API keys.
