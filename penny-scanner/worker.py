"""Benzinga WebSocket -> durable outbox -> Telegram, using Composio accounts."""
import argparse
import asyncio
import contextlib
import hashlib
import json
import logging
import os
import random
import signal
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from websockets.asyncio.client import connect

LOG = logging.getLogger("news-worker")


class ApiError(Exception):
    def __init__(self, status=0, retry_after=0, ambiguous=False):
        super().__init__(f"API request failed (status={status})")
        self.status, self.retry_after, self.ambiguous = status, retry_after, ambiguous


@dataclass
class Config:
    api_key: str
    benzinga_account: str
    telegram_account: str
    chat_id: str
    tickers: str = ""
    channels: str = "Penny Stocks"
    db_path: str = "data/outbox.sqlite3"
    max_age: int = 900
    timezone: str = "Asia/Riyadh"

    @classmethod
    def from_env(cls):
        names = ("COMPOSIO_API_KEY", "BENZINGA_ACCOUNT_ID", "TELEGRAM_ACCOUNT_ID", "TELEGRAM_CHAT_ID")
        missing = [n for n in names if not os.getenv(n, "").strip()]
        if missing:
            raise ValueError("Missing environment variables: " + ", ".join(missing))
        cfg = cls(*(os.environ[n].strip() for n in names),
                  tickers=os.getenv("TICKERS", "").strip(),
                  channels=os.getenv("NEWS_CHANNELS", "Penny Stocks").strip(),
                  db_path=os.getenv("STATE_DB", "data/outbox.sqlite3"),
                  max_age=int(os.getenv("MAX_NEWS_AGE_SECONDS", "900")),
                  timezone=os.getenv("TIMEZONE", "Asia/Riyadh"))
        if cfg.max_age < 1:
            raise ValueError("MAX_NEWS_AGE_SECONDS must be positive")
        ZoneInfo(cfg.timezone)
        if not cfg.tickers and not cfg.channels and os.getenv('MODE','news') != 'scanner':
            raise ValueError("Set TICKERS or NEWS_CHANNELS to bound the feed")
        return cfg


class Composio:
    def __init__(self, key, base="https://backend.composio.dev/api/v3.1/tools/execute/"):
        self.key, self.base = key, base

    def _execute(self, slug, account, args):
        body = json.dumps({"connected_account_id": account, "version": "latest", "arguments": args}).encode()
        req = Request(self.base + slug, data=body, method="POST",
                      headers={"x-api-key": self.key, "Content-Type": "application/json"})
        try:
            with urlopen(req, timeout=25) as response:
                result = json.load(response)
        except HTTPError as exc:
            # Never log response bodies or URLs: they can contain connection secrets.
            try:
                retry_after = float(exc.headers.get("Retry-After", 0))
            except ValueError:
                retry_after = 0
            raise ApiError(exc.code, retry_after, ambiguous=exc.code >= 500) from None
        except Exception:
            raise ApiError(ambiguous=True) from None
        if not isinstance(result, dict):
            raise ApiError(ambiguous=True)
        if result.get("successful") is not True:
            data = result.get("data") or {}
            code = data.get("error_code", data.get("status_code", 0)) if isinstance(data, dict) else 0
            params = data.get("parameters") or {} if isinstance(data, dict) else {}
            raise ApiError(code, params.get("retry_after", 0), ambiguous=code != 429)
        data = result.get("data", {})
        if isinstance(data, dict) and data.get("ok") is False:
            code = data.get("error_code", 0)
            raise ApiError(code, (data.get("parameters") or {}).get("retry_after", 0), ambiguous=False)
        return data

    async def execute(self, slug, account, args):
        return await asyncio.to_thread(self._execute, slug, account, args)

    async def send(self, cfg, text):
        result = await self.execute("TELEGRAM_SEND_MESSAGE", cfg.telegram_account,
                                    {"chat_id": cfg.chat_id, "text": text, "disable_web_page_preview": True})
        msg = result.get("result", {}) if isinstance(result, dict) else {}
        if result.get("ok") is not True or not msg.get("message_id"):
            raise ApiError(ambiguous=True)
        return msg["message_id"]


def stream_url(details, cfg):
    parts = urlsplit(details["url"])
    if (parts.scheme, parts.hostname, parts.path) != ("wss", "api.benzinga.com", "/api/v1/news/stream"):
        raise ValueError("Unexpected Benzinga stream endpoint")
    query = dict(parse_qsl(parts.query))
    query["token"] = details["token"]
    if cfg.tickers:
        query["tickers"] = cfg.tickers
    if cfg.channels:
        query["channels"] = cfg.channels
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), ""))


def parse_date(value):
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        dt = parsedate_to_datetime(value)
    if dt.tzinfo is None:
        raise ValueError("Timestamp has no timezone")
    return dt


def notification(event, cfg, now=None):
    """Return one bounded plain-text alert; reject old/malformed/nonmatching events."""
    if not isinstance(event, dict) or event.get("kind") != "news":
        return None
    data = event.get("data")
    if not isinstance(data, dict):
        return None
    article = data.get("content")
    if not isinstance(article, dict):
        return None
    action = str(data.get("action", "")).lower()
    if action not in {"created", "updated", "deleted"}:
        return None
    article_id = article.get("id") or data.get("id")
    if not article_id or not article.get("title"):
        return None
    try:
        stamp = article.get("updated") or data.get("timestamp") or article.get("created")
        published = parse_date(stamp)
    except (ValueError, TypeError, OverflowError):
        return None
    age = (now if now is not None else time.time()) - published.timestamp()
    if age > cfg.max_age or age < -300:
        return None
    stocks = {x.get("name", "").upper() for x in article.get("stocks", []) if isinstance(x, dict)}
    channels = {x.get("name", "").casefold() for x in article.get("channels", []) if isinstance(x, dict)}
    wanted = {x.strip().upper() for x in cfg.tickers.split(",") if x.strip()}
    wanted_channels = {x.strip().casefold() for x in cfg.channels.split(",") if x.strip()}
    if wanted and not wanted.intersection(stocks):
        return None
    if wanted_channels and not wanted_channels.intersection(channels):
        return None
    url = str(article.get("url", ""))
    p = urlsplit(url)
    if p.scheme != "https" or p.hostname not in {"benzinga.com", "www.benzinga.com"}:
        url = ""
    label = {"created": "خبر جديد", "updated": "تحديث خبر", "deleted": "سحب خبر"}[action]
    when = published.astimezone(ZoneInfo(cfg.timezone)).strftime("%Y-%m-%d %H:%M:%S")
    text = (f"Benzinga | {label}\n"
            f"الرموز: {', '.join(sorted(stocks))[:200] or 'غير محددة'}\n"
            f"وقت الخبر: {when} ({cfg.timezone})\n\n"
            f"{str(article['title'])[:1200]}\n\n{url[:600]}\n\n"
            "تنبيه إخباري؛ السعر الحالي وأهلية السهم للمضاربة غير متحقق منهما.")
    # Different IDs in the envelope do not defeat article-level deduplication.
    signature = json.dumps([article_id, action, stamp, article.get("title"), url], ensure_ascii=False)
    key = hashlib.sha256(signature.encode()).hexdigest()
    return key, text, published.timestamp()


class Outbox:
    def __init__(self, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("""CREATE TABLE IF NOT EXISTS alerts (
            key TEXT PRIMARY KEY, text TEXT, published REAL,
            state TEXT DEFAULT 'pending', message_id TEXT, created REAL)""")
        if 'expires' not in {row[1] for row in self.db.execute('PRAGMA table_info(alerts)')}:
            self.db.execute('ALTER TABLE alerts ADD COLUMN expires REAL')
        # A crash during a send must not silently replay a possibly delivered message.
        self.db.execute("UPDATE alerts SET state='uncertain' WHERE state='sending'")
        self.db.commit()

    def enqueue(self, item, ttl=None):
        expires = item[2]+ttl if ttl is not None else None
        self.db.execute("INSERT OR IGNORE INTO alerts(key,text,published,created,expires) VALUES(?,?,?,?,?)", (*item, time.time(),expires))
        self.db.commit()

    def next(self):
        return self.db.execute("SELECT key,text,published,expires FROM alerts WHERE state='pending' ORDER BY created LIMIT 1").fetchone()

    def state(self, key, state, message_id=None):
        self.db.execute("UPDATE alerts SET state=?,message_id=? WHERE key=?", (state, message_id, key))
        self.db.commit()

    def prune(self):
        self.db.execute("DELETE FROM alerts WHERE state IN ('sent','expired') AND created < ?", (time.time() - 7*86400,))
        self.db.commit()


async def pause(stop, seconds):
    with contextlib.suppress(asyncio.TimeoutError):
        await asyncio.wait_for(stop.wait(), seconds)


async def sender(api, cfg, box, stop):
    while not stop.is_set():
        row = box.next()
        if not row:
            await pause(stop, 0.5)
            continue
        key, text, published, expires = row
        if time.time() > (expires if expires is not None else published+cfg.max_age):
            box.state(key, "expired")
            continue
        box.state(key, "sending")
        try:
            mid = await api.send(cfg, text)
        except ApiError as exc:
            if exc.status == 429:
                box.state(key, "pending")
                await pause(stop, max(2, float(exc.retry_after or 2)))
                continue
            box.state(key, "uncertain" if exc.ambiguous else "failed")
            LOG.error("Telegram send requires review: alert=%s status=%s ambiguous=%s", key[:12], exc.status, exc.ambiguous)
            # Stop consuming new events when the destination isn't working.
            stop.set()
            return
        box.state(key, "sent", str(mid))
        LOG.info("Alert delivered: %s", key[:12])
        await pause(stop, 1.1)


async def receive(api, cfg, box, stop, *, connector=connect, url_factory=stream_url, event_hook=None):
    failures = 0
    while not stop.is_set():
        started = time.monotonic()
        try:
            details = await api.execute("BENZINGA_GET_NEWSFEED_STREAM", cfg.benzinga_account, {})
            uri = url_factory(details, cfg)
            async with connector(uri, open_timeout=15, ping_interval=25, ping_timeout=20, close_timeout=5, max_size=2**21) as ws:
                LOG.info("Benzinga WebSocket connected")
                await ws.send("replay")
                while not stop.is_set():
                    try:
                        raw = await asyncio.wait_for(ws.recv(), 30)
                    except asyncio.TimeoutError:
                        await ws.send("ping")
                        continue
                    try:
                        event = json.loads(raw)
                        if event_hook and not event_hook(event):
                            continue
                        item = notification(event, cfg)
                    except (ValueError, TypeError, AttributeError):
                        LOG.warning("Ignored malformed stream event")
                        continue
                    if item:
                        box.enqueue(item)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # Do not log exception strings: handshake errors may contain token URLs.
            LOG.warning("Benzinga disconnected (%s); reconnecting", type(exc).__name__)
        if time.monotonic() - started > 60:
            failures = 0
        failures = min(failures + 1, 7)
        box.prune()
        await pause(stop, min(60, 2**failures) + random.random())


async def check(api, cfg):
    for slug, args in [("TELEGRAM_GET_ME", {}), ("TELEGRAM_GET_CHAT", {"chat_id": cfg.chat_id})]:
        result = await api.execute(slug, cfg.telegram_account, args)
        if result.get("ok") is not True:
            raise ValueError("Telegram verification failed")
    details = await api.execute("BENZINGA_GET_NEWSFEED_STREAM", cfg.benzinga_account, {})
    async with connect(stream_url(details, cfg), open_timeout=15, close_timeout=5) as ws:
        pong = await ws.ping()
        await asyncio.wait_for(pong, 10)
    LOG.info("Connection check passed (no news delivery asserted)")


async def main(args):
    cfg = Config.from_env()
    mode = os.getenv('MODE','news')
    if mode not in {'news','scanner'}:
        raise ValueError('MODE must be news or scanner')
    engine = None
    if mode == 'scanner':
        from scanner import Engine, Rules
        from market import Alpaca, scan_market
        rules = Rules.load(os.getenv('STRATEGY_FILE','strategy.json'))
        engine = Engine(rules)
        market_client = Alpaca()
        # Current candidate symbols are the local news filter, across all Benzinga channels.
        cfg.channels = ''
        cfg.tickers = ''
    api = Composio(cfg.api_key)
    await check(api, cfg)
    if args.send_test:
        await api.send(cfg, "✅ نجح فحص اتصال Benzinga وتيليجرام عبر Composio. هذه رسالة اختبار من برنامج البث.")
    if args.check:
        if engine:
            universe = await market_client.universe()
            candidates = await market_client.discover(universe,engine.rules,30)
            LOG.info('Scanner REST check passed: %d candidates; live subscription not tested',len(candidates))
        return
    box = Outbox(cfg.db_path)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)
    producer = asyncio.create_task(receive(api, cfg, box, stop,event_hook=engine.add_news if engine else None))
    consumer = asyncio.create_task(sender(api, cfg, box, stop))
    scanner = asyncio.create_task(scan_market(engine,box,stop,client=market_client)) if engine else None
    tasks = [t for t in (producer,consumer,scanner) if t is not None]
    for task in tasks:
        task.add_done_callback(lambda _:stop.set())
    try:
        await stop.wait()
    finally:
        producer.cancel()
        if scanner:
            scanner.cancel()
        outcomes = await asyncio.gather(*(t for t in (producer,scanner) if t),return_exceptions=True)
        for outcome in outcomes:
            if isinstance(outcome,Exception):
                LOG.error('Worker task ended (%s)',type(outcome).__name__)
        # Let an in-flight Telegram request finish before shutdown.
        await consumer
        box.db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Verify accounts and WebSocket, then exit")
    parser.add_argument("--send-test", action="store_true", help="Send one Telegram test after verification")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("websockets").setLevel(logging.CRITICAL)
    try:
        asyncio.run(main(parser.parse_args()))
    except KeyboardInterrupt:
        pass
    except Exception as exc:
        LOG.error("Startup stopped (%s). Check environment and account access.", type(exc).__name__)
        raise SystemExit(1) from None
