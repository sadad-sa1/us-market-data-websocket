import asyncio
import copy
import json
import tempfile
import threading
import time
import unittest
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from websockets.asyncio.server import serve

from worker import ApiError, Composio, Config, Outbox, notification, receive, sender, stream_url


def event():
    stamp = datetime.now(timezone.utc).isoformat()
    return {"kind": "news", "id": "envelope-1", "data": {"action": "created", "id": 123,
            "timestamp": stamp, "content": {"id": 123, "title": "Test catalyst",
            "updated": stamp, "url": "https://www.benzinga.com/news/test",
            "stocks": [{"name": "TEST"}], "channels": [{"name": "Penny Stocks"}]}}}


def config(path="unused.sqlite3"):
    return Config("test-key", "ca_bz", "ca_tg", "123", db_path=path)


class FilterTests(unittest.TestCase):
    def test_old_and_unmatched_news_are_rejected(self):
        item = event()
        item["data"]["content"]["updated"] = "2020-01-01T00:00:00Z"
        self.assertIsNone(notification(item, config()))
        item = event()
        item["data"]["content"]["channels"] = [{"name": "Large Cap"}]
        self.assertIsNone(notification(item, config()))

    def test_article_identity_and_update_alerts(self):
        first = event()
        replay = copy.deepcopy(first)
        replay["id"] = "another-envelope"
        self.assertEqual(notification(first, config())[0], notification(replay, config())[0])
        replay["data"]["action"] = "updated"
        self.assertNotEqual(notification(first, config())[0], notification(replay, config())[0])

    def test_long_unicode_title_stays_within_telegram_utf16_limit(self):
        item = event()
        item["data"]["content"]["title"] = "😀" * 8000
        item["data"]["content"]["url"] = "https://www.benzinga.com/" + "a" * 2000
        text = notification(item, config())[1]
        self.assertLessEqual(len(text.encode("utf-16-le")) // 2, 4096)

    def test_stream_config_rejects_unknown_host(self):
        with self.assertRaises(ValueError):
            stream_url({"url": "wss://example.com/", "token": "secret"}, config())
        url = stream_url({"url": "wss://api.benzinga.com/api/v1/news/stream", "token": "a+b&c"}, config())
        self.assertIn("token=a%2Bb%26c", url)

    def test_restart_does_not_replay_uncertain_delivery(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "outbox.db")
            box = Outbox(path)
            item = notification(event(), config())
            box.enqueue(item)
            box.state(item[0], "sending")
            box.db.close()
            reopened = Outbox(path)
            reopened.enqueue(item)
            self.assertIsNone(reopened.next())
            self.assertEqual(reopened.db.execute("SELECT state FROM alerts").fetchone()[0], "uncertain")
            reopened.db.close()


class DeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_rate_limit_is_retried_but_timeout_is_not(self):
        class Fake:
            attempts = 0
            async def send(self, cfg, text):
                self.attempts += 1
                if self.attempts == 1:
                    raise ApiError(429, 0)
                return 7
        with tempfile.TemporaryDirectory() as tmp:
            box = Outbox(str(Path(tmp)/"db"))
            box.enqueue(notification(event(), config()))
            stop = asyncio.Event()
            api = Fake()
            task = asyncio.create_task(sender(api, config(), box, stop))
            async def wait_delivery():
                while box.db.execute("SELECT state FROM alerts").fetchone()[0] != "sent":
                    await asyncio.sleep(0.02)
            await asyncio.wait_for(wait_delivery(), 4)
            stop.set()
            await task
            self.assertEqual(api.attempts, 2)
            box.db.close()
        class Timeout:
            attempts = 0
            async def send(self, cfg, text):
                self.attempts += 1
                raise ApiError(ambiguous=True)
        with tempfile.TemporaryDirectory() as tmp:
            box = Outbox(str(Path(tmp)/"db"))
            box.enqueue(notification(event(), config()))
            api = Timeout()
            await sender(api, config(), box, asyncio.Event())
            self.assertEqual(api.attempts, 1)
            self.assertEqual(box.db.execute("SELECT state FROM alerts").fetchone()[0], "uncertain")
            box.db.close()

    async def test_websocket_to_real_http_client_deduplicates_and_recovers(self):
        sent = []
        fixture = event()
        connections = []
        async def ws_handler(ws):
            connections.append(1)
            self.assertEqual(await ws.recv(), "replay")
            await ws.send(json.dumps(fixture))
            await ws.send(json.dumps(fixture))
            if len(connections) == 1:
                await ws.close()
            else:
                await asyncio.sleep(0.2)
        async with serve(ws_handler, "127.0.0.1", 0) as server:
            ws_url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"
            class Handler(BaseHTTPRequestHandler):
                def log_message(self, *args): pass
                def do_POST(self):
                    self.server.payload = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                    assert self.headers['x-api-key'] == 'test-key'
                    if self.path.endswith('BENZINGA_GET_NEWSFEED_STREAM'):
                        data = {"url": ws_url, "token": "fixture"}
                    else:
                        sent.append(self.server.payload)
                        data = {"ok": True, "result": {"message_id": 44}}
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"successful": True, "data": data}).encode())
            http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
            thread = threading.Thread(target=http.serve_forever, daemon=True)
            thread.start()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    cfg = config(str(Path(tmp)/"db"))
                    box = Outbox(cfg.db_path)
                    api = Composio(cfg.api_key, f"http://127.0.0.1:{http.server_port}/")
                    stop = asyncio.Event()
                    producer = asyncio.create_task(receive(api, cfg, box, stop, url_factory=lambda d,c: d['url']))
                    consumer = asyncio.create_task(sender(api, cfg, box, stop))
                    async def completed():
                        while len(connections) < 2 or not sent:
                            await asyncio.sleep(0.02)
                    await asyncio.wait_for(completed(), 6)
                    stop.set()
                    producer.cancel()
                    await asyncio.gather(producer, return_exceptions=True)
                    await consumer
                    self.assertEqual(len(sent), 1)
                    self.assertEqual(sent[0]['connected_account_id'], 'ca_tg')
                    self.assertEqual(sent[0]['arguments']['chat_id'], '123')
                    self.assertIn('Test catalyst', sent[0]['arguments']['text'])
                    box.db.close()
            finally:
                await asyncio.to_thread(http.shutdown)
                http.server_close()
                thread.join()


if __name__ == '__main__':
    unittest.main()
