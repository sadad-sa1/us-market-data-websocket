"""Alpaca SIP discovery, historical warmup and live bars/quotes. Read-only."""
import asyncio
import contextlib
import csv
import json
import logging
import os
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from websockets.asyncio.client import connect

from scanner import Bar, NY, timestamp, format_signal

LOG = logging.getLogger('market')


class MarketError(Exception):
    pass


class Alpaca:
    def __init__(self):
        self.key = os.getenv('ALPACA_API_KEY', '')
        self.secret = os.getenv('ALPACA_API_SECRET', '')
        if not self.key or not self.secret:
            raise ValueError('Alpaca market-data keys are required for scanner mode')
        self.headers = {'APCA-API-KEY-ID':self.key,'APCA-API-SECRET-KEY':self.secret}
        # Refuse delayed or single-venue feeds for this strategy's volume/spread rules.
        if os.getenv('ALPACA_FEED','sip') != 'sip':
            raise ValueError('This scanner requires SIP entitlement; no silent feed fallback')
        mode = os.getenv('ALPACA_ACCOUNT_MODE','paper')
        if mode not in {'paper','live'}:
            raise ValueError('ALPACA_ACCOUNT_MODE must be paper or live')
        self.trading_base = 'https://paper-api.alpaca.markets' if mode == 'paper' else 'https://api.alpaca.markets'

    def _get(self, path, params, catalog=False):
        base = self.trading_base if catalog else 'https://data.alpaca.markets'
        req = Request(base+path+'?'+urlencode(params),headers=self.headers)
        try:
            with urlopen(req,timeout=25) as response:
                return json.load(response)
        except Exception as exc:
            raise MarketError(type(exc).__name__) from None

    async def get(self, path, params, catalog=False):
        return await asyncio.to_thread(self._get,path,params,catalog)

    async def universe(self):
        explicit = [s.strip().upper() for s in os.getenv('UNIVERSE_SYMBOLS','').split(',') if s.strip()]
        if explicit:
            return list(dict.fromkeys(explicit))
        assets = await self.get('/v2/assets',{'status':'active','asset_class':'us_equity'},catalog=True)
        return sorted({a['symbol'] for a in assets if a.get('tradable') and a.get('exchange') in {'NYSE','NASDAQ','AMEX','ARCA','NYSEARCA','BATS'}
                       and not any(x in a.get('name','').upper() for x in (' ETF',' ETN',' FUND'))})

    async def discover(self, universe, rules, limit):
        candidates = []
        now = time.time()
        for start in range(0,len(universe),200):
            data = await self.get('/v2/stocks/snapshots',{'symbols':','.join(universe[start:start+200]),'feed':'sip'})
            for symbol, snapshot in data.items():
                if not isinstance(snapshot,dict):
                    continue
                try:
                    last = snapshot['latestTrade']
                    price = float(last['p'])
                    previous = float(snapshot['prevDailyBar']['c'])
                    vol = float(snapshot['dailyBar']['v'])
                    change = (price/previous-1)*100
                    stale = now-timestamp(last['t'])
                except (KeyError,TypeError,ValueError,ZeroDivisionError):
                    continue
                if rules.min_price<=price<rules.max_price and vol>=rules.min_day_volume and change>=rules.min_change_pct and 0<=stale<=120:
                    candidates.append((symbol,change,vol,previous))
            await asyncio.sleep(.35)
        candidates.sort(key=lambda x:(x[1],x[2]),reverse=True)
        LOG.info('Discovery: universe=%d qualifying=%d subscribed_limit=%d',len(universe),len(candidates),limit)
        return candidates[:limit]

    async def history(self, symbols):
        if not symbols:
            return {}
        now = datetime.now(NY)
        start = now.replace(hour=4,minute=0,second=0,microsecond=0)
        if now < start:
            return {}
        result = {s:[] for s in symbols}
        token = None
        while True:
            params = {'symbols':','.join(symbols),'timeframe':'1Min','start':start.isoformat(),
                      'end':(datetime.now(timezone.utc).replace(second=0,microsecond=0)-timedelta(microseconds=1)).isoformat(),
                      'feed':'sip','adjustment':'raw','limit':10000,'sort':'asc'}
            if token:
                params['page_token'] = token
            data = await self.get('/v2/stocks/bars',params)
            for symbol, bars in (data.get('bars') or {}).items():
                result.setdefault(symbol,[]).extend(Bar.parse(b) for b in bars)
            token = data.get('next_page_token')
            if not token:
                return result
            await asyncio.sleep(.35)


def metadata_file():
    path = Path(os.getenv('FUNDAMENTALS_CSV','fundamentals.csv'))
    if not path.exists():
        return {}
    with path.open(newline='',encoding='utf-8-sig') as f:
        return {row['symbol'].strip().upper():row for row in csv.DictReader(f) if row.get('symbol')}


async def scan_market(engine, box, stop, *, client=None, connector=connect):
    client = client or Alpaca()
    universe = await client.universe()
    limit = int(os.getenv('MAX_SYMBOLS','30'))
    if not 1<=limit<=100:
        raise ValueError('MAX_SYMBOLS must be 1..100')
    refresh_seconds = max(60,int(os.getenv('DISCOVERY_SECONDS','300')))
    failures = 0
    while not stop.is_set():
        try:
            candidates = await client.discover(universe,engine.rules,limit)
            snapshots = {symbol:previous for symbol,change,vol,previous in candidates}
            symbols = set(snapshots)
            history = await client.history(sorted(symbols))
            metadata = metadata_file()
            engine.set_symbols(symbols)
            for symbol in symbols:
                engine.seed(symbol,history.get(symbol,[]),snapshots[symbol],metadata.get(symbol,{}))
            if not symbols:
                LOG.info('No fresh eligible penny stocks; waiting for next discovery')
                await interruptible_wait(stop,refresh_seconds)
                continue
            # One connection handles quotes and bars; refresh independently of receive loop.
            async with connector('wss://stream.data.alpaca.markets/v2/sip',additional_headers=client.headers,
                                 open_timeout=15,ping_interval=20,ping_timeout=20,max_size=2**22) as ws:
                subscribed = False
                refresh_at = time.monotonic()+refresh_seconds
                while not stop.is_set():
                    try:
                        raw = await asyncio.wait_for(ws.recv(),timeout=1)
                    except asyncio.TimeoutError:
                        if time.monotonic() >= refresh_at:
                            # Break/reseed on refresh: never analyze candles across a data gap.
                            break
                        continue
                    messages = json.loads(raw)
                    if not isinstance(messages,list):
                        raise MarketError('Unexpected payload')
                    for item in messages:
                        kind = item.get('T')
                        if kind=='error':
                            # Don't expose raw auth errors, and never silently downgrade to delayed/IEX.
                            LOG.error('Alpaca stream error code=%s',item.get('code'))
                            raise MarketError('Alpaca stream rejected subscription')
                        if kind=='success' and item.get('msg')=='authenticated' and not subscribed:
                            sub = {'action':'subscribe','bars':sorted(symbols),
                                   'updatedBars':sorted(symbols),'quotes':sorted(symbols)}
                            if os.getenv('ENABLE_STATUS_STREAM','false').lower() == 'true':
                                sub['statuses'] = sorted(symbols)
                            await ws.send(json.dumps(sub))
                            subscribed = True
                            LOG.info('SIP stream authenticated; subscribed to %d candidates',len(symbols))
                        symbol = item.get('S')
                        if symbol not in engine.states:
                            continue
                        if kind=='q':
                            engine.quote(symbol,timestamp(item['t']),float(item['bp']),float(item['ap']))
                        elif kind=='s':
                            # Only explicit normal/trading/resume clears an observed halt.
                            code = str(item.get('sc','')).upper()
                            if code in {'H','P','Q','2','F'}:
                                engine.states[symbol].halted = True
                            elif code in {'T','3'}:
                                engine.states[symbol].halted = False
                        elif kind in {'b','u'}:
                            bar = Bar.parse(item)
                            existing = engine.states[symbol].ordered()
                            if kind=='b' and existing and bar.t > existing[-1].t+60:
                                refill = await client.history([symbol])
                                engine.seed(symbol,refill.get(symbol,[]),snapshots[symbol],metadata.get(symbol,{}))
                                LOG.info('Reseeded after minute gap: %s',symbol)
                                continue
                            for signal in engine.on_bar(symbol,bar,correction=kind=='u'):
                                box.enqueue((signal['key'],format_signal(signal),signal['t']),ttl=60)
                                LOG.info('Signal queued symbol=%s kind=%s',symbol,signal['kind'])
                    if time.monotonic()>=refresh_at:
                        break
                failures = 0
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            failures = min(7,failures+1)
            LOG.warning('Scanner reconnect/reseed (%s)',type(exc).__name__)
            await interruptible_wait(stop,min(60,2**failures)+random.random())
        # Reconnects always REST-backfill the current session before evaluating new bars.


async def interruptible_wait(stop, seconds):
    with contextlib.suppress(asyncio.TimeoutError):
        await asyncio.wait_for(stop.wait(),seconds)
